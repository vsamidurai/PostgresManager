package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	pgv1 "github.com/vsamidurai/PostgresManager/api/v1alpha1"
	"github.com/vsamidurai/PostgresManager/internal/ha"
	"github.com/vsamidurai/PostgresManager/internal/postgres"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;create;patch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;create;patch

func (r *PostgresClusterReconciler) saveHA(ctx context.Context, cm *corev1.ConfigMap, s ha.State) error {
	if err := s.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	cm.Data = map[string]string{"state.json": string(data)}
	return r.Update(ctx, cm)
}

func (r *PostgresClusterReconciler) reconcileHA(ctx context.Context, c *pgv1.PostgresCluster) (ctrl.Result, error) {
	if err := postgres.Validate(c); err != nil {
		return r.haStatus(ctx, c, ha.State{}, nil, "InvalidSpec", err.Error())
	}
	cm := &corev1.ConfigMap{}
	err := r.reader().Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: postgres.HAControlName(c)}, cm)
	if apierrors.IsNotFound(err) {
		if controllerutil.ContainsFinalizer(c, finalizer) {
			return r.haStatus(ctx, c, ha.State{}, nil, "RecoveryRequired", "Regional authority record is missing")
		}
		if !c.DeletionTimestamp.IsZero() {
			return ctrl.Result{}, nil
		}
		s := ha.State{Version: 1, ClusterUID: string(c.UID), ContractHash: postgres.ContractHash(c), Term: 1, Phase: ha.Running, Primary: postgres.InstanceNames(c, 1).Instance}
		for i := 1; i <= int(c.Spec.Instances); i++ {
			s.Members = append(s.Members, ha.Member{Name: postgres.InstanceNames(c, i).Instance})
		}
		if c.Spec.Replication.Mode == "Synchronous" {
			s.SyncMember = s.Members[1].Name
		}
		data, _ := json.Marshal(s)
		cm = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: postgres.HAControlName(c), Namespace: c.Namespace, Labels: postgres.Labels(c)}, Data: map[string]string{"state.json": string(data)}}
		if err := controllerutil.SetControllerReference(c, cm, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, cm); err != nil {
			return ctrl.Result{}, err
		}
	} else if err != nil {
		return ctrl.Result{}, err
	}
	if !metav1.IsControlledBy(cm, c) || !cm.DeletionTimestamp.IsZero() {
		return r.haStatus(ctx, c, ha.State{}, nil, "RecoveryRequired", "Authority record ownership or lifecycle changed")
	}
	s, err := ha.Decode(cm.Data["state.json"])
	if err != nil || s.ClusterUID != string(c.UID) || s.ContractHash != postgres.ContractHash(c) || ((c.Spec.Replication.Mode == "Synchronous") != (s.SyncMember != "")) {
		return r.haStatus(ctx, c, s, nil, "RecoveryRequired", "Regional authority state is invalid or belongs to another contract")
	}
	if err := r.haLabels(ctx, c, cm, s.Members[0].Name); err != nil {
		return ctrl.Result{}, err
	}
	if !controllerutil.ContainsFinalizer(c, finalizer) {
		for _, m := range s.Members {
			if m.PVCRequested {
				return r.haStatus(ctx, c, s, nil, "RecoveryRequired", "Protection removed after data allocation")
			}
		}
		before := c.DeepCopy()
		controllerutil.AddFinalizer(c, finalizer)
		if err := r.Patch(ctx, c, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, err
		}
	}
	observations := map[string]ha.Observation{}
	for _, m := range s.Members {
		object := &corev1.ConfigMap{}
		err := r.reader().Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: postgres.ObservationName(m.Name)}, object)
		if err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		if err == nil && metav1.IsControlledBy(object, c) {
			var o ha.Observation
			if json.Unmarshal([]byte(object.Data["observation.json"]), &o) == nil && ha.Bound(s, string(cm.UID), o) {
				observations[m.Name] = o
			}
		}
	}
	if !c.DeletionTimestamp.IsZero() {
		return r.deleteHA(ctx, c, cm, s, observations)
	}
	// Retain the reservation until its terminal status is durable. A lost
	// status response must never let a later request erase completion evidence.
	if s.Request != nil && s.CompletedOperation == s.Request.UID {
		op := &pgv1.PostgresOperation{}
		err := r.reader().Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: s.Request.Name}, op)
		if apierrors.IsNotFound(err) || (err == nil && (string(op.UID) != s.Request.UID || op.Status.Phase == "Succeeded" || op.Status.Phase == "Rejected")) {
			s.Request = nil
			return ctrl.Result{}, r.saveHA(ctx, cm, s)
		}
		if err != nil {
			return ctrl.Result{}, err
		}
	}
	primary := observations[s.Primary]
	if primary.SystemID != "" && primary.Role == ha.Primary && ha.Fresh(primary, r.now()) {
		if s.SystemID != "" && primary.SystemID != s.SystemID {
			return r.haStatus(ctx, c, s, observations, "RecoveryRequired", "Primary system identifier changed")
		}
		if s.SystemID != primary.SystemID || s.Timeline != primary.Timeline {
			s.SystemID = primary.SystemID
			s.Timeline = primary.Timeline
			return ctrl.Result{}, r.saveHA(ctx, cm, s)
		}
	}
	if s.Phase == ha.Running && s.Request != nil && s.Request.Type == "Rejoin" && s.CompletedOperation != s.Request.UID {
		for i := range s.Members {
			m := &s.Members[i]
			if m.Name != s.Request.Target {
				continue
			}
			if m.Name == s.Primary {
				if o := observations[m.Name]; o.Operation == s.Request.UID && o.Role == ha.Primary && ha.Fresh(o, r.now()) {
					s.CompletedOperation = s.Request.UID
					s.CompletedOutcome = "Succeeded"
					return ctrl.Result{}, r.saveHA(ctx, cm, s)
				}
				return r.haStatus(ctx, c, s, observations, "OperationRejected", "Cannot rebuild the current primary")
			}
			if m.RejoinOperation != s.Request.UID {
				pod := &corev1.Pod{}
				err := r.reader().Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: m.Name}, pod)
				if apierrors.IsNotFound(err) && m.PodRequested {
					f := m.Fence
					if m.FenceConsumed || f == nil || !ha.RepairFence(s, string(cm.UID), *m, *f) {
						return r.haStatus(ctx, c, s, observations, "AwaitingFencing", "Missing Pod repair requires an unused, identity-bound shutdown receipt")
					}
					m.PreviousPodUID = m.PodUID
					m.PodUID = ""
					m.PodRequested = false
					return ctrl.Result{}, r.saveHA(ctx, cm, s)
				}
				if err != nil && !apierrors.IsNotFound(err) {
					return ctrl.Result{}, err
				}
				if m.PodUID == "" {
					break
				} // Bind replacement before authorizing rejoin.
				m.RejoinOperation = s.Request.UID
				m.FenceConsumed = true
				return ctrl.Result{}, r.saveHA(ctx, cm, s)
			}
			if o := observations[m.Name]; o.Operation == s.Request.UID && o.Role == ha.Standby && ha.Fresh(o, r.now()) {
				s.CompletedOperation = s.Request.UID
				s.CompletedOutcome = "Succeeded"
				return ctrl.Result{}, r.saveHA(ctx, cm, s)
			}
		}
	}
	if s.Phase == ha.Running && s.SystemID != "" {
		requested := s.Request != nil && s.Request.Type == "Switchover" && s.Request.Target != s.Primary && s.CompletedOperation != s.Request.UID
		failed := primary.Role == ha.Fenced || primary.Role == ha.Failed || (!ha.Fresh(primary, r.now()) && r.now().Sub(s.GrantedAt) > ha.ObservationTimeout)
		if requested || failed {
			if failed && s.Request != nil && s.Request.Type == "Switchover" && s.Request.Target == s.Primary && s.CompletedOperation != s.Request.UID {
				s.CompletedOperation = s.Request.UID
				s.CompletedOutcome = "Rejected"
			}
			s.Phase = ha.Fencing
			s.Operation = "auto-" + string(uuid.NewUUID())
			s.Target = ""
			s.Fence = nil
			if requested {
				s.Operation = s.Request.UID
				s.Target = s.Request.Target
			}
			return ctrl.Result{}, r.saveHA(ctx, cm, s)
		}
	}
	if s.Phase == ha.Fencing {
		if err := r.haServices(ctx, c, s, observations); err != nil {
			return ctrl.Result{}, err
		}
		if !ha.FenceVerified(s, string(cm.UID), primary) {
			return r.haStatus(ctx, c, s, observations, "AwaitingFencing", "Previous writer must confirm durable inhibition and PostgreSQL exit; timeout or Pod deletion is insufficient")
		}
		s.Fence = &primary
		if primary.Timeline <= 0 {
			return r.haStatus(ctx, c, s, observations, "DurabilityProofUnavailable", "Stopped primary timeline is unknown")
		}
		s.Timeline = primary.Timeline
		s.Phase = ha.Preparing
		for i := range s.Members {
			if s.Members[i].Name == s.Primary {
				s.Members[i].Fence = &primary
				s.Members[i].FenceConsumed = false
			}
		}
		return ctrl.Result{}, r.saveHA(ctx, cm, s)
	}
	if s.Phase == ha.Preparing {
		if err := r.haServices(ctx, c, s, observations); err != nil {
			return ctrl.Result{}, err
		}
		// Give every responsive replica the opportunity to drain and pause.
		// Otherwise a fast candidate can strand a healthy synchronous partner
		// on the old timeline before it has observed the preparation barrier.
		for _, m := range s.Members {
			o := observations[m.Name]
			if m.Name != s.Primary && o.Role == ha.Standby && ha.Fresh(o, r.now()) {
				return r.haStatus(ctx, c, s, observations, "AwaitingStandbyPreparation", "Waiting for responsive replicas to record their replay barrier")
			}
		}
		candidate, err := ha.Candidate(s, string(cm.UID), observations, c.Spec.Replication.Mode, c.Spec.Replication.DataLossPolicy, r.now())
		if err != nil {
			return r.haStatus(ctx, c, s, observations, err.Error(), "Promotion is held until fencing, lineage, replay and durability evidence are complete")
		}
		if err := r.verifyHAMember(ctx, c, s, candidate.Instance); err != nil {
			return r.haStatus(ctx, c, s, observations, "RecoveryRequired", err.Error())
		}
		old := s.Primary
		s.GrantFence = s.Fence
		s.Primary = candidate.Instance
		s.ForkLSN = candidate.ReplayLSN
		s.ParentTimeline = candidate.Timeline
		s.Term++
		s.Phase = ha.Running
		s.GrantedAt = r.now()
		s.SyncMember = ""
		if c.Spec.Replication.Mode == "Synchronous" {
			for _, m := range s.Members {
				if m.Name != s.Primary && m.Name != old && observations[m.Name].Role == ha.Prepared && ha.Fresh(observations[m.Name], r.now()) {
					s.SyncMember = m.Name
					break
				}
			}
			if s.SyncMember == "" {
				s.SyncMember = old
			} // Block commits until protection is restored; never weaken policy.
		}
		return ctrl.Result{}, r.saveHA(ctx, cm, s)
	}
	if s.Request != nil && s.Request.Type == "Switchover" && s.CompletedOperation != s.Request.UID && s.Primary == s.Request.Target && primary.Role == ha.Primary && ha.Fresh(primary, r.now()) {
		s.CompletedOperation = s.Request.UID
		s.CompletedOutcome = "Succeeded"
		return ctrl.Result{}, r.saveHA(ctx, cm, s)
	}
	if len(s.Members) < int(c.Spec.Instances) {
		s.Members = append(s.Members, ha.Member{Name: postgres.InstanceNames(c, len(s.Members)+1).Instance})
		return ctrl.Result{}, r.saveHA(ctx, cm, s)
	}
	if delay := s.RetryAfter.Sub(r.now()); delay > 0 {
		return r.haStatus(ctx, c, s, observations, "CreateRejected", "Waiting before retrying rejected Kubernetes resource creation")
	}
	if err := r.checkHASecrets(ctx, c); err != nil {
		return r.haStatus(ctx, c, s, observations, "CredentialsUnavailable", err.Error())
	}
	if err := r.haServices(ctx, c, s, observations); err != nil {
		return ctrl.Result{}, err
	}
	for i := range s.Members {
		if err := r.haMember(ctx, c, cm, &s, i); err != nil {
			if apierrors.IsConflict(err) || apierrors.IsAlreadyExists(err) {
				return ctrl.Result{}, err
			}
			return r.haStatus(ctx, c, s, observations, "RecoveryRequired", err.Error())
		}
	}
	return r.haStatus(ctx, c, s, observations, "Reconciled", "Regional resources reconciled")
}

func (r *PostgresClusterReconciler) verifyHAMember(ctx context.Context, c *pgv1.PostgresCluster, s ha.State, name string) error {
	m, ok := s.Member(name)
	if !ok {
		return fmt.Errorf("candidate is no longer assigned")
	}
	pod := &corev1.Pod{}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.reader().Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: name}, pod); err != nil {
		return err
	}
	if err := r.reader().Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: name + "-data"}, pvc); err != nil {
		return err
	}
	if string(pod.UID) != m.PodUID || string(pvc.UID) != m.PVCUID || !pod.DeletionTimestamp.IsZero() || !pvc.DeletionTimestamp.IsZero() || !metav1.IsControlledBy(pod, c) || metav1.GetControllerOf(pvc) != nil {
		return fmt.Errorf("candidate Pod or volume identity changed")
	}
	return nil
}

func (r *PostgresClusterReconciler) deleteHA(ctx context.Context, c *pgv1.PostgresCluster, cm *corev1.ConfigMap, s ha.State, obs map[string]ha.Observation) (ctrl.Result, error) {
	if s.Phase != ha.Stopping {
		s.Phase = ha.Stopping
		s.Operation = "delete-" + string(c.UID)
		return ctrl.Result{}, r.saveHA(ctx, cm, s)
	}
	if err := r.haServices(ctx, c, s, obs); err != nil {
		return ctrl.Result{}, err
	}
	for i, m := range s.Members {
		if !m.PodRequested {
			continue
		}
		// Capture each receipt before deleting any Pod. Its termination handler
		// can publish a later self-fence and its observation can be garbage
		// collected; neither invalidates the already verified shutdown barrier.
		if f := m.Fence; f != nil && ha.Bound(s, string(cm.UID), *f) && f.Stopped && f.Role == ha.Fenced && f.FenceTerm == s.Term && f.Operation == s.Operation {
			continue
		}
		o := obs[m.Name]
		if !o.Stopped || o.FenceTerm != s.Term || o.Role != ha.Fenced || o.Operation != s.Operation {
			return r.haStatus(ctx, c, s, obs, "AwaitingFencing", "Waiting for every assigned instance to stop before deletion")
		}
		s.Members[i].Fence = &o
		return ctrl.Result{}, r.saveHA(ctx, cm, s)
	}
	for _, m := range s.Members {
		p := &corev1.Pod{}
		err := r.reader().Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: m.Name}, p)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return ctrl.Result{}, err
		}
		if string(p.UID) != m.PodUID {
			return r.haStatus(ctx, c, s, obs, "RecoveryRequired", "Refusing to delete a replacement Pod")
		}
		if p.DeletionTimestamp.IsZero() {
			if err := r.Delete(ctx, p, client.Preconditions{UID: &p.UID}); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	before := c.DeepCopy()
	controllerutil.RemoveFinalizer(c, finalizer)
	return ctrl.Result{}, r.Patch(ctx, c, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}
