package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	pgv1 "github.com/vsamidurai/PostgresManager/api/v1alpha1"
	"github.com/vsamidurai/PostgresManager/internal/postgres"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const finalizer = "postgres.example.com/cluster-protection"

// Bootstrap state is separate from editable public status. Requests are recorded
// before creating storage/processes: an uncertain outcome must not reinitialize
// a database. This conservative slice requires intervention after such a failure.
type bootstrapState struct {
	ContractHash string    `json:"contractHash"`
	PVCRequested bool      `json:"pvcRequested"`
	PVCUID       types.UID `json:"pvcUID,omitempty"`
	PodRequested bool      `json:"podRequested"`
	PodUID       types.UID `json:"podUID,omitempty"`
}

type PostgresClusterReconciler struct {
	client.Client
	APIReader client.Reader
	Scheme    *runtime.Scheme
}

func (r *PostgresClusterReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// +kubebuilder:rbac:groups=postgres.example.com,resources=postgresclusters,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=postgres.example.com,resources=postgresclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=postgres.example.com,resources=postgresclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods;services;configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch

// Reconcile applies the initial bootstrap contract without an HA election.
func (r *PostgresClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	c := &pgv1.PostgresCluster{}
	if err := r.reader().Get(ctx, req.NamespacedName, c); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !c.DeletionTimestamp.IsZero() {
		return r.deleteCluster(ctx, c)
	}
	if err := postgres.Validate(c); err != nil {
		return r.report(ctx, c, nil, nil, "InvalidSpec", err.Error(), true)
	}
	n := postgres.ResourceNames(c)
	key := func(name string) types.NamespacedName {
		return types.NamespacedName{Namespace: c.Namespace, Name: name}
	}
	pod := &corev1.Pod{}
	podErr := r.reader().Get(ctx, key(n.Instance), pod)
	if podErr != nil && !apierrors.IsNotFound(podErr) {
		return ctrl.Result{}, podErr
	}
	if apierrors.IsNotFound(podErr) {
		pod = nil
	}
	pvc := &corev1.PersistentVolumeClaim{}
	pvcErr := r.reader().Get(ctx, key(n.PVC), pvc)
	if pvcErr != nil && !apierrors.IsNotFound(pvcErr) {
		return ctrl.Result{}, pvcErr
	}
	if apierrors.IsNotFound(pvcErr) {
		pvc = nil
	}

	cm := &corev1.ConfigMap{}
	err := r.reader().Get(ctx, key(n.Control), cm)
	if apierrors.IsNotFound(err) {
		if controllerutil.ContainsFinalizer(c, finalizer) || pvc != nil || pod != nil {
			return r.report(ctx, c, pod, pvc, "RecoveryRequired", "Bootstrap control state is missing; existing resources will not be adopted or recreated", true)
		}
		before := c.DeepCopy()
		controllerutil.AddFinalizer(c, finalizer)
		if err := r.Patch(ctx, c, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, err
		}
		cm = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: n.Control, Namespace: c.Namespace, Labels: postgres.Labels(c)}}
		if err := controllerutil.SetControllerReference(c, cm, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := encodeState(cm, bootstrapState{ContractHash: postgres.ContractHash(c)}); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, cm); err != nil {
			return ctrl.Result{}, err
		}
	} else if err != nil {
		return ctrl.Result{}, err
	}
	if !metav1.IsControlledBy(cm, c) || !controllerutil.ContainsFinalizer(c, finalizer) {
		return r.report(ctx, c, pod, pvc, "OwnershipConflict", "Bootstrap record ownership or cluster protection does not match", true)
	}
	var state bootstrapState
	if err := json.Unmarshal([]byte(cm.Data["state.json"]), &state); err != nil || state.ContractHash == "" {
		return r.report(ctx, c, pod, pvc, "RecoveryRequired", "Bootstrap control state is invalid", true)
	}
	if state.ContractHash != postgres.ContractHash(c) {
		return r.report(ctx, c, pod, pvc, "UpdateRequiresOperation", "Only storage growth is supported after bootstrap", true)
	}

	if pvc == nil {
		if state.PVCRequested {
			return r.report(ctx, c, pod, nil, "RecoveryRequired", "The requested data PVC is missing; refusing to create an empty replacement", true)
		}
		if err := r.checkSecret(ctx, c); err != nil {
			return r.report(ctx, c, pod, nil, "CredentialsUnavailable", err.Error(), true)
		}
		state.PVCRequested = true
		if err := r.saveState(ctx, cm, state); err != nil {
			return ctrl.Result{}, err
		}
		pvc = postgres.PVC(c)
		if err := r.Create(ctx, pvc); err != nil {
			return ctrl.Result{}, err
		}
	}
	if !state.PVCRequested || pvc.Labels[postgres.ClusterUIDLabel] != string(c.UID) || pvc.Labels[postgres.InstanceLabel] != n.Instance || metav1.GetControllerOf(pvc) != nil {
		return r.report(ctx, c, pod, pvc, "OwnershipConflict", "Data PVC identity does not match this cluster incarnation", true)
	}
	if !pvc.DeletionTimestamp.IsZero() || (state.PVCUID != "" && pvc.UID != state.PVCUID) {
		return r.report(ctx, c, pod, pvc, "RecoveryRequired", "Data PVC was deleted or replaced; refusing to reuse its name", true)
	}
	if state.PVCUID == "" {
		state.PVCUID = pvc.UID
		if err := r.saveState(ctx, cm, state); err != nil {
			return ctrl.Result{}, err
		}
	}
	if err := r.reconcileCapacity(ctx, c, pvc); err != nil {
		return r.report(ctx, c, pod, pvc, "StorageBlocked", err.Error(), true)
	}
	for _, headless := range []bool{false, true} {
		if err := r.reconcileService(ctx, c, headless); err != nil {
			return r.report(ctx, c, pod, pvc, "ServiceBlocked", err.Error(), true)
		}
	}

	if pod == nil {
		if state.PodRequested {
			return r.report(ctx, c, nil, pvc, "RecoveryRequired", "The requested Pod is missing; replacement requires a verified repair workflow", true)
		}
		if err := r.checkSecret(ctx, c); err != nil {
			return r.report(ctx, c, nil, pvc, "CredentialsUnavailable", err.Error(), true)
		}
		state.PodRequested = true
		if err := r.saveState(ctx, cm, state); err != nil {
			return ctrl.Result{}, err
		}
		pod = postgres.Pod(c)
		if err := controllerutil.SetControllerReference(c, pod, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, pod); err != nil {
			return ctrl.Result{}, err
		}
	}
	if !state.PodRequested || !metav1.IsControlledBy(pod, c) || pod.Labels[postgres.ClusterUIDLabel] != string(c.UID) || pod.Labels[postgres.InstanceLabel] != n.Instance {
		return r.report(ctx, c, nil, pvc, "OwnershipConflict", "Postgres Pod identity does not match the bootstrap record", true)
	}
	if state.PodUID != "" && pod.UID != state.PodUID {
		return r.report(ctx, c, nil, pvc, "RecoveryRequired", "Pod UID changed outside an authorized operation", true)
	}
	if state.PodUID == "" {
		state.PodUID = pod.UID
		if err := r.saveState(ctx, cm, state); err != nil {
			return ctrl.Result{}, err
		}
	}
	if !pod.DeletionTimestamp.IsZero() {
		return r.report(ctx, c, pod, pvc, "RecoveryRequired", "Postgres Pod is terminating; automatic replacement is not implemented", true)
	}
	if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
		return r.report(ctx, c, pod, pvc, "RecoveryRequired", "Postgres Pod exited; inspect it before requesting recovery", true)
	}
	if !podReady(pod) {
		return r.report(ctx, c, pod, pvc, "WaitingForPostgres", "Waiting for the Postgres startup and primary readiness probes", false)
	}
	requested := resource.MustParse(c.Spec.Storage.Data.Size)
	capacity := pvc.Status.Capacity[corev1.ResourceStorage]
	if capacity.Cmp(requested) < 0 {
		return r.report(ctx, c, pod, pvc, "Resizing", "Waiting for the PVC to report the requested capacity", false)
	}
	for _, condition := range pvc.Status.Conditions {
		if condition.Type == corev1.PersistentVolumeClaimFileSystemResizePending && condition.Status == corev1.ConditionTrue {
			return r.report(ctx, c, pod, pvc, "FilesystemResizePending", "Filesystem expansion has not completed; the operator will not restart the Pod", false)
		}
	}
	return r.report(ctx, c, pod, pvc, "Reconciled", "Postgres is available and the requested resources are reconciled", false)
}

func encodeState(cm *corev1.ConfigMap, state bootstrapState) error {
	b, err := json.Marshal(state)
	if err == nil {
		cm.Data = map[string]string{"state.json": string(b)}
	}
	return err
}

func (r *PostgresClusterReconciler) saveState(ctx context.Context, cm *corev1.ConfigMap, state bootstrapState) error {
	if err := encodeState(cm, state); err != nil {
		return err
	}
	return r.Update(ctx, cm) // resourceVersion is the compare-and-swap guard.
}

func (r *PostgresClusterReconciler) checkSecret(ctx context.Context, c *pgv1.PostgresCluster) error {
	secret := &corev1.Secret{}
	err := r.reader().Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: c.Spec.Bootstrap.InitDB.SuperuserSecretRef.Name}, secret)
	if err != nil {
		return fmt.Errorf("bootstrap Secret is not accessible: %w", err)
	}
	if len(secret.Data["password"]) == 0 {
		return fmt.Errorf("bootstrap Secret must contain a nonempty password key")
	}
	return nil
}

func (r *PostgresClusterReconciler) reconcileCapacity(ctx context.Context, c *pgv1.PostgresCluster, pvc *corev1.PersistentVolumeClaim) error {
	if len(pvc.Spec.AccessModes) != 1 || pvc.Spec.AccessModes[0] != c.Spec.Storage.Data.AccessMode {
		return fmt.Errorf("PVC access mode does not match the cluster")
	}
	if pvc.Spec.VolumeMode != nil && *pvc.Spec.VolumeMode != corev1.PersistentVolumeFilesystem {
		return fmt.Errorf("a filesystem volume is required")
	}
	if desired := c.Spec.Storage.Data.StorageClassName; desired != "" && (pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != desired) {
		return fmt.Errorf("PVC storage class does not match the cluster")
	}
	wanted := resource.MustParse(c.Spec.Storage.Data.Size)
	current := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if wanted.Cmp(current) < 0 {
		return fmt.Errorf("storage cannot shrink")
	}
	if wanted.Cmp(current) == 0 {
		return nil
	}
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName == "" {
		return fmt.Errorf("volume expansion requires a StorageClass")
	}
	sc := &storagev1.StorageClass{}
	if err := r.reader().Get(ctx, types.NamespacedName{Name: *pvc.Spec.StorageClassName}, sc); err != nil {
		return fmt.Errorf("cannot inspect StorageClass: %w", err)
	}
	if sc.AllowVolumeExpansion == nil || !*sc.AllowVolumeExpansion {
		return fmt.Errorf("StorageClass does not allow volume expansion")
	}
	before := pvc.DeepCopy()
	pvc.Spec.Resources.Requests[corev1.ResourceStorage] = wanted
	return r.Patch(ctx, pvc, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}

func (r *PostgresClusterReconciler) reconcileService(ctx context.Context, c *pgv1.PostgresCluster, headless bool) error {
	wanted := postgres.Service(c, headless)
	current := &corev1.Service{}
	err := r.reader().Get(ctx, client.ObjectKeyFromObject(wanted), current)
	if apierrors.IsNotFound(err) {
		if err := controllerutil.SetControllerReference(c, wanted, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, wanted)
	}
	if err != nil {
		return err
	}
	if !metav1.IsControlledBy(current, c) {
		return fmt.Errorf("Service %s belongs to another owner", current.Name)
	}
	if (current.Spec.ClusterIP == corev1.ClusterIPNone) != headless || current.Spec.Type != corev1.ServiceTypeClusterIP || len(current.Spec.ExternalIPs) != 0 {
		return fmt.Errorf("Service %s has unsupported networking configuration", current.Name)
	}
	if reflect.DeepEqual(current.Spec.Selector, wanted.Spec.Selector) && reflect.DeepEqual(current.Spec.Ports, wanted.Spec.Ports) {
		return nil
	}
	before := current.DeepCopy()
	current.Spec.Selector = wanted.Spec.Selector
	current.Spec.Ports = wanted.Spec.Ports
	return r.Patch(ctx, current, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}

func (r *PostgresClusterReconciler) deleteCluster(ctx context.Context, c *pgv1.PostgresCluster) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(c, finalizer) {
		return ctrl.Result{}, nil
	}
	pod := &corev1.Pod{}
	err := r.reader().Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: postgres.ResourceNames(c).Instance}, pod)
	if err == nil {
		if !metav1.IsControlledBy(pod, c) {
			return ctrl.Result{}, fmt.Errorf("refusing to delete a Pod owned by another cluster")
		}
		if pod.DeletionTimestamp.IsZero() {
			if err := r.Delete(ctx, pod, client.Preconditions{UID: &pod.UID}); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	if !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	before := c.DeepCopy()
	controllerutil.RemoveFinalizer(c, finalizer)
	return ctrl.Result{}, r.Patch(ctx, c, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}

func podReady(pod *corev1.Pod) bool {
	if pod == nil || !pod.DeletionTimestamp.IsZero() || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func (r *PostgresClusterReconciler) report(ctx context.Context, c *pgv1.PostgresCluster, pod *corev1.Pod, pvc *corev1.PersistentVolumeClaim, reason, message string, blocked bool) (ctrl.Result, error) {
	before := c.DeepCopy()
	n := postgres.ResourceNames(c)
	c.Status.ObservedGeneration = c.Generation
	c.Status.InstanceID, c.Status.PodName, c.Status.PVCName = n.Instance, n.Instance, n.PVC
	c.Status.Endpoint = n.Writer + "." + c.Namespace + ".svc:5432"
	c.Status.PodUID, c.Status.PVCUID, c.Status.VolumeCapacity = "", "", ""
	if pod != nil {
		c.Status.PodUID = string(pod.UID)
	}
	if pvc != nil {
		c.Status.PVCUID = string(pvc.UID)
		if q, ok := pvc.Status.Capacity[corev1.ResourceStorage]; ok {
			c.Status.VolumeCapacity = q.String()
		}
	}
	c.Status.ReadyInstances = 0
	available := podReady(pod)
	if available {
		c.Status.ReadyInstances = 1
	}
	for conditionType, value := range map[string]bool{"Ready": reason == "Reconciled", "Available": available, "Blocked": blocked} {
		status := metav1.ConditionFalse
		if value {
			status = metav1.ConditionTrue
		}
		meta.SetStatusCondition(&c.Status.Conditions, metav1.Condition{Type: conditionType, Status: status, ObservedGeneration: c.Generation, Reason: reason, Message: message})
	}
	if !reflect.DeepEqual(before.Status, c.Status) {
		if err := r.Status().Patch(ctx, c, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *PostgresClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&pgv1.PostgresCluster{}).
		Owns(&corev1.Pod{}).Owns(&corev1.Service{}).Owns(&corev1.ConfigMap{}).
		Watches(&corev1.PersistentVolumeClaim{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, object client.Object) []reconcile.Request {
			if name := object.GetLabels()[postgres.ClusterLabel]; name != "" {
				return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: object.GetNamespace(), Name: name}}}
			}
			return nil
		})).Complete(r)
}
