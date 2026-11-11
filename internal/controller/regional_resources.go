package controller

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"time"

	pgv1 "github.com/vsamidurai/PostgresManager/api/v1alpha1"
	"github.com/vsamidurai/PostgresManager/internal/ha"
	"github.com/vsamidurai/PostgresManager/internal/postgres"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func (r *PostgresClusterReconciler) checkHASecrets(ctx context.Context, c *pgv1.PostgresCluster) error {
	if err := r.checkSecret(ctx, c); err != nil {
		return err
	}
	for _, name := range []string{c.Spec.Replication.CredentialsSecretRef.Name, c.Spec.Replication.TLSSecretRef.Name} {
		s := &corev1.Secret{}
		if err := r.reader().Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: name}, s); err != nil {
			return fmt.Errorf("HA Secret %s is unavailable", name)
		}
		if name == c.Spec.Replication.CredentialsSecretRef.Name && len(s.Data["password"]) == 0 {
			return fmt.Errorf("replication Secret requires a password")
		}
		if name == c.Spec.Replication.TLSSecretRef.Name {
			pair, err := tls.X509KeyPair(s.Data["tls.crt"], s.Data["tls.key"])
			if err != nil {
				return fmt.Errorf("TLS Secret requires a valid certificate/key pair and ca.crt")
			}
			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(s.Data["ca.crt"]) {
				return fmt.Errorf("TLS Secret has no valid trusted CA certificates")
			}
			leaf, err := x509.ParseCertificate(pair.Certificate[0])
			if err != nil {
				return fmt.Errorf("TLS leaf certificate is invalid")
			}
			intermediates := x509.NewCertPool()
			for _, der := range pair.Certificate[1:] {
				cert, err := x509.ParseCertificate(der)
				if err != nil {
					return fmt.Errorf("TLS certificate chain is invalid")
				}
				intermediates.AddCert(cert)
			}
			for ordinal := 1; ordinal <= int(c.Spec.Instances); ordinal++ {
				host := postgres.InstanceNames(c, ordinal).Instance + "." + postgres.ResourceNames(c).Headless + "." + c.Namespace + ".svc"
				if _, err := leaf.Verify(x509.VerifyOptions{DNSName: host, Roots: roots, Intermediates: intermediates, CurrentTime: r.now()}); err != nil {
					return fmt.Errorf("TLS Secret does not provide a currently trusted server certificate for %s", host)
				}
			}
		}
	}
	return nil
}

func (r *PostgresClusterReconciler) haMember(ctx context.Context, c *pgv1.PostgresCluster, cm *corev1.ConfigMap, s *ha.State, index int) error {
	m := &s.Members[index]
	key := func(name string) types.NamespacedName {
		return types.NamespacedName{Namespace: c.Namespace, Name: name}
	}
	pvc := &corev1.PersistentVolumeClaim{}
	err := r.reader().Get(ctx, key(m.Name+"-data"), pvc)
	if apierrors.IsNotFound(err) {
		if m.PVCRequested {
			return fmt.Errorf("data PVC for %s is missing; refusing an empty replacement", m.Name)
		}
		m.PVCRequested = true
		if err := r.saveHA(ctx, cm, *s); err != nil {
			return err
		}
		pvc = postgres.HAPVC(c, m.Name)
		if err := r.Create(ctx, pvc); err != nil {
			if createRejected(err) {
				m.PVCRequested = false
				s.RetryAfter = r.now().Add(retryDelay)
				return errors.Join(err, r.saveHA(ctx, cm, *s))
			}
			return err
		}
	} else if err != nil {
		return err
	}
	if !m.PVCRequested || (m.PVCUID == "" && (pvc.Labels[postgres.ClusterUIDLabel] != string(c.UID) || pvc.Labels[postgres.InstanceLabel] != m.Name)) || metav1.GetControllerOf(pvc) != nil || !pvc.DeletionTimestamp.IsZero() || (m.PVCUID != "" && m.PVCUID != string(pvc.UID)) {
		return fmt.Errorf("data identity mismatch for %s", m.Name)
	}
	if m.PVCUID == "" {
		m.PVCUID = string(pvc.UID)
		if err := r.saveHA(ctx, cm, *s); err != nil {
			return err
		}
	}
	if err := r.haLabels(ctx, c, pvc, m.Name); err != nil {
		return err
	}
	if err := r.reconcileCapacity(ctx, c, pvc); err != nil {
		return err
	}
	for _, object := range postgres.HAIdentityObjects(c, m.Name) {
		if err := r.ensureHAObject(ctx, c, object); err != nil {
			return err
		}
	}
	observation := &corev1.ConfigMap{}
	err = r.reader().Get(ctx, key(postgres.ObservationName(m.Name)), observation)
	if apierrors.IsNotFound(err) {
		if m.PodRequested {
			return fmt.Errorf("observation record for %s is missing", m.Name)
		}
		observation = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: postgres.ObservationName(m.Name), Namespace: c.Namespace, Labels: postgres.HALabels(c, m.Name)}}
		if err := controllerutil.SetControllerReference(c, observation, r.Scheme); err != nil {
			return err
		}
		if err := r.Create(ctx, observation); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if !metav1.IsControlledBy(observation, c) {
		return fmt.Errorf("observation record has foreign ownership")
	}
	if err := r.haLabels(ctx, c, observation, m.Name); err != nil {
		return err
	}
	pod := &corev1.Pod{}
	err = r.reader().Get(ctx, key(m.Name), pod)
	if apierrors.IsNotFound(err) {
		if m.PodRequested {
			return fmt.Errorf("pod %s is missing; fencing evidence is required before repair", m.Name)
		}
		m.PodRequested = true
		if err := r.saveHA(ctx, cm, *s); err != nil {
			return err
		}
		pod = postgres.HAPod(c, *m, string(cm.UID))
		if err := controllerutil.SetControllerReference(c, pod, r.Scheme); err != nil {
			return err
		}
		if err := r.Create(ctx, pod); err != nil {
			if createRejected(err) {
				m.PodRequested = false
				s.RetryAfter = r.now().Add(retryDelay)
				return errors.Join(err, r.saveHA(ctx, cm, *s))
			}
			return err
		}
	} else if err != nil {
		return err
	}
	if !metav1.IsControlledBy(pod, c) || (m.PodUID == "" && pod.Labels[postgres.InstanceLabel] != m.Name) || !m.PodRequested || (m.PodUID != "" && m.PodUID != string(pod.UID)) {
		return fmt.Errorf("pod identity changed for %s", m.Name)
	}
	if err := r.haLabels(ctx, c, pod, m.Name); err != nil {
		return err
	}
	if m.PodUID == "" {
		m.PodUID = string(pod.UID)
		return r.saveHA(ctx, cm, *s)
	}
	return nil
}

func (r *PostgresClusterReconciler) haLabels(ctx context.Context, c *pgv1.PostgresCluster, object client.Object, instance string) error {
	before := object.DeepCopyObject().(client.Object)
	labels := maps.Clone(object.GetLabels())
	if labels == nil {
		labels = map[string]string{}
	}
	maps.Copy(labels, postgres.HALabels(c, instance))
	object.SetLabels(labels)
	if reflect.DeepEqual(before.GetLabels(), labels) {
		return nil
	}
	return r.Patch(ctx, object, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}

func (r *PostgresClusterReconciler) ensureHAObject(ctx context.Context, c *pgv1.PostgresCluster, wanted client.Object) error {
	current := wanted.DeepCopyObject().(client.Object)
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
		return fmt.Errorf("foreign resource %s", current.GetName())
	}
	before := current.DeepCopyObject().(client.Object)
	labels := maps.Clone(current.GetLabels())
	if labels == nil {
		labels = map[string]string{}
	}
	maps.Copy(labels, wanted.GetLabels())
	current.SetLabels(labels)
	switch o := current.(type) {
	case *rbacv1.Role:
		o.Rules = wanted.(*rbacv1.Role).Rules
	case *rbacv1.RoleBinding:
		w := wanted.(*rbacv1.RoleBinding)
		if o.RoleRef != w.RoleRef {
			return fmt.Errorf("identity RoleBinding was changed")
		}
		o.Subjects = w.Subjects
	case *corev1.Service:
		w := wanted.(*corev1.Service)
		if (o.Spec.ClusterIP == corev1.ClusterIPNone) != (w.Spec.ClusterIP == corev1.ClusterIPNone) || o.Spec.Type != corev1.ServiceTypeClusterIP || len(o.Spec.ExternalIPs) != 0 {
			return fmt.Errorf("unsupported managed Service networking")
		}
		o.Spec.Selector = w.Spec.Selector
		o.Spec.Ports = w.Spec.Ports
		o.Spec.PublishNotReadyAddresses = w.Spec.PublishNotReadyAddresses
	}
	if reflect.DeepEqual(before, current) {
		return nil
	}
	return r.Patch(ctx, current, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}

func (r *PostgresClusterReconciler) haServices(ctx context.Context, c *pgv1.PostgresCluster, s ha.State, obs map[string]ha.Observation) error {
	headless := postgres.Service(c, true)
	headless.Spec.Selector = map[string]string{postgres.ClusterUIDLabel: string(c.UID)}
	// Internal discovery must resolve replicas before synchronous readiness.
	// The application writer Service always excludes unready Pods.
	headless.Spec.PublishNotReadyAddresses = true
	if err := r.ensureHAObject(ctx, c, headless); err != nil {
		return err
	}
	writer := postgres.Service(c, false)
	writer.Spec.Selector[postgres.InstanceLabel] = "no-writer"
	if o := obs[s.Primary]; s.Phase == ha.Running && o.Role == ha.Primary && ha.Fresh(o, r.now()) {
		writer.Spec.Selector[postgres.InstanceLabel] = s.Primary
	}
	return r.ensureHAObject(ctx, c, writer)
}

func (r *PostgresClusterReconciler) haStatus(ctx context.Context, c *pgv1.PostgresCluster, s ha.State, obs map[string]ha.Observation, reason, message string) (ctrl.Result, error) {
	before := c.DeepCopy()
	c.Status.ObservedGeneration = c.Generation
	c.Status.PrimaryInstance = s.Primary
	c.Status.AuthorityTerm = s.Term
	c.Status.HAPhase = s.Phase
	c.Status.Endpoint = postgres.ResourceNames(c).Writer + "." + c.Namespace + ".svc:5432"
	c.Status.ReadyInstances = 0
	c.Status.Instances = nil
	available := false
	for _, m := range s.Members {
		o := obs[m.Name]
		c.Status.Instances = append(c.Status.Instances, pgv1.PostgresInstanceStatus{Name: m.Name, PodUID: m.PodUID, PVCUID: m.PVCUID, Role: o.Role, SystemIdentifier: o.SystemID, Timeline: o.Timeline, ReplayLSN: o.ReplayLSN, Message: o.Message})
		p := &corev1.Pod{}
		err := r.reader().Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: m.Name}, p)
		if err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		if err == nil && string(p.UID) == m.PodUID && podReady(p) && ha.Fresh(o, r.now()) && (o.Role == ha.Primary || o.Role == ha.Standby) {
			c.Status.ReadyInstances++
			if m.Name == s.Primary && o.Role == ha.Primary && s.Phase == ha.Running {
				available = true
			}
		}
	}
	ready := reason == "Reconciled" && available && c.Status.ReadyInstances == c.Spec.Instances
	for kind, value := range map[string]bool{"Ready": ready, "Available": available, "ReplicationHealthy": ready, "Blocked": reason != "Reconciled"} {
		status := metav1.ConditionFalse
		if value {
			status = metav1.ConditionTrue
		}
		meta.SetStatusCondition(&c.Status.Conditions, metav1.Condition{Type: kind, Status: status, Reason: reason, Message: message, ObservedGeneration: c.Generation})
	}
	if !reflect.DeepEqual(before.Status, c.Status) {
		if err := r.Status().Patch(ctx, c, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}
