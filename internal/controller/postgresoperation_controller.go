package controller

import (
	"context"
	"reflect"
	"time"

	pgv1 "github.com/vsamidurai/PostgresManager/api/v1alpha1"
	"github.com/vsamidurai/PostgresManager/internal/ha"
	"github.com/vsamidurai/PostgresManager/internal/postgres"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type PostgresOperationReconciler struct{ *PostgresClusterReconciler }

// +kubebuilder:rbac:groups=postgres.example.com,resources=postgresoperations,verbs=get;list;watch
// +kubebuilder:rbac:groups=postgres.example.com,resources=postgresoperations/status,verbs=get;patch

// Reconcile reserves a request; only the cluster reconciler advances authority.
// Deleting an operation does not cancel an already reserved transition.
func (r *PostgresOperationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	op := &pgv1.PostgresOperation{}
	if err := r.reader().Get(ctx, req.NamespacedName, op); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	report := func(phase, message string) (ctrl.Result, error) {
		before := op.DeepCopy()
		op.Status = pgv1.PostgresOperationStatus{Phase: phase, Message: message, ObservedGeneration: op.Generation}
		if !reflect.DeepEqual(before.Status, op.Status) {
			if err := r.Status().Patch(ctx, op, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				return ctrl.Result{}, err
			}
		}
		if phase == "Succeeded" || phase == "Rejected" {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	if op.Status.Phase == "Succeeded" || op.Status.Phase == "Rejected" {
		return ctrl.Result{}, nil
	}
	cluster := &pgv1.PostgresCluster{}
	if err := r.reader().Get(ctx, types.NamespacedName{Namespace: op.Namespace, Name: op.Spec.ClusterName}, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return report("Rejected", "Referenced cluster no longer exists")
		}
		return ctrl.Result{}, err
	}
	if string(cluster.UID) != op.Spec.ClusterUID || cluster.Spec.Replication == nil || !cluster.DeletionTimestamp.IsZero() {
		return report("Rejected", "Operation does not match an active managed HA cluster incarnation")
	}
	cm := &corev1.ConfigMap{}
	if err := r.reader().Get(ctx, types.NamespacedName{Namespace: op.Namespace, Name: postgres.HAControlName(cluster)}, cm); err != nil {
		if apierrors.IsNotFound(err) {
			return report("Pending", "Waiting for regional authority")
		}
		return ctrl.Result{}, err
	}
	state, err := ha.Decode(cm.Data["state.json"])
	if err != nil || !metav1.IsControlledBy(cm, cluster) {
		return report("Rejected", "Regional authority is invalid")
	}
	if state.CompletedOperation == string(op.UID) {
		if state.CompletedOutcome == "Rejected" {
			return report("Rejected", "Requested promotion failed; automatic recovery superseded the operation")
		}
		return report("Succeeded", "Requested operation completed")
	}
	if _, ok := state.Member(op.Spec.TargetInstance); !ok {
		return report("Rejected", "Target is not an assigned instance")
	}
	if state.Request != nil && state.Request.UID == string(op.UID) {
		return report("Running", "Regional controller is executing the reserved operation")
	}
	if state.Request != nil || state.Phase != ha.Running {
		return report("Pending", "Another regional transition is in progress")
	}
	if op.Spec.TargetInstance == state.Primary {
		return report("Rejected", "Target must be a different instance from the current primary")
	}
	state.Request = &ha.Request{Name: op.Name, UID: string(op.UID), Type: op.Spec.Type, Target: op.Spec.TargetInstance}
	if err := r.saveHA(ctx, cm, state); err != nil {
		return ctrl.Result{}, err
	}
	return report("Running", "Operation reserved using regional authority concurrency checks")
}

func (r *PostgresOperationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&pgv1.PostgresOperation{}, builder.WithPredicates(clusterPredicate())).Complete(r)
}
