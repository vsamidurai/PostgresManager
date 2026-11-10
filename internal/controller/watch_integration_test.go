//go:build integration

package controller

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	pgv1 "github.com/vsamidurai/PostgresManager/api/v1alpha1"
	"github.com/vsamidurai/PostgresManager/internal/postgres"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func eventually(t *testing.T, check func() (bool, error)) {
	t.Helper()
	must(t, wait.PollUntilContextTimeout(t.Context(), 100*time.Millisecond, 45*time.Second, true, func(context.Context) (bool, error) { return check() }))
}

func TestManagerWatchesFleetAndFiltersCache(t *testing.T) {
	f := newFixture(t, true)
	outside := newFixture(t, true)
	unmanaged := []client.Object{postgres.Pod(f.c), postgres.PVC(f.c), postgres.Service(f.c, false), &corev1.ConfigMap{}}
	for _, object := range unmanaged {
		object.SetNamespace(f.c.Namespace)
		object.SetName("unmanaged")
		object.SetLabels(nil)
		must(t, testClient.Create(t.Context(), object))
	}
	mgr, err := ctrl.NewManager(testConfig, ctrl.Options{
		Scheme: testScheme, Cache: CacheOptions(f.c.Namespace),
		Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0",
		// -count repeats sequential managers in one process. controller-runtime
		// keeps its name registry after shutdown; production validation stays on.
		Controller: config.Controller{SkipNameValidation: ptr.To(true)},
	})
	must(t, err)
	r := &PostgresClusterReconciler{Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), Scheme: testScheme}
	must(t, r.SetupWithManager(mgr))
	ready := CacheReady(mgr.GetCache())
	if ready(httptest.NewRequest("GET", "/readyz", nil)) == nil {
		t.Fatal("unstarted cache reported ready")
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("manager did not stop")
		}
	})
	// Reconcile a fleet through real informers and the workqueue, without calling
	// Reconcile directly. Each CR has its own volume/process identity.
	for i := range 20 {
		c := f.c.DeepCopy()
		c.ObjectMeta = metav1.ObjectMeta{Name: fmt.Sprintf("fleet-%02d", i), Namespace: f.c.Namespace}
		must(t, testClient.Create(t.Context(), c))
	}
	eventually(t, func() (bool, error) {
		clusters := &pgv1.PostgresClusterList{}
		if err := testClient.List(t.Context(), clusters, client.InNamespace(f.c.Namespace)); err != nil {
			return false, err
		}
		if len(clusters.Items) != 21 {
			return false, nil
		}
		identities := map[string]bool{}
		for _, c := range clusters.Items {
			if c.Status.PodUID == "" || c.Status.PVCUID == "" {
				return false, nil
			}
			if identities[c.Status.PodUID] || identities[c.Status.PVCUID] {
				return false, fmt.Errorf("fleet resource identities overlap")
			}
			identities[c.Status.PodUID], identities[c.Status.PVCUID] = true, true
		}
		return true, nil
	})
	must(t, ready(httptest.NewRequest("GET", "/readyz", nil)))
	for _, object := range unmanaged {
		if err := mgr.GetClient().Get(t.Context(), client.ObjectKeyFromObject(object), object); !apierrors.IsNotFound(err) {
			t.Fatalf("unmanaged %T entered the cache: %v", object, err)
		}
	}
	var uncached *cache.ErrResourceNotCached
	if err := mgr.GetClient().Get(t.Context(), f.key("bootstrap"), &corev1.Secret{}); !errors.As(err, &uncached) {
		t.Fatalf("Secret read did not reject an undeclared informer: %v", err)
	}
	must(t, testClient.Get(t.Context(), client.ObjectKeyFromObject(outside.c), outside.c))
	if outside.c.Status.PodUID != "" {
		t.Fatal("namespace-scoped manager reconciled a different namespace")
	}

	pod, pvc := f.pod(t), f.pvc(t)
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	must(t, testClient.Status().Update(t.Context(), pod))
	pvc.Status.Capacity = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}
	must(t, testClient.Status().Update(t.Context(), pvc))
	eventually(t, func() (bool, error) {
		if err := testClient.Get(t.Context(), client.ObjectKeyFromObject(f.c), f.c); err != nil {
			return false, err
		}
		return meta.IsStatusConditionTrue(f.c.Status.Conditions, "Ready"), nil
	})

	// Removing a selector label produces a watch deletion. A fresh API read must
	// repair it, otherwise subsequent readiness changes would be invisible.
	pod = f.pod(t)
	delete(pod.Labels, postgres.ManagedByLabel)
	must(t, testClient.Update(t.Context(), pod))
	eventually(t, func() (bool, error) {
		if err := testClient.Get(t.Context(), client.ObjectKeyFromObject(pod), pod); err != nil {
			return false, err
		}
		return pod.Labels[postgres.ManagedByLabel] == postgres.ManagedByValue, nil
	})
	pod.Status.Conditions[0].Status = corev1.ConditionFalse
	must(t, testClient.Status().Update(t.Context(), pod))
	eventually(t, func() (bool, error) {
		if err := testClient.Get(t.Context(), client.ObjectKeyFromObject(f.c), f.c); err != nil {
			return false, err
		}
		return meta.IsStatusConditionFalse(f.c.Status.Conditions, "Available"), nil
	})

	// A deletionTimestamp does not change generation. The CR predicate must
	// still enqueue finalization after status-only events have been filtered.
	must(t, testClient.Delete(t.Context(), f.c))
	eventually(t, func() (bool, error) {
		err := testClient.Get(t.Context(), client.ObjectKeyFromObject(pod), pod)
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return err == nil && !pod.DeletionTimestamp.IsZero(), err
	})
	if err := testClient.Delete(t.Context(), pod, client.GracePeriodSeconds(0)); err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
	eventually(t, func() (bool, error) {
		err := testClient.Get(t.Context(), client.ObjectKeyFromObject(f.c), &pgv1.PostgresCluster{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	})
	if f.pvc(t).UID != pvc.UID {
		t.Fatal("watch-driven finalization removed data")
	}
}
