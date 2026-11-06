//go:build integration

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	pgv1 "github.com/vsamidurai/PostgresManager/api/v1alpha1"
	"github.com/vsamidurai/PostgresManager/internal/postgres"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func TestRejectedCreatesCanBeRetried(t *testing.T) {
	for _, target := range []client.Object{&corev1.ConfigMap{}, &corev1.PersistentVolumeClaim{}, &corev1.Pod{}} {
		t.Run(fmt.Sprintf("%T", target), func(t *testing.T) {
			f := newFixture(t, true)
			clock := clocktesting.NewFakeClock(time.Now())
			f.r.Clock = clock
			attempts := 0
			f.r.Client = interceptor.NewClient(testClient, interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if reflect.TypeOf(obj) == reflect.TypeOf(target) {
					attempts++
					if attempts == 1 {
						return apierrors.NewForbidden(schema.GroupResource{Resource: "test"}, obj.GetName(), fmt.Errorf("quota temporarily exhausted"))
					}
				}
				return c.Create(ctx, obj, opts...)
			}})
			_, err := f.r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.c)})
			if !apierrors.IsForbidden(err) {
				t.Fatalf("expected injected rejection, got %v", err)
			}
			if _, controlRecord := target.(*corev1.ConfigMap); !controlRecord {
				// Checkpoint events must not bypass retry backoff, including after
				// a new reconciler takes over with no in-memory retry history.
				f.r = &PostgresClusterReconciler{Client: f.r.Client, APIReader: testClient, Scheme: testScheme, Clock: clock}
				for range 5 {
					f.reconcile(t)
				}
				f.condition(t, "Blocked", "CreateRejected", metav1.ConditionTrue)
				if attempts != 1 {
					t.Fatalf("checkpoint events retried immediately %d times", attempts)
				}
				clock.Step(time.Minute)
			}
			f.reconcile(t)
			f.condition(t, "Blocked", "WaitingForPostgres", metav1.ConditionFalse)
			if attempts != 2 || f.pod(t).UID == "" || f.pvc(t).UID == "" {
				t.Fatal("definitively rejected create did not recover")
			}
		})
	}
}

func TestAmbiguousCreateOutcomes(t *testing.T) {
	for _, target := range []client.Object{&corev1.ConfigMap{}, &corev1.PersistentVolumeClaim{}, &corev1.Pod{}} {
		for _, accepted := range []bool{false, true} {
			t.Run(fmt.Sprintf("%T/accepted=%v", target, accepted), func(t *testing.T) {
				f := newFixture(t, true)
				attempts := 0
				f.r.Client = interceptor.NewClient(testClient, interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if reflect.TypeOf(obj) == reflect.TypeOf(target) {
						attempts++
						if attempts == 1 {
							if accepted {
								if err := c.Create(ctx, obj, opts...); err != nil {
									return err
								}
							}
							return apierrors.NewTimeoutError("create response lost", 0)
						}
					}
					return c.Create(ctx, obj, opts...)
				}})
				_, err := f.r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.c)})
				if !apierrors.IsTimeout(err) {
					t.Fatalf("expected timeout, got %v", err)
				}
				for range 3 {
					f.reconcile(t)
				}
				_, controlRecord := target.(*corev1.ConfigMap)
				if accepted || controlRecord {
					f.condition(t, "Blocked", "WaitingForPostgres", metav1.ConditionFalse)
					_ = f.pod(t)
					_ = f.pvc(t)
					wantAttempts := 1
					if !accepted {
						wantAttempts = 2
					} // Empty control state is safe to retry.
					if attempts != wantAttempts {
						t.Fatalf("create attempts = %d, want %d", attempts, wantAttempts)
					}
				} else {
					f.condition(t, "Blocked", "RecoveryRequired", metav1.ConditionTrue)
					if attempts != 1 {
						t.Fatal("ambiguous database resource create was retried")
					}
					if err := testClient.Get(t.Context(), f.key(postgres.ResourceNames(f.c).Instance), &corev1.Pod{}); !apierrors.IsNotFound(err) {
						t.Fatalf("uncertain bootstrap started a database: %v", err)
					}
				}
			})
		}
	}
}

func TestFailedIntentResetRemainsConservative(t *testing.T) {
	f := newFixture(t, true)
	f.r.Client = interceptor.NewClient(testClient, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*corev1.PersistentVolumeClaim); ok {
				return apierrors.NewForbidden(schema.GroupResource{Resource: "persistentvolumeclaims"}, obj.GetName(), fmt.Errorf("quota"))
			}
			return c.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if cm, ok := obj.(*corev1.ConfigMap); ok {
				var state bootstrapState
				if err := json.Unmarshal([]byte(cm.Data["state.json"]), &state); err != nil {
					return err
				}
				if !state.PVCRequested {
					return apierrors.NewTimeoutError("intent reset outcome unknown", 0)
				}
			}
			return c.Update(ctx, obj, opts...)
		},
	})
	_, err := f.r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.c)})
	if err == nil {
		t.Fatal("injected failure was ignored")
	}
	f.r.Client = testClient
	f.reconcile(t)
	f.condition(t, "Blocked", "RecoveryRequired", metav1.ConditionTrue)
}

func TestCorruptControlStateDoesNotChangeDatabaseResources(t *testing.T) {
	for _, corrupt := range []string{
		`{"contractHash":"%s","pvcUID":"invented"}`,
		`{"contractHash":"%s","podRequested":true}`,
		`{"contractHash":"%s","pvcRequested":true,"pvcUID":"invented","podUID":"invented"}`,
	} {
		f := newFixture(t, true)
		f.reconcile(t)
		pod, pvc := f.pod(t), f.pvc(t)
		cm := &corev1.ConfigMap{}
		must(t, testClient.Get(t.Context(), f.key(postgres.ResourceNames(f.c).Control), cm))
		cm.Data["state.json"] = fmt.Sprintf(corrupt, postgres.ContractHash(f.c))
		must(t, testClient.Update(t.Context(), cm))
		f.reconcile(t)
		f.condition(t, "Blocked", "RecoveryRequired", metav1.ConditionTrue)
		if f.pod(t).ResourceVersion != pod.ResourceVersion || f.pvc(t).ResourceVersion != pvc.ResourceVersion {
			t.Fatal("corrupt control state mutated database resources")
		}
	}
}

func TestServiceReadinessDriftIsRepaired(t *testing.T) {
	f := newFixture(t, true)
	f.reconcile(t)
	for _, name := range []string{postgres.ResourceNames(f.c).Writer, postgres.ResourceNames(f.c).Headless} {
		svc := &corev1.Service{}
		must(t, testClient.Get(t.Context(), f.key(name), svc))
		originalIP := svc.Spec.ClusterIP
		svc.Spec.PublishNotReadyAddresses = true
		svc.Spec.Selector = map[string]string{"wrong": "instance"}
		delete(svc.Labels, postgres.ManagedByLabel)
		svc.Labels["user-label"] = "keep"
		must(t, testClient.Update(t.Context(), svc))
		f.reconcile(t)
		must(t, testClient.Get(t.Context(), f.key(name), svc))
		if svc.Spec.PublishNotReadyAddresses || svc.Spec.Selector[postgres.ClusterUIDLabel] != string(f.c.UID) || svc.Spec.ClusterIP != originalIP || svc.Labels["user-label"] != "keep" || svc.Labels[postgres.ManagedByLabel] != postgres.ManagedByValue {
			t.Fatalf("unsafe Service drift not repaired: %+v", svc)
		}
	}
}

func TestReadyStatusAndPrimaryFailure(t *testing.T) {
	f := newFixture(t, true)
	f.reconcile(t)
	pod, pvc := f.pod(t), f.pvc(t)
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	must(t, testClient.Status().Update(t.Context(), pod))
	pvc.Status.Capacity = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}
	must(t, testClient.Status().Update(t.Context(), pvc))
	f.reconcile(t)
	version := f.c.ResourceVersion
	for range 3 {
		result, err := f.r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.c)})
		must(t, err)
		if !result.IsZero() {
			t.Fatalf("healthy cluster polls: %+v", result)
		}
	}
	must(t, testClient.Get(t.Context(), client.ObjectKeyFromObject(f.c), f.c))
	if f.c.ResourceVersion != version {
		t.Fatal("steady-state reconciliation rewrote status")
	}
	pod.Status.Conditions[0].Status = corev1.ConditionFalse
	must(t, testClient.Status().Update(t.Context(), pod))
	f.reconcile(t)
	f.condition(t, "Available", "WaitingForPostgres", metav1.ConditionFalse)
	if f.c.Status.ReadyInstances != 0 {
		t.Fatal("unready primary is advertised as ready")
	}
	pod.Status.Phase = corev1.PodFailed
	must(t, testClient.Status().Update(t.Context(), pod))
	f.reconcile(t)
	f.condition(t, "Blocked", "RecoveryRequired", metav1.ConditionTrue)
	f.condition(t, "Available", "RecoveryRequired", metav1.ConditionFalse)
	if f.pod(t).UID != pod.UID || f.pvc(t).UID != pvc.UID {
		t.Fatal("failed primary was replaced without fencing")
	}
}

func TestReplacedPodUIDIsRejected(t *testing.T) {
	f := newFixture(t, true)
	f.reconcile(t)
	pod := f.pod(t)
	must(t, testClient.Delete(t.Context(), pod, client.GracePeriodSeconds(0)))
	replacement := postgres.Pod(f.c)
	must(t, controllerutil.SetControllerReference(f.c, replacement, testScheme))
	must(t, testClient.Create(t.Context(), replacement))
	f.reconcile(t)
	f.condition(t, "Blocked", "RecoveryRequired", metav1.ConditionTrue)
	if f.c.Status.PodUID != "" {
		t.Fatal("replacement Pod was advertised as authorized")
	}
}

func TestConcurrentReconcilesConverge(t *testing.T) {
	// Simulate work overlapping at a manager handover. CAS must protect bootstrap
	// even though the normal workqueue serializes requests for the same key.
	for range 5 {
		f := newFixture(t, true)
		errors := make(chan error, 8)
		var workers sync.WaitGroup
		for range 8 {
			workers.Go(func() {
				_, err := f.r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.c)})
				errors <- err
			})
		}
		workers.Wait()
		close(errors)
		for err := range errors {
			if err != nil && !apierrors.IsConflict(err) && !apierrors.IsAlreadyExists(err) {
				t.Fatal(err)
			}
		}
		f.reconcile(t)
		pod, pvc := f.pod(t), f.pvc(t)
		f.reconcile(t)
		f.condition(t, "Blocked", "WaitingForPostgres", metav1.ConditionFalse)
		if f.pod(t).UID != pod.UID || f.pvc(t).UID != pvc.UID {
			t.Fatal("concurrent reconcile replaced resources")
		}
	}
}

func TestParameterLengthValidatedOnCreate(t *testing.T) {
	f := newFixture(t, true)
	for _, size := range []int{1024, 1025} {
		c := f.c.DeepCopy()
		c.ObjectMeta = metav1.ObjectMeta{Name: fmt.Sprintf("parameter-%d", size), Namespace: f.c.Namespace}
		c.Spec.Postgres.Parameters = map[string]pgv1.PostgresParameterValue{"log_statement": pgv1.PostgresParameterValue(strings.Repeat("é", size))}
		err := testClient.Create(t.Context(), c)
		if size == 1024 {
			must(t, err)
			must(t, postgres.Validate(c))
		} else if !apierrors.IsInvalid(err) {
			t.Fatalf("oversized immutable parameter admitted: %v", err)
		}
	}
}
