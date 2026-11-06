//go:build integration

package controller

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	pgv1 "github.com/vsamidurai/PostgresManager/api/v1alpha1"
	"github.com/vsamidurai/PostgresManager/internal/postgres"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var testClient client.WithWatch
var testConfig *rest.Config
var testScheme = runtime.NewScheme()

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		fmt.Fprintln(os.Stderr, "KUBEBUILDER_ASSETS is required; run make test-integration")
		os.Exit(1)
	}
	_ = clientgoscheme.AddToScheme(testScheme)
	_ = pgv1.AddToScheme(testScheme)
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	environment := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "crd", "bases")}, ErrorIfCRDPathMissing: true}
	// Fleet tests can be repeated with -count. Envtest has no namespace/GC
	// controllers to release Service IPs between cases, so use a larger range.
	environment.ControlPlane.GetAPIServer().Configure().Set("service-cluster-ip-range", "10.0.0.0/16")
	cfg, err := environment.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	testConfig = cfg
	testClient, err = client.NewWithWatch(cfg, client.Options{Scheme: testScheme})
	if err != nil {
		_ = environment.Stop()
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	if err := environment.Stop(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		code = 1
	}
	os.Exit(code)
}

type fixture struct {
	c *pgv1.PostgresCluster
	r *PostgresClusterReconciler
}

func newFixture(t *testing.T, secret bool) *fixture {
	t.Helper()
	ctx := t.Context()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "pg-test-"}}
	must(t, testClient.Create(ctx, ns))
	c := &pgv1.PostgresCluster{ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: ns.Name}, Spec: pgv1.PostgresClusterSpec{
		Region: "local", Postgres: pgv1.PostgresSpec{},
		Bootstrap: pgv1.BootstrapSpec{InitDB: pgv1.InitDBSpec{SuperuserSecretRef: pgv1.SecretReference{Name: "bootstrap"}}},
		Storage:   pgv1.StorageSpec{Data: pgv1.DataStorageSpec{Size: "1Gi"}},
	}}
	must(t, testClient.Create(ctx, c))
	if secret {
		must(t, testClient.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "bootstrap", Namespace: ns.Name}, Data: map[string][]byte{"password": []byte("integration-test-password")}}))
	}
	return &fixture{c: c, r: &PostgresClusterReconciler{Client: testClient, APIReader: testClient, Scheme: testScheme}}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func (f *fixture) reconcile(t *testing.T) {
	t.Helper()
	_, err := f.r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.c)})
	must(t, err)
	must(t, testClient.Get(t.Context(), client.ObjectKeyFromObject(f.c), f.c))
}
func (f *fixture) key(name string) types.NamespacedName {
	return types.NamespacedName{Namespace: f.c.Namespace, Name: name}
}
func (f *fixture) pod(t *testing.T) *corev1.Pod {
	t.Helper()
	p := &corev1.Pod{}
	must(t, testClient.Get(t.Context(), f.key(postgres.ResourceNames(f.c).Instance), p))
	return p
}
func (f *fixture) pvc(t *testing.T) *corev1.PersistentVolumeClaim {
	t.Helper()
	p := &corev1.PersistentVolumeClaim{}
	must(t, testClient.Get(t.Context(), f.key(postgres.ResourceNames(f.c).PVC), p))
	return p
}
func (f *fixture) condition(t *testing.T, kind, reason string, status metav1.ConditionStatus) {
	t.Helper()
	condition := meta.FindStatusCondition(f.c.Status.Conditions, kind)
	if condition == nil || condition.Status != status || condition.Reason != reason {
		t.Fatalf("unexpected %s condition: %+v", kind, condition)
	}
}

func TestBootstrapAndIdempotency(t *testing.T) {
	f := newFixture(t, true)
	if f.c.Spec.Instances != 1 || f.c.Spec.Postgres.MajorVersion != 17 || f.c.Spec.Postgres.Image == "" || f.c.Spec.DeletionPolicy != "Retain" {
		t.Fatal("API defaults were not applied")
	}
	f.reconcile(t)
	pod, pvc := f.pod(t), f.pvc(t)
	if pod.UID == "" || pvc.UID == "" || f.c.Status.PodUID != string(pod.UID) || f.c.Status.PVCUID != string(pvc.UID) {
		t.Fatal("actual resource identities were not recorded")
	}
	if !metav1.IsControlledBy(pod, f.c) || len(pvc.OwnerReferences) != 0 {
		t.Fatal("incorrect lifecycle ownership")
	}
	for _, name := range []string{postgres.ResourceNames(f.c).Writer, postgres.ResourceNames(f.c).Headless} {
		s := &corev1.Service{}
		must(t, testClient.Get(t.Context(), f.key(name), s))
		if s.Spec.Selector[postgres.ClusterUIDLabel] != string(f.c.UID) {
			t.Fatal("service can select another cluster incarnation")
		}
	}
	f.reconcile(t)
	if f.pod(t).ResourceVersion != pod.ResourceVersion || f.pvc(t).ResourceVersion != pvc.ResourceVersion {
		t.Fatal("unchanged reconciliation mutated a database resource")
	}
	f.condition(t, "Ready", "WaitingForPostgres", metav1.ConditionFalse)
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	must(t, testClient.Status().Update(t.Context(), pod))
	pvc.Status.Capacity = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}
	must(t, testClient.Status().Update(t.Context(), pvc))
	f.reconcile(t)
	f.condition(t, "Ready", "Reconciled", metav1.ConditionTrue)
	f.condition(t, "Available", "Reconciled", metav1.ConditionTrue)
}

func TestMissingSecretDoesNotCreateDataOrPods(t *testing.T) {
	f := newFixture(t, false)
	f.reconcile(t)
	f.condition(t, "Blocked", "CredentialsUnavailable", metav1.ConditionTrue)
	if err := testClient.Get(t.Context(), f.key(postgres.ResourceNames(f.c).PVC), &corev1.PersistentVolumeClaim{}); !apierrors.IsNotFound(err) {
		t.Fatalf("unexpected data allocation: %v", err)
	}
	must(t, testClient.Create(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "bootstrap", Namespace: f.c.Namespace}, Data: map[string][]byte{"password": []byte("password")}}))
	f.reconcile(t)
	_ = f.pod(t)
}

func TestAdmissionRejectsUnsupportedAndUnsafeChanges(t *testing.T) {
	changes := map[string]func(*pgv1.PostgresCluster){
		"replication":   func(c *pgv1.PostgresCluster) { c.Spec.Instances = 2 },
		"major-upgrade": func(c *pgv1.PostgresCluster) { c.Spec.Postgres.MajorVersion = 18 },
		"image-update":  func(c *pgv1.PostgresCluster) { c.Spec.Postgres.Image = "postgres:18" },
		"protected-parameter": func(c *pgv1.PostgresCluster) {
			c.Spec.Postgres.Parameters = map[string]pgv1.PostgresParameterValue{"archive_command": "false"}
		},
		"shrink":                func(c *pgv1.PostgresCluster) { c.Spec.Storage.Data.Size = "512Mi" },
		"storage-migration":     func(c *pgv1.PostgresCluster) { c.Spec.Storage.Data.StorageClassName = "other-class" },
		"credentials-reference": func(c *pgv1.PostgresCluster) { c.Spec.Bootstrap.InitDB.SuperuserSecretRef.Name = "other-secret" },
		"resources": func(c *pgv1.PostgresCluster) {
			c.Spec.Resources.Requests = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}
		},
		"delete-storage": func(c *pgv1.PostgresCluster) { c.Spec.DeletionPolicy = "Delete" },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, true)
			change(f.c)
			if err := testClient.Update(t.Context(), f.c); !apierrors.IsInvalid(err) {
				t.Fatalf("expected admission rejection, got %v", err)
			}
		})
	}
	t.Run("unknown-feature", func(t *testing.T) {
		f := newFixture(t, true)
		u := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": pgv1.GroupVersion.String(), "kind": "PostgresCluster"}}
		must(t, testClient.Get(t.Context(), client.ObjectKeyFromObject(f.c), u))
		must(t, unstructured.SetNestedField(u.Object, map[string]interface{}{"local": map[string]interface{}{"mode": "Synchronous"}}, "spec", "replication"))
		err := testClient.Update(t.Context(), u, client.FieldValidation("Strict"))
		if err == nil {
			t.Fatal("unsupported replication field silently accepted under strict validation")
		}
	})
}

func TestDeletedResourcesAreNotSilentlyRecreated(t *testing.T) {
	for _, target := range []string{"pod", "pvc", "control"} {
		t.Run(target, func(t *testing.T) {
			f := newFixture(t, true)
			f.reconcile(t)
			var object client.Object
			switch target {
			case "pod":
				object = f.pod(t)
			case "pvc":
				object = f.pvc(t)
				object.SetFinalizers(nil)
				must(t, testClient.Update(t.Context(), object)) // envtest has no protection controller.
			case "control":
				object = &corev1.ConfigMap{}
				must(t, testClient.Get(t.Context(), f.key(postgres.ResourceNames(f.c).Control), object))
			}
			must(t, testClient.Delete(t.Context(), object, client.GracePeriodSeconds(0)))
			f.reconcile(t)
			f.condition(t, "Blocked", "RecoveryRequired", metav1.ConditionTrue)
			if err := testClient.Get(t.Context(), client.ObjectKeyFromObject(object), object); !apierrors.IsNotFound(err) {
				t.Fatalf("deleted %s was recreated: %v", target, err)
			}
		})
	}
}

func TestReplacedPVCUIDIsRejected(t *testing.T) {
	f := newFixture(t, true)
	f.reconcile(t)
	old := f.pvc(t)
	old.Finalizers = nil
	must(t, testClient.Update(t.Context(), old))
	must(t, testClient.Delete(t.Context(), old))
	replacement := postgres.PVC(f.c)
	must(t, testClient.Create(t.Context(), replacement))
	if old.UID == replacement.UID {
		t.Fatal("test did not replace the PVC")
	}
	f.reconcile(t)
	f.condition(t, "Blocked", "RecoveryRequired", metav1.ConditionTrue)
}

func TestForeignServiceIsNotAdopted(t *testing.T) {
	f := newFixture(t, true)
	svc := postgres.Service(f.c, false)
	must(t, testClient.Create(t.Context(), svc))
	f.reconcile(t)
	f.condition(t, "Blocked", "ServiceBlocked", metav1.ConditionTrue)
	if err := testClient.Get(t.Context(), f.key(postgres.ResourceNames(f.c).Instance), &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatal("database started despite endpoint ownership conflict")
	}
}

func TestCapacityGrowthDoesNotReplacePod(t *testing.T) {
	f := newFixture(t, true)
	sc := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{GenerateName: "expandable-"}, Provisioner: "example.com/test", AllowVolumeExpansion: ptr.To(true)}
	must(t, testClient.Create(t.Context(), sc))
	// Choose the class before creating the CR, because it is immutable.
	other := f.c.DeepCopy()
	other.Name = "expand"
	other.ResourceVersion = ""
	other.UID = ""
	other.Spec.Storage.Data.StorageClassName = sc.Name
	must(t, testClient.Create(t.Context(), other))
	f.c = other
	f.reconcile(t)
	pod := f.pod(t)
	pvc := f.pvc(t)
	pvc.Spec.VolumeName = "test-volume-" + string(pvc.UID)
	must(t, testClient.Update(t.Context(), pvc))
	pvc.Status.Phase = corev1.ClaimBound
	pvc.Status.Capacity = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}
	must(t, testClient.Status().Update(t.Context(), pvc))
	f.c.Spec.Storage.Data.Size = "2Gi"
	must(t, testClient.Update(t.Context(), f.c))
	f.reconcile(t)
	q := f.pvc(t).Spec.Resources.Requests[corev1.ResourceStorage]
	if q.Cmp(resource.MustParse("2Gi")) != 0 {
		t.Fatal("PVC request was not expanded")
	}
	if f.pod(t).UID != pod.UID || f.pod(t).ResourceVersion != pod.ResourceVersion {
		t.Fatal("volume-only change restarted or changed the Pod")
	}
}

func TestDeletionRetainsPVCAndCredentials(t *testing.T) {
	f := newFixture(t, true)
	f.reconcile(t)
	pvcUID := f.pvc(t).UID
	must(t, testClient.Delete(t.Context(), f.c))
	_, err := f.r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.c)})
	must(t, err)
	// No kubelet runs in envtest; simulate completion of graceful Pod shutdown.
	pod := &corev1.Pod{}
	if err := testClient.Get(t.Context(), f.key(postgres.ResourceNames(f.c).Instance), pod); err == nil {
		must(t, testClient.Delete(t.Context(), pod, client.GracePeriodSeconds(0)))
	} else if !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
	_, err = f.r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.c)})
	must(t, err)
	if err := testClient.Get(t.Context(), client.ObjectKeyFromObject(f.c), &pgv1.PostgresCluster{}); !apierrors.IsNotFound(err) {
		t.Fatalf("cluster finalization did not complete: %v", err)
	}
	if f.pvc(t).UID != pvcUID {
		t.Fatal("data PVC was replaced or removed")
	}
	must(t, testClient.Get(context.Background(), f.key("bootstrap"), &corev1.Secret{}))
}
