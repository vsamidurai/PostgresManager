//go:build integration

package controller

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"sync"
	"testing"
	"time"

	pgv1 "github.com/vsamidurai/PostgresManager/api/v1alpha1"
	"github.com/vsamidurai/PostgresManager/internal/ha"
	"github.com/vsamidurai/PostgresManager/internal/postgres"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func newHAFixture(t *testing.T) *fixture {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "pg-ha-"}}
	must(t, testClient.Create(t.Context(), ns))
	c := &pgv1.PostgresCluster{ObjectMeta: metav1.ObjectMeta{Name: "ha", Namespace: ns.Name}, Spec: pgv1.PostgresClusterSpec{
		Region: "local", Instances: 2, Postgres: pgv1.PostgresSpec{Image: "postgres-manager-postgres:dev"},
		Bootstrap:   pgv1.BootstrapSpec{InitDB: pgv1.InitDBSpec{SuperuserSecretRef: pgv1.SecretReference{Name: "bootstrap"}}},
		Storage:     pgv1.StorageSpec{Data: pgv1.DataStorageSpec{Size: "1Gi"}},
		Replication: &pgv1.RegionalReplicationSpec{CredentialsSecretRef: pgv1.SecretReference{Name: "replication"}, TLSSecretRef: pgv1.SecretReference{Name: "tls"}},
	}}
	must(t, testClient.Create(t.Context(), c))
	for _, name := range []string{"bootstrap", "replication"} {
		must(t, testClient.Create(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns.Name}, Data: map[string][]byte{"password": []byte("test-password")}}))
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	must(t, err)
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, DNSNames: []string{"*.ha-instances." + ns.Name + ".svc"}}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	must(t, err)
	private, err := x509.MarshalPKCS8PrivateKey(key)
	must(t, err)
	crt := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	must(t, testClient.Create(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "tls", Namespace: ns.Name}, Data: map[string][]byte{"tls.crt": crt, "ca.crt": crt, "tls.key": pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private})}}))
	f := &fixture{c: c, r: &PostgresClusterReconciler{Client: testClient, APIReader: testClient, Scheme: testScheme}}
	for i := 0; i < 3; i++ {
		f.reconcile(t)
	}
	return f
}

func (f *fixture) haState(t *testing.T) (*corev1.ConfigMap, ha.State) {
	t.Helper()
	cm := &corev1.ConfigMap{}
	must(t, testClient.Get(t.Context(), f.key(postgres.HAControlName(f.c)), cm))
	s, err := ha.Decode(cm.Data["state.json"])
	must(t, err)
	return cm, s
}
func (f *fixture) observeHA(t *testing.T, s ha.State, controlUID, name, role string) ha.Observation {
	t.Helper()
	m, _ := s.Member(name)
	o := ha.Observation{ClusterUID: s.ClusterUID, ControlUID: controlUID, Instance: name, PodUID: m.PodUID, PVCUID: m.PVCUID, Term: s.Term, Role: role, SystemID: "739000123", Timeline: 1, ReplayLSN: "0/300", ReceiveLSN: "0/300", Operation: s.Operation, At: time.Now()}
	if role == ha.Fenced {
		o.Stopped = true
		o.FenceTerm = s.Term
	}
	f.writeHAObservation(t, o)
	return o
}
func (f *fixture) writeHAObservation(t *testing.T, o ha.Observation) {
	t.Helper()
	cm := &corev1.ConfigMap{}
	must(t, testClient.Get(t.Context(), f.key(postgres.ObservationName(o.Instance)), cm))
	data, err := json.Marshal(o)
	must(t, err)
	cm.Data = map[string]string{"observation.json": string(data)}
	must(t, testClient.Update(t.Context(), cm))
}

func TestHAScaleUpAndRestrictedIdentity(t *testing.T) {
	f := newHAFixture(t)
	cm, s := f.haState(t)
	if f.c.Spec.Replication.Mode != "Synchronous" || f.c.Spec.Replication.DataLossPolicy != "Lossless" {
		t.Fatal("unsafe HA defaults")
	}
	old := s.Members[0]
	f.c.Spec.Instances = 3
	must(t, testClient.Update(t.Context(), f.c))
	for i := 0; i < 3; i++ {
		f.reconcile(t)
	}
	_, s = f.haState(t)
	if len(s.Members) != 3 || s.Members[0] != old {
		t.Fatal("scale up changed existing data identity")
	}
	for _, m := range s.Members {
		if m.PodUID == "" || m.PVCUID == "" {
			t.Fatal("unbound HA member")
		}
		role := &rbacv1.Role{}
		must(t, testClient.Get(t.Context(), f.key(m.Name), role))
		for _, rule := range role.Rules {
			for _, verb := range rule.Verbs {
				if verb != "get" && (verb != "update" || len(rule.ResourceNames) != 1 || rule.ResourceNames[0] != postgres.ObservationName(m.Name)) {
					t.Fatal("instance can mutate authority or another instance")
				}
			}
		}
		p := &corev1.Pod{}
		must(t, testClient.Get(t.Context(), f.key(m.Name), p))
		if p.Spec.ServiceAccountName != m.Name || p.Spec.Affinity.PodAntiAffinity == nil {
			t.Fatal("HA isolation missing")
		}
	}
	f.observeHA(t, s, string(cm.UID), s.Primary, ha.Primary)
	f.reconcile(t)
	f.reconcile(t)
	writer := &corev1.Service{}
	must(t, testClient.Get(t.Context(), f.key(postgres.ResourceNames(f.c).Writer), writer))
	if writer.Spec.PublishNotReadyAddresses || writer.Spec.Selector[postgres.InstanceLabel] != s.Primary {
		t.Fatal("unsafe writer routing")
	}
	f.c.Spec.Instances = 2
	if testClient.Update(t.Context(), f.c) == nil {
		t.Fatal("scale down admitted")
	}
	must(t, testClient.Get(t.Context(), client.ObjectKeyFromObject(f.c), f.c))
	f.c.Spec.Replication.Mode = "Asynchronous"
	if testClient.Update(t.Context(), f.c) == nil {
		t.Fatal("durability policy changed without an operation")
	}
}

func TestHAPromotionRequiresVerifiedFenceAndReplay(t *testing.T) {
	for _, scenario := range []string{"unreachable", "forged-fence", "unprepared", "replaced-candidate", "success"} {
		t.Run(scenario, func(t *testing.T) {
			f := newHAFixture(t)
			cm, s := f.haState(t)
			f.observeHA(t, s, string(cm.UID), s.Primary, ha.Primary)
			f.reconcile(t)
			cm, s = f.haState(t)
			old := s.Primary
			target := s.SyncMember
			o := f.observeHA(t, s, string(cm.UID), old, ha.Primary)
			o.At = time.Now().Add(-time.Minute)
			f.writeHAObservation(t, o)
			f.reconcile(t)
			cm, s = f.haState(t)
			if s.Phase != ha.Fencing {
				t.Fatalf("not fencing: %+v", s)
			}
			if scenario == "unreachable" {
				for i := 0; i < 4; i++ {
					f.reconcile(t)
				}
				_, s = f.haState(t)
				if s.Term != 1 || s.Primary != old {
					t.Fatal("timeout authorized takeover")
				}
				f.condition(t, "Blocked", "AwaitingFencing", metav1.ConditionTrue)
				return
			}
			o = f.observeHA(t, s, string(cm.UID), old, ha.Fenced)
			if scenario == "forged-fence" {
				o.PodUID = "another-pod"
				f.writeHAObservation(t, o)
				f.reconcile(t)
				_, s = f.haState(t)
				if s.Phase != ha.Fencing {
					t.Fatal("unbound fence accepted")
				}
				return
			}
			f.reconcile(t)
			cm, s = f.haState(t)
			if s.Phase != ha.Preparing {
				t.Fatal("verified fence not recorded")
			}
			role := ha.Prepared
			if scenario == "unprepared" {
				role = ha.Standby
			}
			f.observeHA(t, s, string(cm.UID), target, role)
			if scenario == "replaced-candidate" {
				p := &corev1.Pod{}
				must(t, testClient.Get(t.Context(), f.key(target), p))
				must(t, testClient.Delete(t.Context(), p, client.GracePeriodSeconds(0)))
				p.ResourceVersion = ""
				p.UID = ""
				p.DeletionTimestamp = nil
				must(t, testClient.Create(t.Context(), p))
			}
			f.reconcile(t)
			_, s = f.haState(t)
			if scenario != "success" {
				if s.Term != 1 || s.Primary != old {
					t.Fatal("unsafe candidate promoted")
				}
				return
			}
			if s.Term != 2 || s.Primary != target || s.GrantFence == nil || s.SyncMember != old {
				t.Fatalf("incorrect grant: %+v", s)
			}
			// A replayed previous-term report must not publish the writer Service.
			f.reconcile(t)
			service := &corev1.Service{}
			must(t, testClient.Get(t.Context(), f.key(postgres.ResourceNames(f.c).Writer), service))
			if service.Spec.Selector[postgres.InstanceLabel] != "no-writer" {
				t.Fatal("writer exposed before confirming new term")
			}
		})
	}
}

func TestHAOperationsRetainCompletionUntilAcknowledged(t *testing.T) {
	f := newHAFixture(t)
	cm, s := f.haState(t)
	op := &pgv1.PostgresOperation{ObjectMeta: metav1.ObjectMeta{Name: "rejoin", Namespace: f.c.Namespace}, Spec: pgv1.PostgresOperationSpec{ClusterName: f.c.Name, ClusterUID: string(f.c.UID), Type: "Rejoin", TargetInstance: s.SyncMember}}
	must(t, testClient.Create(t.Context(), op))
	r := &PostgresOperationReconciler{f.r}
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(op)}
	_, err := r.Reconcile(t.Context(), req)
	must(t, err)
	f.reconcile(t)
	cm, s = f.haState(t)
	o := f.observeHA(t, s, string(cm.UID), s.SyncMember, ha.Standby)
	o.Operation = string(op.UID)
	f.writeHAObservation(t, o)
	f.reconcile(t)
	_, s = f.haState(t)
	if s.CompletedOperation != string(op.UID) || s.Request == nil {
		t.Fatal("completion not retained")
	}
	_, err = r.Reconcile(t.Context(), req)
	must(t, err)
	must(t, testClient.Get(t.Context(), req.NamespacedName, op))
	if op.Status.Phase != "Succeeded" {
		t.Fatal("operation not completed")
	}
	f.reconcile(t)
	_, s = f.haState(t)
	if s.Request != nil {
		t.Fatal("acknowledged reservation not released")
	}
}

func TestHADeletionRequiresEveryProcessStopped(t *testing.T) {
	f := newHAFixture(t)
	must(t, testClient.Delete(t.Context(), f.c))
	f.reconcile(t)
	cm, s := f.haState(t)
	if s.Phase != ha.Stopping {
		t.Fatal("deletion did not revoke authority")
	}
	f.observeHA(t, s, string(cm.UID), s.Primary, ha.Fenced)
	f.reconcile(t)
	for _, m := range s.Members {
		p := &corev1.Pod{}
		must(t, testClient.Get(t.Context(), f.key(m.Name), p))
		if !p.DeletionTimestamp.IsZero() {
			t.Fatal("Pod deleted before every fence")
		}
	}
	for _, m := range s.Members {
		f.observeHA(t, s, string(cm.UID), m.Name, ha.Fenced)
	}
	f.reconcile(t)
	for _, m := range s.Members {
		pvc := &corev1.PersistentVolumeClaim{}
		must(t, testClient.Get(t.Context(), f.key(m.Name+"-data"), pvc))
		if !pvc.DeletionTimestamp.IsZero() {
			t.Fatal("data destroyed on deletion")
		}
	}
}

func TestHADeletionRetainsReceiptsAfterPodExit(t *testing.T) {
	f := newHAFixture(t)
	must(t, testClient.Delete(t.Context(), f.c))
	f.reconcile(t)
	cm, s := f.haState(t)
	for _, m := range s.Members {
		f.observeHA(t, s, string(cm.UID), m.Name, ha.Fenced)
	}
	for range s.Members {
		f.reconcile(t)
	}
	_, s = f.haState(t)
	for _, m := range s.Members {
		if m.Fence == nil || m.Fence.Operation != s.Operation {
			t.Fatal("deletion fence was not persisted")
		}
		o := f.observeHA(t, s, string(cm.UID), m.Name, ha.Fenced)
		o.Operation = "self-fence"
		f.writeHAObservation(t, o)
		pod := &corev1.Pod{}
		must(t, testClient.Get(t.Context(), f.key(m.Name), pod))
		must(t, testClient.Delete(t.Context(), pod, client.GracePeriodSeconds(0)))
	}
	_, err := f.r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.c)})
	must(t, err)
	if err := testClient.Get(t.Context(), client.ObjectKeyFromObject(f.c), &pgv1.PostgresCluster{}); !apierrors.IsNotFound(err) {
		t.Fatal("lost live receipt stranded deletion despite durable fences")
	}
}

func TestHAConcurrentScaleUpPreservesAssignedIdentities(t *testing.T) {
	f := newHAFixture(t)
	_, initial := f.haState(t)
	f.c.Spec.Instances = 3
	must(t, testClient.Update(t.Context(), f.c))
	var workers sync.WaitGroup
	errors := make(chan error, 20)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.c)}
	for i := 0; i < 20; i++ {
		workers.Add(1)
		go func() { defer workers.Done(); _, err := f.r.Reconcile(t.Context(), request); errors <- err }()
	}
	workers.Wait()
	close(errors)
	for err := range errors {
		if err != nil && !apierrors.IsConflict(err) && !apierrors.IsAlreadyExists(err) {
			t.Fatal(err)
		}
	}
	for i := 0; i < 4; i++ {
		f.reconcile(t)
	}
	_, result := f.haState(t)
	if len(result.Members) != 3 || result.Term != initial.Term || result.Primary != initial.Primary {
		t.Fatal("concurrent scale-up changed writer authority")
	}
	for i, m := range initial.Members {
		if result.Members[i].PodUID != m.PodUID || result.Members[i].PVCUID != m.PVCUID {
			t.Fatal("concurrent scale-up replaced an assigned identity")
		}
	}
	pods := &corev1.PodList{}
	must(t, testClient.List(t.Context(), pods, client.InNamespace(f.c.Namespace)))
	if len(pods.Items) != 3 {
		t.Fatalf("created %d Pods instead of 3", len(pods.Items))
	}
}

func TestHAFailedRequestedPromotionDoesNotReserveRecoveryForever(t *testing.T) {
	f := newHAFixture(t)
	cm, s := f.haState(t)
	op := &pgv1.PostgresOperation{ObjectMeta: metav1.ObjectMeta{Name: "switch", Namespace: f.c.Namespace}, Spec: pgv1.PostgresOperationSpec{ClusterName: f.c.Name, ClusterUID: string(f.c.UID), Type: "Switchover", TargetInstance: s.SyncMember}}
	must(t, testClient.Create(t.Context(), op))
	// The request acquired its target, which then failed before operation
	// completion was observed. Automatic recovery must supersede the request.
	s.Primary, s.SyncMember = s.SyncMember, s.Primary
	s.SystemID = "739000123"
	s.Timeline = 1
	s.Request = &ha.Request{Name: op.Name, UID: string(op.UID), Type: "Switchover", Target: s.Primary}
	must(t, f.r.saveHA(t.Context(), cm, s))
	f.observeHA(t, s, string(cm.UID), s.Primary, ha.Fenced)
	f.reconcile(t)
	_, s = f.haState(t)
	if s.Phase != ha.Fencing || s.CompletedOutcome != "Rejected" || s.CompletedOperation != string(op.UID) {
		t.Fatal("failed promotion remained an active request")
	}
	r := &PostgresOperationReconciler{f.r}
	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(op)})
	must(t, err)
	must(t, testClient.Get(t.Context(), client.ObjectKeyFromObject(op), op))
	if op.Status.Phase != "Rejected" {
		t.Fatal("failed requested promotion reported success")
	}
	f.reconcile(t)
	_, s = f.haState(t)
	if s.Request != nil {
		t.Fatal("failed request still blocks subsequent recovery")
	}
}

func TestHARejectsUntrustedReplicationCertificates(t *testing.T) {
	f := newHAFixture(t)
	secret := &corev1.Secret{}
	must(t, testClient.Get(t.Context(), f.key("tls"), secret))
	secret.Data["ca.crt"] = []byte("invalid CA")
	must(t, testClient.Update(t.Context(), secret))
	if err := f.r.checkHASecrets(t.Context(), f.c); err == nil {
		t.Fatal("invalid trust root accepted")
	}
	secret.Data["ca.crt"] = secret.Data["tls.crt"]
	must(t, testClient.Update(t.Context(), secret))
	f.r.Clock = clocktesting.NewFakeClock(time.Now().Add(48 * time.Hour))
	if err := f.r.checkHASecrets(t.Context(), f.c); err == nil {
		t.Fatal("expired replication certificate accepted")
	}
}
