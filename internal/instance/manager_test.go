package instance

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vsamidurai/PostgresManager/internal/ha"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func managerFixture(t *testing.T) (*Manager, ha.State, *corev1.ConfigMap) {
	t.Helper()
	root := t.TempDir()
	a := &Manager{root: root, cfg: Config{ClusterUID: "cluster", ControlUID: "authority", ControlName: "control", Instance: "one", PodUID: "pod", PVCUID: "pvc", Namespace: "databases", Headless: "instances", DataDir: filepath.Join(root, "pgdata")}, j: ha.Journal{ClusterUID: "cluster", ControlUID: "authority", Instance: "one", PodUID: "pod", PVCUID: "pvc", MaxTerm: 1}}
	a.cfg.Mode = "Asynchronous"
	a.j.PolicyTerm = 1
	s := ha.State{Version: 1, ClusterUID: "cluster", ContractHash: "hash", Term: 1, Phase: ha.Running, Primary: "one", Members: []ha.Member{{Name: "one", PodUID: "pod", PVCUID: "pvc", PVCRequested: true, PodRequested: true}}}
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "control", Namespace: "databases", UID: types.UID("authority")}, Data: map[string]string{"state.json": string(data)}}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	a.api = fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()
	return a, s, cm
}

func TestChangedDurabilityPolicyDoesNotRenewAuthority(t *testing.T) {
	for _, member := range []string{"", "three"} {
		t.Run("replacement-"+member, func(t *testing.T) {
			a, s, cm := managerFixture(t)
			a.cfg.Mode = "Synchronous"
			a.j.SyncMember = "two"
			s.SyncMember = "two"
			s.Members = append(s.Members, ha.Member{Name: "two"}, ha.Member{Name: "three"})
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			data, _ := json.Marshal(s)
			cm.Data["state.json"] = string(data)
			a.api = fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()
			if _, err := a.readState(t.Context()); err != nil {
				t.Fatal(err)
			}
			before := a.contact
			s.SyncMember = member
			data, _ = json.Marshal(s)
			cm.Data["state.json"] = string(data)
			a.api = fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()
			if _, err := a.readState(t.Context()); err == nil {
				t.Fatal("in-place policy change renewed writer authority")
			}
			if !a.contact.Equal(before) {
				t.Fatal("invalid policy extended the watchdog deadline")
			}
		})
	}
}

func TestAuthorityCannotBeRestoredRolledBackOrRebound(t *testing.T) {
	for _, scenario := range []string{"restored", "rollback", "replacement-pod", "replacement-volume", "corrupt"} {
		t.Run(scenario, func(t *testing.T) {
			a, s, cm := managerFixture(t)
			if _, err := a.readState(t.Context()); err != nil {
				t.Fatal(err)
			}
			contact := a.contact
			switch scenario {
			case "restored":
				cm.UID = "restored-authority"
			case "rollback":
				a.j.MaxTerm = 2
			case "replacement-pod":
				s.Members[0].PodUID = "other"
			case "replacement-volume":
				s.Members[0].PVCUID = "other"
			case "corrupt":
				s.Phase = "unknown"
			}
			data, _ := json.Marshal(s)
			cm.Data["state.json"] = string(data)
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			a.api = fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()
			if _, err := a.readState(t.Context()); err == nil {
				t.Fatal("invalid authority refreshed writer access")
			}
			if !a.contact.Equal(contact) {
				t.Fatal("invalid response extended authority lifetime")
			}
		})
	}
}

func TestMonotonicAuthorityExpiryAndRestartInhibition(t *testing.T) {
	a, s, _ := managerFixture(t)
	if _, err := a.readState(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := a.current(s); err != nil {
		t.Fatal(err)
	}
	a.contact = time.Now().Add(-ha.AuthorityTimeout - time.Second)
	if err := a.current(s); err == nil {
		t.Fatal("expired writer grant accepted")
	}
	a.contact = time.Now()
	a.revoke(1)
	if err := a.current(s); err == nil {
		t.Fatal("fresh read revived locally revoked writer")
	}
	a.j.FenceTerm = 1
	if err := a.save(); err != nil {
		t.Fatal(err)
	}
	journal, err := ha.LoadJournal(filepath.Join(a.root, "instance-journal.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Authorize(s, "authority", "one", "pod", "pvc"); err == nil {
		t.Fatal("restart lost persistent inhibition")
	}
}

func TestEstablishedPrimaryCannotInitializeAnEmptyVolume(t *testing.T) {
	a, s, _ := managerFixture(t)
	a.contact = time.Now()
	s.SystemID = "already-established"
	err := a.step(t.Context(), s)
	if err == nil || !strings.Contains(err.Error(), "refusing to initialize an established database") {
		t.Fatalf("empty replacement volume was not held: %v", err)
	}
	if a.j.Initialized || a.j.WriterTerm != 0 || a.pg.cmd != nil {
		t.Fatal("replacement volume started a database")
	}
}

func TestManagedConfigurationProtectsReplicationAndBootstrap(t *testing.T) {
	a, s, _ := managerFixture(t)
	s.SyncMember = "two"
	a.cfg.Parameters = map[string]string{"log_statement": "all", "shared_buffers": "64MB"}
	if err := a.config(s, true); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(filepath.Join(a.root, "manager.conf"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(content), "log_statement") || !strings.Contains(string(content), "listen_addresses = ''") || !strings.Contains(string(content), "synchronous_standby_names = ''") {
		t.Fatal("initialization exposes credential SQL or blocks on absent standby")
	}
	if err := a.config(s, false); err != nil {
		t.Fatal(err)
	}
	content, _ = os.ReadFile(filepath.Join(a.root, "manager.conf"))
	if !strings.Contains(string(content), `ANY 1 ("two")`) || !strings.Contains(string(content), "synchronous_commit = 'on'") {
		t.Fatal("strict named durability policy missing")
	}
	s.Primary = "two"
	s.SyncMember = "one"
	if err := a.config(s, false); err != nil {
		t.Fatal(err)
	}
	content, _ = os.ReadFile(filepath.Join(a.root, "manager.conf"))
	if !strings.Contains(string(content), "sslmode=verify-full") || !strings.Contains(string(content), "application_name=one") {
		t.Fatal("upstream identity is not verified")
	}
}

func TestInterruptedCloneInstallRejectsForeignIdentity(t *testing.T) {
	for _, field := range []string{"cluster", "control", "volume", "instance", "history", "future-term"} {
		t.Run(field, func(t *testing.T) {
			a, s, _ := managerFixture(t)
			s.SystemID = "system"
			s.Primary = "two"
			if err := os.MkdirAll(a.cfg.DataDir, 0700); err != nil {
				t.Fatal(err)
			}
			if err := a.markClone(a.cfg.DataDir, s, "rejoin"); err != nil {
				t.Fatal(err)
			}
			switch field {
			case "cluster":
				a.cfg.ClusterUID = "other"
			case "control":
				a.cfg.ControlUID = "other"
			case "volume":
				a.cfg.PVCUID = "other"
			case "instance":
				a.cfg.Instance = "other"
			case "history":
				s.SystemID = "other"
			case "future-term":
				s.Term = 0
			}
			if _, err := a.resumeInstall(t.Context(), s, "rejoin"); err == nil {
				t.Fatal("foreign clone installed after interrupted operation")
			}
			if a.j.Initialized {
				t.Fatal("foreign data activated")
			}
		})
	}
}

func TestInterruptedCloneFromOldTermIsRetained(t *testing.T) {
	a, s, _ := managerFixture(t)
	s.SystemID = "system"
	s.Primary = "two"
	if err := os.MkdirAll(a.cfg.DataDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(a.cfg.DataDir, "evidence"), []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := a.markClone(a.cfg.DataDir, s, "rejoin"); err != nil {
		t.Fatal(err)
	}
	s.Term = 2
	if done, err := a.resumeInstall(t.Context(), s, "rejoin"); err != nil || done {
		t.Fatalf("unsafe stale install result: %v %v", done, err)
	}
	if data, err := os.ReadFile(filepath.Join(a.root, "retained-clone-1", "evidence")); err != nil || string(data) != "preserve" {
		t.Fatal("stale backup evidence was lost")
	}
	if a.j.Initialized {
		t.Fatal("stale clone activated")
	}
}
