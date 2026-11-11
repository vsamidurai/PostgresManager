package ha

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func fixture() (State, map[string]Observation) {
	s := State{Version: 1, ClusterUID: "cluster", ContractHash: "hash", Term: 1, Phase: Preparing, Primary: "one", SyncMember: "two", SystemID: "database", Timeline: 1, Operation: "failover"}
	observations := map[string]Observation{}
	for _, name := range []string{"one", "two", "three"} {
		s.Members = append(s.Members, Member{Name: name, PodUID: name + "-pod", PVCUID: name + "-pvc", PVCRequested: true, PodRequested: true})
		observations[name] = Observation{ClusterUID: s.ClusterUID, ControlUID: "control", Instance: name, PodUID: name + "-pod", PVCUID: name + "-pvc", Term: 1, Role: Prepared, SystemID: s.SystemID, Timeline: 1, ReplayLSN: "0/100", ReceiveLSN: "0/100", Operation: s.Operation, At: time.Now()}
	}
	f := observations["one"]
	f.Role = Fenced
	f.FenceTerm = 1
	f.Stopped = true
	s.Fence = &f
	return s, observations
}

func TestPromotionRequiresFenceAndDurabilityEvidence(t *testing.T) {
	for _, name := range []string{"no-fence", "stale-pod", "wrong-control", "wrong-term", "not-stopped", "wrong-system", "unprepared", "stale-observation", "missing-sync-copy", "async-unknown-loss", "never-streamed"} {
		t.Run(name, func(t *testing.T) {
			s, obs := fixture()
			mode := "Synchronous"
			switch name {
			case "no-fence":
				s.Fence = nil
			case "stale-pod":
				s.Fence.PodUID = "replaced"
			case "wrong-control":
				s.Fence.ControlUID = "restored-control"
			case "wrong-term":
				s.Fence.Term = 0
			case "not-stopped":
				s.Fence.Stopped = false
			case "wrong-system":
				for k, o := range obs {
					o.SystemID = "other"
					obs[k] = o
				}
			case "unprepared":
				for k, o := range obs {
					o.Role = Standby
					obs[k] = o
				}
			case "stale-observation":
				for k, o := range obs {
					o.At = time.Now().Add(-time.Minute)
					obs[k] = o
				}
			case "missing-sync-copy":
				delete(obs, "two")
			case "async-unknown-loss":
				mode = "Asynchronous"
			case "never-streamed":
				for k, o := range obs {
					o.ReceiveLSN = "0/0"
					obs[k] = o
				}
			}
			if _, err := Candidate(s, "control", obs, mode, "Lossless", time.Now()); err == nil {
				t.Fatal("unsafe promotion authorized")
			}
		})
	}
	s, obs := fixture()
	if o, err := Candidate(s, "control", obs, "Synchronous", "Lossless", time.Now()); err != nil || o.Instance == "one" {
		t.Fatalf("safe candidate rejected: %+v %v", o, err)
	}
	if _, err := Candidate(s, "control", obs, "Asynchronous", "AllowPotentialDataLoss", time.Now()); err != nil {
		t.Fatal(err)
	}
	s.Fence = nil
	if _, err := Candidate(s, "control", obs, "Asynchronous", "AllowPotentialDataLoss", time.Now()); err == nil {
		t.Fatal("data-loss acceptance waived fencing")
	}
}

func TestAsyncLosslessRequiresCaughtUpBoundary(t *testing.T) {
	s, obs := fixture()
	s.Fence.CleanShutdown = true
	s.Fence.FinalLSN = "0/200"
	if _, err := Candidate(s, "control", obs, "Asynchronous", "Lossless", time.Now()); err == nil {
		t.Fatal("lagging candidate accepted")
	}
	o := obs["three"]
	o.ReplayLSN = "0/200"
	o.ReceiveLSN = "0/200"
	obs["three"] = o
	if got, err := Candidate(s, "control", obs, "Asynchronous", "Lossless", time.Now()); err != nil || got.Instance != "three" {
		t.Fatalf("caught-up candidate rejected: %+v %v", got, err)
	}
}

func TestJournalFencesStaleWritersAcrossRestart(t *testing.T) {
	s, _ := fixture()
	s.Phase = Running
	j := Journal{ClusterUID: s.ClusterUID, ControlUID: "control", Instance: "one", PodUID: "one-pod", PVCUID: "one-pvc", MaxTerm: 1, FenceTerm: 1}
	j.PolicyTerm = 1
	j.SyncMember = s.SyncMember
	path := filepath.Join(t.TempDir(), "journal.json")
	if err := SaveJournal(path, j); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := loaded.Authorize(s, "control", "one", "one-pod", "one-pvc"); err == nil {
		t.Fatal("old writer restarted after durable fence")
	}
	if err := os.WriteFile(path, []byte(`{"payload":{},"sha256":"corrupt"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadJournal(path); err == nil {
		t.Fatal("corrupt journal accepted")
	}
}

func TestLSNComparisonUsesBothHalves(t *testing.T) {
	a, _ := LSN("1/0")
	b, _ := LSN("0/FFFFFFFF")
	if a <= b {
		t.Fatal("WAL ordering incorrect")
	}
	for _, v := range []string{"", "1", "100000000/0", "0/G", "1/2/3"} {
		if _, err := LSN(v); err == nil {
			t.Fatalf("invalid LSN accepted: %q", v)
		}
	}
}
