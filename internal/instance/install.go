package instance

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/vsamidurai/PostgresManager/internal/ha"
)

// The marker moves atomically with the base backup. It makes a crash between
// directory installation and journal commit distinguishable from old data.
type installMarker struct {
	ClusterUID string `json:"clusterUID"`
	ControlUID string `json:"controlUID"`
	Instance   string `json:"instance"`
	PVCUID     string `json:"pvcUID"`
	SystemID   string `json:"systemID"`
	Term       int64  `json:"term"`
	Operation  string `json:"operation"`
}

func (a *Manager) markClone(dir string, s ha.State, operation string) error {
	data, err := json.Marshal(installMarker{ClusterUID: a.cfg.ClusterUID, ControlUID: a.cfg.ControlUID, Instance: a.cfg.Instance, PVCUID: a.cfg.PVCUID, SystemID: s.SystemID, Term: s.Term, Operation: operation})
	if err != nil {
		return err
	}
	return ha.AtomicWrite(filepath.Join(dir, ".pgm-installed.json"), data, 0600)
}

func (a *Manager) resumeInstall(ctx context.Context, s ha.State, operation string) (bool, error) {
	data, err := os.ReadFile(filepath.Join(a.cfg.DataDir, ".pgm-installed.json"))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var marker installMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		return false, err
	}
	if marker.Operation != operation {
		return false, nil
	}
	if marker.ClusterUID != a.cfg.ClusterUID || marker.ControlUID != a.cfg.ControlUID || marker.Instance != a.cfg.Instance || marker.PVCUID != a.cfg.PVCUID || marker.SystemID != s.SystemID || marker.Term > s.Term {
		return false, fmt.Errorf("installed clone belongs to another authority or history")
	}
	if marker.Term < s.Term {
		// A clone installed before a lost grant must not be attached to a new
		// timeline on assumption. Retain it and take a new verified base backup.
		if a.j.Initialized {
			return false, fmt.Errorf("older installed clone was already activated")
		}
		destination := filepath.Join(a.root, fmt.Sprintf("retained-clone-%d", marker.Term))
		if _, err := os.Stat(destination); err == nil {
			return false, fmt.Errorf("older clone retention destination already exists")
		}
		if err := os.Rename(a.cfg.DataDir, destination); err != nil {
			return false, err
		}
		dir, err := os.Open(a.root)
		if err != nil {
			return false, err
		}
		defer dir.Close()
		return false, dir.Sync()
	}
	info, err := controlData(ctx, a.cfg.DataDir)
	if err != nil {
		return false, err
	}
	if info["Database system identifier"] != s.SystemID {
		return false, fmt.Errorf("installed clone system identifier changed")
	}
	if _, err := os.Stat(filepath.Join(a.cfg.DataDir, "standby.signal")); err != nil {
		return false, fmt.Errorf("installed clone is not a standby")
	}
	a.j.Initialized = true
	a.j.SystemID = s.SystemID
	a.j.FollowTerm = s.Term
	a.j.RejoinOperation = operation
	return true, a.save()
}
