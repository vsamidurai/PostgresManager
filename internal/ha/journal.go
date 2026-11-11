package ha

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Journal lives outside PGDATA so base backups cannot overwrite authority.
// FenceTerm inhibits writable starts at or below that term, across restarts.
type Journal struct {
	ClusterUID      string       `json:"clusterUID"`
	ControlUID      string       `json:"controlUID"`
	Instance        string       `json:"instance"`
	PVCUID          string       `json:"pvcUID"`
	PodUID          string       `json:"podUID"`
	MaxTerm         int64        `json:"maxTerm"`
	FenceTerm       int64        `json:"fenceTerm"`
	WriterTerm      int64        `json:"writerTerm"`
	PolicyTerm      int64        `json:"policyTerm"`
	SyncMember      string       `json:"syncMember,omitempty"`
	SystemID        string       `json:"systemID,omitempty"`
	Timeline        int64        `json:"timeline,omitempty"`
	Initialized     bool         `json:"initialized,omitempty"`
	RejoinOperation string       `json:"rejoinOperation,omitempty"`
	RejoinIntent    string       `json:"rejoinIntent,omitempty"`
	PreparedTerm    int64        `json:"preparedTerm,omitempty"`
	PreparedLSN     string       `json:"preparedLSN,omitempty"`
	FollowTerm      int64        `json:"followTerm,omitempty"`
	Fence           *Observation `json:"fence,omitempty"`
}

type envelope struct {
	Payload json.RawMessage `json:"payload"`
	SHA256  string          `json:"sha256"`
}

func LoadJournal(path string) (Journal, error) {
	var j Journal
	data, err := os.ReadFile(path)
	if err != nil {
		return j, err
	}
	var e envelope
	if err := json.Unmarshal(data, &e); err != nil {
		return j, err
	}
	sum := sha256.Sum256(e.Payload)
	if e.SHA256 != hex.EncodeToString(sum[:]) {
		return j, fmt.Errorf("authority journal checksum mismatch")
	}
	if err := json.Unmarshal(e.Payload, &j); err != nil {
		return j, err
	}
	if j.ClusterUID == "" || j.ControlUID == "" || j.Instance == "" || j.PVCUID == "" || j.PodUID == "" || j.MaxTerm < j.FenceTerm || j.MaxTerm < j.WriterTerm {
		return j, fmt.Errorf("invalid authority journal")
	}
	return j, nil
}

func SaveJournal(path string, j Journal) error {
	payload, err := json.Marshal(j)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(payload)
	data, err := json.Marshal(envelope{Payload: payload, SHA256: hex.EncodeToString(sum[:])})
	if err != nil {
		return err
	}
	return AtomicWrite(path, data, 0600)
}

// AtomicWrite fsyncs both the file and directory before reporting success.
func AtomicWrite(path string, data []byte, mode os.FileMode) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".pgm-write-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	defer f.Close()
	if err = f.Chmod(mode); err != nil {
		return err
	}
	if _, err = f.Write(data); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (j Journal) Authorize(s State, controlUID, instance, podUID, pvcUID string) error {
	if err := s.Validate(); err != nil {
		return err
	}
	m, ok := s.Member(instance)
	if !ok || m.PodUID != podUID || m.PVCUID != pvcUID || j.ClusterUID != s.ClusterUID || j.ControlUID != controlUID || j.Instance != instance || j.PVCUID != pvcUID || j.PodUID != podUID {
		return fmt.Errorf("authority or storage identity mismatch")
	}
	if s.Term < j.MaxTerm {
		return fmt.Errorf("authority rollback detected")
	}
	if j.PolicyTerm != s.Term || j.SyncMember != s.SyncMember {
		return fmt.Errorf("durability policy is not journaled for this term")
	}
	if s.Primary == instance && s.Phase == Running && s.Term <= j.FenceTerm {
		return fmt.Errorf("writer term is durably fenced")
	}
	return nil
}
