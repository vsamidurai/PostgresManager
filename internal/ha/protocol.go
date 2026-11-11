// Package ha defines the regional authority protocol. Kubernetes lease expiry
// and Pod deletion are never fencing evidence.
package ha

import (
	"encoding/json"
	"fmt"
	"k8s.io/apimachinery/pkg/util/validation"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	Running            = "Running"
	Fencing            = "Fencing"
	Preparing          = "Preparing"
	Stopping           = "Stopping"
	Primary            = "Primary"
	Standby            = "Standby"
	Prepared           = "Prepared"
	Fenced             = "Fenced"
	Failed             = "Failed"
	AuthorityTimeout   = 15 * time.Second
	ObservationTimeout = 20 * time.Second
)

type Member struct {
	Name            string       `json:"name"`
	PVCUID          string       `json:"pvcUID,omitempty"`
	PodUID          string       `json:"podUID,omitempty"`
	PVCRequested    bool         `json:"pvcRequested,omitempty"`
	PodRequested    bool         `json:"podRequested,omitempty"`
	RejoinOperation string       `json:"rejoinOperation,omitempty"`
	Fence           *Observation `json:"fence,omitempty"`
	FenceConsumed   bool         `json:"fenceConsumed,omitempty"`
	PreviousPodUID  string       `json:"previousPodUID,omitempty"`
}

type Request struct {
	Name   string `json:"name"`
	UID    string `json:"uid"`
	Type   string `json:"type"`
	Target string `json:"target"`
}

// State is written only by regional controllers using resourceVersion CAS.
// Its Kubernetes UID is pinned into every database Pod and volume journal.
type State struct {
	Version            int          `json:"version"`
	ClusterUID         string       `json:"clusterUID"`
	ContractHash       string       `json:"contractHash"`
	Term               int64        `json:"term"`
	Phase              string       `json:"phase"`
	Primary            string       `json:"primary"`
	SyncMember         string       `json:"syncMember,omitempty"`
	SystemID           string       `json:"systemID,omitempty"`
	Timeline           int64        `json:"timeline,omitempty"`
	Members            []Member     `json:"members"`
	Operation          string       `json:"operation,omitempty"`
	Target             string       `json:"target,omitempty"`
	Request            *Request     `json:"request,omitempty"`
	CompletedOperation string       `json:"completedOperation,omitempty"`
	CompletedOutcome   string       `json:"completedOutcome,omitempty"`
	Fence              *Observation `json:"fence,omitempty"`
	// GrantFence remains attached to a new writer for its entire term.
	GrantFence     *Observation `json:"grantFence,omitempty"`
	ForkLSN        string       `json:"forkLSN,omitempty"`
	ParentTimeline int64        `json:"parentTimeline,omitempty"`
	RetryAfter     time.Time    `json:"retryAfter,omitempty"`
	GrantedAt      time.Time    `json:"grantedAt,omitempty"`
}

// Observation is writable only by the named instance's ServiceAccount. A fence
// acknowledgement is published after a durable journal barrier and verified
// PostgreSQL exit. Heartbeat age never substitutes for that acknowledgement.
type Observation struct {
	ClusterUID    string    `json:"clusterUID"`
	ControlUID    string    `json:"controlUID"`
	Instance      string    `json:"instance"`
	PodUID        string    `json:"podUID"`
	PVCUID        string    `json:"pvcUID"`
	Term          int64     `json:"term"`
	Role          string    `json:"role"`
	SystemID      string    `json:"systemID,omitempty"`
	Timeline      int64     `json:"timeline,omitempty"`
	ReplayLSN     string    `json:"replayLSN,omitempty"`
	ReceiveLSN    string    `json:"receiveLSN,omitempty"`
	FinalLSN      string    `json:"finalLSN,omitempty"`
	FenceTerm     int64     `json:"fenceTerm,omitempty"`
	Operation     string    `json:"operation,omitempty"`
	Stopped       bool      `json:"stopped,omitempty"`
	CleanShutdown bool      `json:"cleanShutdown,omitempty"`
	At            time.Time `json:"at"`
	Message       string    `json:"message,omitempty"`
}

func (s State) Member(name string) (Member, bool) {
	for _, m := range s.Members {
		if m.Name == name {
			return m, true
		}
	}
	return Member{}, false
}

func (s State) Validate() error {
	if s.Version != 1 || s.ClusterUID == "" || s.ContractHash == "" || s.Term < 1 || len(s.Members) < 1 || len(s.Members) > 5 {
		return fmt.Errorf("invalid authority identity or membership")
	}
	seen := map[string]bool{}
	for _, m := range s.Members {
		if len(validation.IsDNS1123Label(m.Name)) != 0 || seen[m.Name] || (m.PVCUID != "" && !m.PVCRequested) || (m.PodRequested && m.PVCUID == "") || (m.PodUID != "" && !m.PodRequested) {
			return fmt.Errorf("invalid instance binding")
		}
		seen[m.Name] = true
	}
	if !seen[s.Primary] || (s.SyncMember != "" && (!seen[s.SyncMember] || s.SyncMember == s.Primary)) {
		return fmt.Errorf("invalid writer or synchronous member")
	}
	if s.Phase != Running && s.Phase != Fencing && s.Phase != Preparing && s.Phase != Stopping {
		return fmt.Errorf("invalid authority phase")
	}
	if s.Phase != Running && s.Operation == "" {
		return fmt.Errorf("transition has no operation identity")
	}
	if s.Term > 1 {
		f := s.GrantFence
		position, err := LSN(s.ForkLSN)
		if f == nil || !f.Stopped || f.Role != Fenced || f.PodUID == "" || f.PVCUID == "" || f.ControlUID == "" || f.FenceTerm != s.Term-1 || f.Term != s.Term-1 || f.Instance == s.Primary || f.ClusterUID != s.ClusterUID || f.SystemID != s.SystemID || f.SystemID == "" || f.Timeline != s.ParentTimeline || s.ParentTimeline < 1 || err != nil || position == 0 {
			return fmt.Errorf("writer grant lacks previous-term fencing or history evidence")
		}
	}
	return nil
}

func Decode(data string) (State, error) {
	var s State
	if err := json.Unmarshal([]byte(data), &s); err != nil {
		return s, err
	}
	return s, s.Validate()
}

func LSN(value string) (uint64, error) {
	parts := strings.Split(value, "/")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid WAL position")
	}
	hi, e1 := strconv.ParseUint(parts[0], 16, 32)
	lo, e2 := strconv.ParseUint(parts[1], 16, 32)
	if e1 != nil || e2 != nil {
		return 0, fmt.Errorf("invalid WAL position")
	}
	return hi<<32 | lo, nil
}

func Bound(s State, controlUID string, o Observation) bool {
	m, ok := s.Member(o.Instance)
	return ok && m.PodUID != "" && m.PVCUID != "" && o.ClusterUID == s.ClusterUID && o.ControlUID == controlUID && o.PodUID == m.PodUID && o.PVCUID == m.PVCUID && o.Term == s.Term
}

func Fresh(o Observation, now time.Time) bool {
	age := now.Sub(o.At)
	return !o.At.IsZero() && age >= -5*time.Second && age < ObservationTimeout
}

func FenceVerified(s State, controlUID string, o Observation) bool {
	return Bound(s, controlUID, o) && o.Instance == s.Primary && (o.Operation == s.Operation || o.Operation == "self-fence") && o.Role == Fenced && o.Stopped && o.FenceTerm == s.Term && o.SystemID == s.SystemID && s.SystemID != ""
}

// RepairFence cannot be reused after the instance has been allowed to rejoin.
func RepairFence(s State, controlUID string, m Member, f Observation) bool {
	return !m.FenceConsumed && m.PodUID != "" && f.PodUID == m.PodUID && f.PVCUID == m.PVCUID && f.Instance == m.Name && f.ClusterUID == s.ClusterUID && f.ControlUID == controlUID && f.Role == Fenced && f.Stopped && f.FenceTerm == f.Term && f.Term > 0 && f.Term < s.Term && f.SystemID == s.SystemID
}

// Candidate selects only stopped-source, replay-drained standbys of a verified
// history. A sync member supplies the durable boundary even after a primary
// crash. Async lossless takeover requires a clean source shutdown boundary.
func Candidate(s State, controlUID string, observations map[string]Observation, mode, lossPolicy string, now time.Time) (Observation, error) {
	if s.Phase != Preparing || s.Fence == nil || !FenceVerified(s, controlUID, *s.Fence) {
		return Observation{}, fmt.Errorf("AwaitingFencing")
	}
	eligible := []Observation{}
	for _, m := range s.Members {
		o := observations[m.Name]
		if m.Name == s.Primary || !Bound(s, controlUID, o) || !Fresh(o, now) || o.Role != Prepared || o.Operation != s.Operation || o.SystemID != s.SystemID || o.Timeline != s.Timeline {
			continue
		}
		replay, e1 := LSN(o.ReplayLSN)
		receive, e2 := LSN(o.ReceiveLSN)
		if e1 != nil || e2 != nil || replay == 0 || receive == 0 || replay < receive {
			continue
		}
		eligible = append(eligible, o)
	}
	var boundary uint64
	proven := false
	if s.Fence.CleanShutdown {
		var err error
		boundary, err = LSN(s.Fence.FinalLSN)
		proven = err == nil
	}
	if mode == "Synchronous" {
		for _, o := range eligible {
			if o.Instance == s.SyncMember {
				boundary, _ = LSN(o.ReplayLSN)
				proven = true
			}
		}
	}
	if !proven && lossPolicy != "AllowPotentialDataLoss" {
		return Observation{}, fmt.Errorf("DurabilityProofUnavailable")
	}
	sort.Slice(eligible, func(i, j int) bool {
		a, _ := LSN(eligible[i].ReplayLSN)
		b, _ := LSN(eligible[j].ReplayLSN)
		if a == b {
			return eligible[i].Instance < eligible[j].Instance
		}
		return a > b
	})
	for _, o := range eligible {
		position, _ := LSN(o.ReplayLSN)
		if (s.Target == "" || s.Target == o.Instance) && (!proven || position >= boundary) {
			return o, nil
		}
	}
	return Observation{}, fmt.Errorf("NoEligibleStandby")
}
