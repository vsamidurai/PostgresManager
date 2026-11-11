// Package instance supervises PostgreSQL and enforces regional authority inside
// its Pod. It exposes health only; control commands come from a read-only API record.
package instance

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/vsamidurai/PostgresManager/internal/ha"
	"github.com/vsamidurai/PostgresManager/internal/postgres"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Config struct {
	Namespace, Instance, ClusterUID, ControlName, ControlUID, PodUID, PVCUID, Headless, DataDir string
	Parameters                                                                                  map[string]string
	Mode                                                                                        string
}

type Manager struct {
	cfg           Config
	api           client.Client
	root          string
	j             ha.Journal
	pg            postgresProcess
	ready         atomic.Bool
	activeWriter  atomic.Int64
	revoked       atomic.Int64
	contactMu     sync.RWMutex
	contact       time.Time
	lastPublished ha.Observation
	configChanged bool
	lastReload    time.Time
	lock          *os.File
}

func New(cfg Config, api client.Client) (*Manager, error) {
	if cfg.Mode != "Synchronous" && cfg.Mode != "Asynchronous" {
		return nil, fmt.Errorf("managed replication mode is required")
	}
	if cfg.Namespace == "" || cfg.Instance == "" || cfg.ClusterUID == "" || cfg.ControlUID == "" || cfg.ControlName == "" || cfg.PodUID == "" || cfg.PVCUID == "" || cfg.Headless == "" || cfg.DataDir != "/var/lib/postgresql/data/pgdata" {
		return nil, fmt.Errorf("incomplete instance identity or unsupported data path")
	}
	a := &Manager{cfg: cfg, api: api, root: filepath.Dir(cfg.DataDir)}
	a.pg = postgresProcess{data: cfg.DataDir, root: a.root}
	var err error
	a.lock, err = os.OpenFile(filepath.Join(a.root, ".instance-manager.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(a.lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		a.lock.Close()
		return nil, fmt.Errorf("volume is already supervised: %w", err)
	}
	a.j, err = ha.LoadJournal(filepath.Join(a.root, "instance-journal.json"))
	if os.IsNotExist(err) {
		entries, e := os.ReadDir(a.root)
		if e != nil {
			a.lock.Close()
			return nil, e
		}
		for _, entry := range entries {
			if entry.Name() != "lost+found" && entry.Name() != ".instance-manager.lock" {
				a.lock.Close()
				return nil, fmt.Errorf("refusing to adopt an unjournaled nonempty volume")
			}
		}
		a.j = ha.Journal{ClusterUID: cfg.ClusterUID, ControlUID: cfg.ControlUID, Instance: cfg.Instance, PodUID: cfg.PodUID, PVCUID: cfg.PVCUID}
		err = a.save()
	}
	if err != nil {
		a.lock.Close()
		return nil, err
	}
	if a.j.ClusterUID != cfg.ClusterUID || a.j.ControlUID != cfg.ControlUID || a.j.Instance != cfg.Instance || a.j.PVCUID != cfg.PVCUID {
		a.lock.Close()
		return nil, fmt.Errorf("journal does not match this Pod and volume assignment")
	}
	// A manager restart is not authorization to restart a formerly writable
	// process. Its term must first be fenced and reconciled by the controller.
	if a.j.WriterTerm > a.j.FenceTerm {
		a.revoked.Store(a.j.WriterTerm)
	}
	return a, nil
}

func (a *Manager) save() error {
	return ha.SaveJournal(filepath.Join(a.root, "instance-journal.json"), a.j)
}

func (a *Manager) readState(ctx context.Context) (ha.State, error) {
	started := time.Now()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	cm := &corev1.ConfigMap{}
	if err := a.api.Get(ctx, types.NamespacedName{Namespace: a.cfg.Namespace, Name: a.cfg.ControlName}, cm); err != nil {
		return ha.State{}, err
	}
	if string(cm.UID) != a.cfg.ControlUID || !cm.DeletionTimestamp.IsZero() {
		return ha.State{}, fmt.Errorf("authority record replaced or deleting")
	}
	s, err := ha.Decode(cm.Data["state.json"])
	if err != nil {
		return s, err
	}
	if (a.cfg.Mode == "Synchronous") != (s.SyncMember != "") || (a.j.PolicyTerm == s.Term && a.j.SyncMember != s.SyncMember) {
		return s, fmt.Errorf("authority changed the immutable durability policy")
	}
	m, ok := s.Member(a.cfg.Instance)
	if !ok || s.ClusterUID != a.cfg.ClusterUID || m.PodUID != a.cfg.PodUID || m.PVCUID != a.cfg.PVCUID || s.Term < a.j.MaxTerm {
		return s, fmt.Errorf("authority binding changed or rolled back")
	}
	if a.j.PodUID != a.cfg.PodUID {
		if err := a.migrateAssignment(s, m); err != nil {
			return s, err
		}
	}
	if time.Since(started) >= ha.AuthorityTimeout {
		return s, fmt.Errorf("authority response expired")
	}
	a.contactMu.Lock()
	a.contact = started
	a.contactMu.Unlock()
	return s, nil
}

func (a *Manager) migrateAssignment(s ha.State, m ha.Member) error {
	old := m
	old.PodUID = m.PreviousPodUID
	old.FenceConsumed = false // Consumption authorizes this one replacement only.
	if m.PreviousPodUID != a.j.PodUID || m.Fence == nil || !ha.RepairFence(s, a.cfg.ControlUID, old, *m.Fence) || a.j.WriterTerm > m.Fence.FenceTerm || a.j.FollowTerm > m.Fence.FenceTerm || a.j.FenceTerm < m.Fence.FenceTerm {
		return fmt.Errorf("replacement Pod lacks a durable unused fence for its previous assignment")
	}
	if err := a.pg.stop(); err != nil {
		return err
	}
	a.j.PodUID = a.cfg.PodUID
	return a.save()
}

func (a *Manager) current(s ha.State) error {
	a.contactMu.RLock()
	age := time.Since(a.contact)
	a.contactMu.RUnlock()
	if age >= ha.AuthorityTimeout {
		return fmt.Errorf("authority read expired")
	}
	if s.Primary == a.cfg.Instance && s.Phase == ha.Running && a.revoked.Load() >= s.Term {
		return fmt.Errorf("writer authority revoked locally")
	}
	return a.j.Authorize(s, a.cfg.ControlUID, a.cfg.Instance, a.cfg.PodUID, a.cfg.PVCUID)
}

func (a *Manager) revoke(term int64) {
	for old := a.revoked.Load(); old < term; old = a.revoked.Load() {
		if a.revoked.CompareAndSwap(old, term) {
			return
		}
	}
}

func (a *Manager) Run(ctx context.Context) error {
	defer a.lock.Close()
	server := &http.Server{Addr: ":8080", ReadHeaderTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		if !a.ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	server.Handler = mux
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("Health server failed", "error", err)
		}
	}()
	defer server.Close()
	monitorCtx, stopMonitor := context.WithCancel(context.Background())
	defer stopMonitor()
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-monitorCtx.Done():
				return
			case <-ticker.C:
				term := a.activeWriter.Load()
				if term == 0 {
					continue
				}
				a.contactMu.RLock()
				age := time.Since(a.contact)
				a.contactMu.RUnlock()
				if age >= ha.AuthorityTimeout {
					a.ready.Store(false)
					a.revoke(term)
					if err := a.pg.stop(); err != nil {
						slog.Error("Authority-loss shutdown is unconfirmed", "error", err)
					}
				}
			}
		}
	}()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	var last ha.State
	for {
		s, err := a.readState(ctx)
		if err == nil {
			last = s
			err = a.step(ctx, s)
		} else {
			a.ready.Store(false)
			if last.Term > 0 && a.revoked.Load() >= last.Term {
				_, _ = a.fence(context.Background(), last, "self-fence")
			}
		}
		if err != nil {
			a.ready.Store(false)
			slog.Warn("Instance reconciliation held", "instance", a.cfg.Instance, "error", err)
		}
		select {
		case <-ctx.Done():
			a.ready.Store(false)
			if last.Term > 0 {
				shutdown, cancel := context.WithTimeout(context.Background(), 35*time.Second)
				_, err := a.fence(shutdown, last, "self-fence")
				cancel()
				return err
			}
			return a.pg.stop()
		case <-ticker.C:
		}
	}
}

func (a *Manager) observation(s ha.State) ha.Observation {
	return ha.Observation{ClusterUID: a.cfg.ClusterUID, ControlUID: a.cfg.ControlUID, Instance: a.cfg.Instance, PodUID: a.cfg.PodUID, PVCUID: a.cfg.PVCUID, Term: s.Term, SystemID: a.j.SystemID, Timeline: a.j.Timeline, Operation: a.j.RejoinOperation, At: time.Now()}
}

func (a *Manager) publish(ctx context.Context, o ha.Observation) error {
	if o.Role == a.lastPublished.Role && o.Term == a.lastPublished.Term && o.Operation == a.lastPublished.Operation && o.Message == a.lastPublished.Message && time.Since(a.lastPublished.At) < 5*time.Second {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	cm := &corev1.ConfigMap{}
	key := types.NamespacedName{Namespace: a.cfg.Namespace, Name: postgres.ObservationName(a.cfg.Instance)}
	if err := a.api.Get(ctx, key, cm); err != nil {
		return err
	}
	owner := metav1.GetControllerOf(cm)
	if owner == nil || string(owner.UID) != a.cfg.ClusterUID {
		return fmt.Errorf("observation record ownership changed")
	}
	data, err := json.Marshal(o)
	if err != nil {
		return err
	}
	cm.Data = map[string]string{"observation.json": string(data)}
	if err := a.api.Update(ctx, cm); err != nil {
		return err
	}
	a.lastPublished = o
	return nil
}

func (a *Manager) fence(ctx context.Context, s ha.State, operation string) (ha.Observation, error) {
	a.ready.Store(false)
	a.revoke(s.Term)
	if s.Term > a.j.MaxTerm || a.j.PolicyTerm != s.Term {
		a.j.MaxTerm = s.Term
		a.j.PolicyTerm = s.Term
		a.j.SyncMember = s.SyncMember
	}
	if s.Term > a.j.FenceTerm {
		a.j.FenceTerm = s.Term
	}
	// Persist restart inhibition BEFORE claiming that the process has stopped.
	if err := a.save(); err != nil {
		_ = a.pg.stop()
		return ha.Observation{}, err
	}
	if err := a.pg.stop(); err != nil {
		return ha.Observation{}, err
	}
	a.activeWriter.Store(0)
	o := a.observation(s)
	o.Role = ha.Fenced
	o.Stopped = true
	o.FenceTerm = s.Term
	o.Operation = operation
	if info, err := controlData(ctx, a.cfg.DataDir); err == nil {
		o.SystemID = info["Database system identifier"]
		o.Timeline, _ = strconv.ParseInt(info["Latest checkpoint's TimeLineID"], 10, 64)
		o.CleanShutdown = info["Database cluster state"] == "shut down"
		if o.CleanShutdown {
			o.FinalLSN = info["Latest checkpoint location"]
		}
	}
	a.j.Fence = &o
	a.j.SystemID = o.SystemID
	a.j.Timeline = o.Timeline
	if err := a.save(); err != nil {
		return o, err
	}
	return o, a.publish(ctx, o)
}

func (a *Manager) step(ctx context.Context, s ha.State) error {
	if s.Term > a.j.MaxTerm || a.j.PolicyTerm != s.Term {
		a.j.MaxTerm = s.Term
		a.j.PolicyTerm = s.Term
		a.j.SyncMember = s.SyncMember
		if err := a.save(); err != nil {
			return err
		}
	}
	if s.Phase == ha.Stopping || ((s.Phase == ha.Fencing || s.Phase == ha.Preparing) && s.Primary == a.cfg.Instance) {
		_, err := a.fence(ctx, s, s.Operation)
		return err
	}
	member, _ := s.Member(a.cfg.Instance)
	if a.revoked.Load() >= s.Term && s.Primary == a.cfg.Instance {
		_, err := a.fence(ctx, s, "self-fence")
		return err
	}
	if err := a.current(s); err != nil {
		return err
	}
	if !a.j.Initialized && s.Primary == a.cfg.Instance && s.SystemID != "" {
		return fmt.Errorf("refusing to initialize an established database from an incomplete or empty volume journal")
	}
	if err := a.credentials(); err != nil {
		return err
	}
	if s.Primary != a.cfg.Instance && member.RejoinOperation != "" && member.RejoinOperation != a.j.RejoinOperation {
		return a.rejoin(ctx, s, member.RejoinOperation)
	}
	if !a.j.Initialized {
		if s.Term == 1 && s.Primary == a.cfg.Instance {
			if err := a.initialize(ctx, s); err != nil {
				return err
			}
		} else {
			if err := a.clone(ctx, s, "bootstrap"); err != nil {
				return err
			}
		}
	}
	if s.Primary == a.cfg.Instance && a.j.WriterTerm == s.Term && !a.pg.running() {
		_, err := a.fence(ctx, s, "self-fence")
		return err
	}
	if s.Primary != a.cfg.Instance {
		if _, err := os.Stat(filepath.Join(a.cfg.DataDir, "standby.signal")); err != nil {
			if a.pg.running() {
				_, err := a.fence(ctx, s, "self-fence")
				return err
			}
			o := a.observation(s)
			o.Role = ha.Fenced
			o.Message = "Former primary is quarantined; request Rejoin to preserve and rebuild its data"
			return a.publish(ctx, o)
		}
		if a.j.FollowTerm != s.Term {
			if s.Term > 1 && (a.j.PreparedTerm != s.Term-1 || s.Timeline <= s.ParentTimeline) {
				return fmt.Errorf("waiting for verified new timeline or explicit rejoin")
			}
			replay, e1 := ha.LSN(a.j.PreparedLSN)
			fork, e2 := ha.LSN(s.ForkLSN)
			if s.Term > 1 && (e1 != nil || e2 != nil || replay > fork) {
				return fmt.Errorf("standby history requires explicit rejoin")
			}
			a.j.FollowTerm = s.Term
			if err := a.save(); err != nil {
				return err
			}
		}
	}
	if err := a.config(s, false); err != nil {
		return err
	}
	if !a.pg.running() {
		if s.Primary == a.cfg.Instance && s.Term == 1 {
			a.j.WriterTerm = s.Term
			if err := a.save(); err != nil {
				return err
			}
			a.activeWriter.Store(s.Term)
		}
		if err := a.pg.start(func() error { return a.current(s) }); err != nil {
			return err
		}
	}
	o, err := a.observe(ctx, s)
	if err != nil {
		a.ready.Store(false)
		return err
	}
	if s.SystemID != "" && o.SystemID != s.SystemID {
		_, _ = a.fence(ctx, s, "self-fence")
		return fmt.Errorf("database system identity changed")
	}
	if a.j.SystemID != o.SystemID || a.j.Timeline != o.Timeline {
		a.j.SystemID = o.SystemID
		a.j.Timeline = o.Timeline
		if err := a.save(); err != nil {
			return err
		}
	}
	if s.Phase == ha.Preparing && s.Primary != a.cfg.Instance {
		a.ready.Store(false)
		if o.Role != ha.Standby {
			return fmt.Errorf("candidate is not in recovery")
		}
		receiving, err := query(ctx, "SELECT count(*) FROM pg_stat_wal_receiver WHERE status='streaming'")
		if err != nil {
			return err
		}
		if receiving != "0" {
			return fmt.Errorf("waiting for old upstream to disconnect")
		}
		if _, err := query(ctx, "SELECT pg_wal_replay_resume()"); err != nil {
			return err
		}
		o, err = a.observe(ctx, s)
		if err != nil {
			return err
		}
		replay, e1 := ha.LSN(o.ReplayLSN)
		received, e2 := ha.LSN(o.ReceiveLSN)
		if e1 != nil || e2 != nil || received == 0 || replay < received {
			return fmt.Errorf("waiting for all received WAL to replay")
		}
		if _, err := query(ctx, "SELECT pg_wal_replay_pause()"); err != nil {
			return err
		}
		paused, err := query(ctx, "SELECT pg_get_wal_replay_pause_state()")
		if err != nil || paused != "paused" {
			return fmt.Errorf("waiting for replay pause")
		}
		o, err = a.observe(ctx, s)
		if err != nil {
			return err
		}
		o.Role = ha.Prepared
		o.Operation = s.Operation
		a.j.PreparedTerm = s.Term
		a.j.PreparedLSN = o.ReplayLSN
		if err := a.save(); err != nil {
			return err
		}
		return a.publish(ctx, o)
	}
	if s.Primary == a.cfg.Instance && o.Role == ha.Standby {
		if s.Term <= a.j.FenceTerm || s.GrantFence == nil || s.GrantFence.ControlUID != a.cfg.ControlUID || a.j.PreparedTerm != s.Term-1 || o.ReplayLSN != s.ForkLSN {
			return fmt.Errorf("promotion preconditions do not match the grant")
		}
		// Refresh authority immediately before the irreversible transition.
		fresh, err := a.readState(ctx)
		if err != nil {
			return err
		}
		if fresh.Term != s.Term || fresh.Primary != s.Primary || fresh.Phase != ha.Running {
			return fmt.Errorf("promotion grant changed")
		}
		a.j.WriterTerm = s.Term
		if err := a.save(); err != nil {
			return err
		}
		if err := a.reload(ctx, true); err != nil {
			return err
		}
		if err := a.current(s); err != nil {
			return err
		}
		a.activeWriter.Store(s.Term)
		promoteCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		if _, err := command(promoteCtx, "", "pg_ctl", "promote", "-D", a.cfg.DataDir, "-w", "-t", "10"); err != nil {
			return err
		}
		if _, err := query(ctx, "CHECKPOINT"); err != nil {
			return err
		}
		return nil // Observe the actual role/timeline before publishing readiness.
	}
	if s.Primary == a.cfg.Instance && o.Role == ha.Primary {
		a.activeWriter.Store(s.Term)
		if err := a.current(s); err != nil {
			return err
		}
		if err := a.reload(ctx, false); err != nil {
			return err
		}
		for _, m := range s.Members {
			if m.Name != a.cfg.Instance {
				slot := slotName(m.Name)
				if _, err := query(ctx, "SELECT pg_create_physical_replication_slot("+sqlQuote(slot)+",true) WHERE NOT EXISTS (SELECT FROM pg_replication_slots WHERE slot_name="+sqlQuote(slot)+")"); err != nil {
					return err
				}
			}
		}
		ready := true
		if s.SyncMember != "" {
			value, err := query(ctx, "SHOW synchronous_standby_names")
			if err != nil {
				return err
			}
			if value != "ANY 1 (\""+s.SyncMember+"\")" {
				_, _ = a.fence(ctx, s, "self-fence")
				return fmt.Errorf("effective synchronous policy changed")
			}
			value, err = query(ctx, "SELECT count(*)=1 FROM pg_stat_replication WHERE application_name="+sqlQuote(s.SyncMember)+" AND sync_state IN ('sync','quorum') AND state='streaming'")
			if err != nil {
				return err
			}
			ready = value == "t"
		}
		a.ready.Store(ready)
	} else if s.Primary != a.cfg.Instance && o.Role == ha.Standby {
		if s.Phase == ha.Running {
			if err := a.reload(ctx, false); err != nil {
				return err
			}
			if _, err := query(ctx, "SELECT pg_wal_replay_resume() WHERE pg_get_wal_replay_pause_state() <> 'not paused'"); err != nil {
				return err
			}
		}
		connected, err := query(ctx, "SELECT EXISTS (SELECT FROM pg_stat_wal_receiver WHERE status='streaming' AND sender_host="+sqlQuote(a.host(s.Primary))+" AND received_tli="+strconv.FormatInt(s.Timeline, 10)+")")
		if err != nil {
			return err
		}
		a.ready.Store(s.Phase == ha.Running && connected == "t")
		o.Operation = a.j.RejoinOperation
	} else {
		_, _ = a.fence(ctx, s, "self-fence")
		return fmt.Errorf("observed role conflicts with authority")
	}
	return a.publish(ctx, o)
}

func (a *Manager) rejoin(ctx context.Context, s ha.State, operation string) error {
	if _, err := a.fence(ctx, s, operation); err != nil {
		return err
	}
	if a.j.RejoinIntent != operation {
		a.j.RejoinIntent = operation
		if err := a.save(); err != nil {
			return err
		}
	}
	if resumed, err := a.resumeInstall(ctx, s, operation); err != nil || resumed {
		return err
	}
	sum := sha256.Sum256([]byte(operation))
	retained := filepath.Join(a.root, fmt.Sprintf("retained-%x", sum[:12]))
	if _, err := os.Stat(a.cfg.DataDir); err == nil {
		if _, err := os.Stat(retained); err == nil {
			return fmt.Errorf("rejoin install outcome needs inspection; retained and current data both exist")
		}
		if err := os.Rename(a.cfg.DataDir, retained); err != nil {
			return err
		}
		dir, err := os.Open(a.root)
		if err != nil {
			return err
		}
		err = dir.Sync()
		_ = dir.Close()
		if err != nil {
			return err
		}
	}
	a.j.Initialized = false
	if err := a.save(); err != nil {
		return err
	}
	return a.clone(ctx, s, operation)
}

func ConfigFromEnvironment(parameters map[string]string) Config {
	return Config{Namespace: os.Getenv("PGM_NAMESPACE"), Instance: os.Getenv("PGM_INSTANCE"), ClusterUID: os.Getenv("PGM_CLUSTER_UID"), ControlName: os.Getenv("PGM_CONTROL"), ControlUID: os.Getenv("PGM_CONTROL_UID"), PodUID: os.Getenv("PGM_POD_UID"), PVCUID: os.Getenv("PGM_PVC_UID"), Headless: os.Getenv("PGM_HEADLESS"), DataDir: os.Getenv("PGDATA"), Parameters: parameters, Mode: os.Getenv("PGM_MODE")}
}

// Ensure an accidentally whitespace-padded identity is not used as a DNS name.
func (c Config) ValidateNames() error {
	for _, v := range []string{c.Instance, c.Headless, c.Namespace} {
		if strings.TrimSpace(v) != v || strings.ContainsAny(v, "/'\\ \t\n") {
			return fmt.Errorf("invalid instance DNS identity")
		}
	}
	return nil
}
