package instance

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/vsamidurai/PostgresManager/internal/ha"
)

type postgresProcess struct {
	mu         sync.Mutex
	cmd        *exec.Cmd
	done       chan struct{}
	data, root string
}

func (p *postgresProcess) running() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd == nil {
		return false
	}
	select {
	case <-p.done:
		return false
	default:
		return true
	}
}

func (p *postgresProcess) start(guard func() error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd != nil {
		select {
		case <-p.done:
			p.cmd = nil
		default:
			return nil
		}
	}
	if err := guard(); err != nil {
		return err
	}
	if err := noPostgresProcesses(); err != nil {
		return err
	}
	if err := postgresVersion(p.data); err != nil {
		return err
	}
	cmd := exec.Command("postgres", "-D", p.data, "-c", "config_file="+filepath.Join(p.root, "manager.conf"), "-c", "hba_file="+filepath.Join(p.root, "manager_hba.conf"))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	p.cmd = cmd
	p.done = make(chan struct{})
	done := p.done
	go func() { _ = cmd.Wait(); close(done) }()
	return nil
}

// Never acknowledge a fence merely because the postmaster PID disappeared.
// Fast/immediate shutdown asks PostgreSQL to stop its own children. A separate
// /proc scan must also find no PostgreSQL processes before an acknowledgement.
func (p *postgresProcess) stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd != nil {
		select {
		case <-p.done:
		default:
			_ = p.cmd.Process.Signal(syscall.SIGINT)
			select {
			case <-p.done:
			case <-time.After(12 * time.Second):
				_ = p.cmd.Process.Signal(syscall.SIGQUIT)
				select {
				case <-p.done:
				case <-time.After(10 * time.Second):
					return fmt.Errorf("PostgreSQL shutdown is not confirmed")
				}
			}
		}
		p.cmd = nil
	}
	// A killed postmaster can leave backends behind. This container has its
	// own PID namespace; stop every surviving PostgreSQL process, then verify
	// exit independently. An unkillable process keeps the fence unconfirmed.
	for attempt := 0; attempt < 50; attempt++ {
		pids, err := postgresProcesses()
		if err != nil {
			return err
		}
		if len(pids) == 0 {
			return nil
		}
		for _, pid := range pids {
			_ = syscall.Kill(pid, syscall.SIGQUIT)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return noPostgresProcesses()
}

func noPostgresProcesses() error {
	pids, err := postgresProcesses()
	if err != nil {
		return err
	}
	if len(pids) > 0 {
		return fmt.Errorf("PostgreSQL processes are still present: %v", pids)
	}
	return nil
}

func postgresProcesses() ([]int, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	var pids []int
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		exe, err := os.Readlink(filepath.Join("/proc", entry.Name(), "exe"))
		if os.IsNotExist(err) {
			// As PID 1, reap orphaned PostgreSQL children after a postmaster
			// crash. Never reap unrelated commands owned by exec.Cmd.Wait.
			stat, _ := os.ReadFile(filepath.Join("/proc", entry.Name(), "stat"))
			fields := strings.Fields(string(stat))
			if len(fields) > 3 && fields[1] == "(postgres)" && fields[2] == "Z" && fields[3] == strconv.Itoa(os.Getpid()) {
				var status syscall.WaitStatus
				_, _ = syscall.Wait4(pid, &status, syscall.WNOHANG, nil)
			}
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("cannot verify process exit: %w", err)
		}
		if filepath.Base(strings.TrimSuffix(exe, " (deleted)")) == "postgres" {
			pids = append(pids, pid)
		}
	}
	return pids, nil
}

func postgresVersion(data string) error {
	version, err := os.ReadFile(filepath.Join(data, "PG_VERSION"))
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(version)) != "17" {
		return fmt.Errorf("only PostgreSQL 17 data is supported")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	binary, err := command(ctx, "", "postgres", "--version")
	if err != nil {
		return err
	}
	if !strings.HasPrefix(binary, "postgres (PostgreSQL) 17.") {
		return fmt.Errorf("only PostgreSQL 17 binaries are supported")
	}
	return nil
}

func command(ctx context.Context, input string, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = append(os.Environ(), "LC_ALL=C")
	cmd.Stdin = strings.NewReader(input)
	// Never return command input or stderr: bootstrap SQL may contain credentials.
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("%s failed: %w", name, err)
	}
	return strings.TrimSpace(string(output)), nil
}

func query(ctx context.Context, sql string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return command(ctx, sql, "psql", "-X", "-h", "/var/run/postgresql", "-U", "postgres", "-d", "postgres", "-At", "-v", "ON_ERROR_STOP=1", "-f", "-")
}

func sqlQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
func slotName(instance string) string {
	sum := sha256.Sum256([]byte(instance))
	return fmt.Sprintf("pgm_%x", sum[:12])
}

func (a *Manager) config(s ha.State, localOnly bool) error {
	values := map[string]string{
		"listen_addresses": "*", "port": "5432", "unix_socket_directories": "/var/run/postgresql",
		"wal_level": "replica", "max_wal_senders": "16", "max_replication_slots": "16",
		"wal_keep_size": "256MB", "max_slot_wal_keep_size": "1GB", "hot_standby": "on",
		"wal_log_hints": "on", "full_page_writes": "on", "password_encryption": "scram-sha-256",
		"synchronous_commit": "on", "synchronous_standby_names": "",
		"ssl": "on", "ssl_cert_file": "/etc/postgres-tls/tls.crt", "ssl_key_file": "/etc/postgres-tls/tls.key",
		"ssl_ca_file": "/etc/postgres-tls/ca.crt", "recovery_target_timeline": "latest", "wal_receiver_status_interval": "1s",
	}
	if !localOnly {
		for k, v := range a.cfg.Parameters {
			values[k] = v
		}
	}
	if s.SyncMember != "" {
		values["synchronous_standby_names"] = "ANY 1 (\"" + s.SyncMember + "\")"
	}
	if s.Primary != a.cfg.Instance {
		values["primary_conninfo"] = "host=" + a.host(s.Primary) + " port=5432 user=pgm_replicator application_name=" + a.cfg.Instance + " passfile=/tmp/replication.pgpass sslmode=verify-full sslrootcert=/etc/postgres-tls/ca.crt connect_timeout=3"
		values["primary_slot_name"] = slotName(a.cfg.Instance)
	}
	if localOnly {
		values["listen_addresses"] = ""
		values["ssl"] = "off"
		values["synchronous_standby_names"] = ""
		delete(values, "primary_conninfo")
		delete(values, "primary_slot_name")
	}
	keys := []string{}
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%s = %s\n", k, sqlQuote(strings.ReplaceAll(values[k], "\\", "\\\\")))
	}
	if err := a.writeConfiguration(filepath.Join(a.root, "manager.conf"), []byte(b.String())); err != nil {
		return err
	}
	hba := "local all postgres trust\nlocal all all reject\n"
	if !localOnly {
		hba += "hostssl replication pgm_replicator all scram-sha-256\nhostssl all all all scram-sha-256\n"
	}
	hba += "host all all all reject\nhost replication all all reject\n"
	return a.writeConfiguration(filepath.Join(a.root, "manager_hba.conf"), []byte(hba))
}

func writeChanged(path string, data []byte) (bool, error) {
	previous, err := os.ReadFile(path)
	if err == nil && bytes.Equal(previous, data) {
		return false, nil
	}
	if err != nil && !os.IsNotExist(err) {
		return false, err
	}
	return true, ha.AtomicWrite(path, data, 0600)
}

func (a *Manager) writeConfiguration(path string, data []byte) error {
	changed, err := writeChanged(path, data)
	if changed {
		a.configChanged = true
	}
	return err
}

func (a *Manager) reload(ctx context.Context, force bool) error {
	// Periodic reload also picks up projected TLS certificate updates.
	if !force && !a.configChanged && time.Since(a.lastReload) < 30*time.Second {
		return nil
	}
	if _, err := query(ctx, "SELECT pg_reload_conf()"); err != nil {
		return err
	}
	a.configChanged = false
	a.lastReload = time.Now()
	return nil
}

func (a *Manager) host(instance string) string {
	return instance + "." + a.cfg.Headless + "." + a.cfg.Namespace + ".svc"
}

func (a *Manager) credentials() error {
	password, err := os.ReadFile("/etc/postgres-replication/password")
	if err != nil {
		return err
	}
	value := strings.TrimSuffix(string(password), "\n")
	if value == "" || strings.ContainsAny(value, "\n\r\x00") {
		return fmt.Errorf("replication password must be one nonempty line")
	}
	value = strings.ReplaceAll(strings.ReplaceAll(value, "\\", "\\\\"), ":", "\\:")
	_, err = writeChanged("/tmp/replication.pgpass", []byte("*:5432:*:pgm_replicator:"+value+"\n"))
	return err
}

func (a *Manager) initialize(ctx context.Context, s ha.State) error {
	if _, err := os.Stat(filepath.Join(a.cfg.DataDir, "PG_VERSION")); os.IsNotExist(err) {
		ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		if _, err := command(ctx, "", "initdb", "-D", a.cfg.DataDir, "--username=postgres", "--pwfile=/etc/postgres-bootstrap/password", "--data-checksums", "--auth-local=trust", "--auth-host=scram-sha-256"); err != nil {
			return err
		}
	}
	if err := a.config(s, true); err != nil {
		return err
	}
	if err := a.pg.start(func() error { return a.current(s) }); err != nil {
		return err
	}
	for i := 0; i < 30; i++ {
		if _, err := query(ctx, "SELECT 1"); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	password, err := os.ReadFile("/etc/postgres-replication/password")
	if err != nil {
		return err
	}
	sql := "SET standard_conforming_strings=on; DO $pgm$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='pgm_replicator') THEN CREATE ROLE pgm_replicator WITH REPLICATION LOGIN; END IF; END $pgm$; ALTER ROLE pgm_replicator PASSWORD " + sqlQuote(strings.TrimSuffix(string(password), "\n")) + ";"
	if _, err := query(ctx, sql); err != nil {
		return err
	}
	if err := a.pg.stop(); err != nil {
		return err
	}
	info, err := controlData(ctx, a.cfg.DataDir)
	if err != nil {
		return err
	}
	a.j.SystemID = info["Database system identifier"]
	a.j.Timeline, err = strconv.ParseInt(info["Latest checkpoint's TimeLineID"], 10, 64)
	if err != nil {
		return err
	}
	a.j.Initialized = true
	return a.save()
}

func (a *Manager) clone(ctx context.Context, s ha.State, operation string) error {
	if s.SystemID == "" || s.Primary == a.cfg.Instance {
		return fmt.Errorf("verified upstream is not available")
	}
	if resumed, err := a.resumeInstall(ctx, s, operation); err != nil || resumed {
		return err
	}
	if _, err := os.Stat(a.cfg.DataDir); err == nil {
		return fmt.Errorf("refusing to overwrite an existing data directory")
	}
	staging := filepath.Join(a.root, "clone-in-progress")
	// This directory has never served PostgreSQL. The identity journal must
	// already exist before any interrupted base-backup scratch data is removed.
	if err := os.RemoveAll(staging); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "pg_basebackup", "-h", a.host(s.Primary), "-U", "pgm_replicator", "-D", staging, "-X", "stream", "-c", "fast", "--no-password")
	cmd.Env = append(os.Environ(), "PGPASSFILE=/tmp/replication.pgpass", "PGSSLMODE=verify-full", "PGSSLROOTCERT=/etc/postgres-tls/ca.crt")
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("base backup failed: %w", err)
	}
	info, err := controlData(ctx, staging)
	if err != nil {
		return err
	}
	if info["Database system identifier"] != s.SystemID {
		return fmt.Errorf("base backup system identifier mismatch")
	}
	latest, err := a.readState(ctx)
	if err != nil {
		return err
	}
	if latest.Term != s.Term || latest.Primary != s.Primary || latest.Phase != ha.Running {
		return fmt.Errorf("authority changed during base backup")
	}
	if err := ha.AtomicWrite(filepath.Join(staging, "standby.signal"), nil, 0600); err != nil {
		return err
	}
	if err := a.markClone(staging, s, operation); err != nil {
		return err
	}
	if err := os.Rename(staging, a.cfg.DataDir); err != nil {
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
	a.j.Initialized = true
	a.j.SystemID = s.SystemID
	a.j.FollowTerm = s.Term
	a.j.RejoinOperation = operation
	return a.save()
}

func controlData(ctx context.Context, dir string) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	output, err := command(ctx, "", "pg_controldata", "-D", dir)
	if err != nil {
		return nil, err
	}
	values := map[string]string{}
	for _, line := range strings.Split(output, "\n") {
		key, value, ok := strings.Cut(line, ":")
		if ok {
			values[strings.TrimSpace(key)] = strings.TrimSpace(value)
		}
	}
	return values, nil
}

func (a *Manager) observe(ctx context.Context, s ha.State) (ha.Observation, error) {
	o := a.observation(s)
	result, err := query(ctx, `SELECT pg_is_in_recovery(), (pg_control_system()).system_identifier, COALESCE((SELECT received_tli FROM pg_stat_wal_receiver LIMIT 1),(pg_control_checkpoint()).timeline_id), CASE WHEN pg_is_in_recovery() THEN COALESCE(pg_last_wal_replay_lsn(),'0/0') ELSE pg_current_wal_flush_lsn() END, COALESCE(pg_last_wal_receive_lsn(),'0/0')`)
	if err != nil {
		return o, err
	}
	fields := strings.Split(result, "|")
	if len(fields) != 5 {
		return o, fmt.Errorf("invalid PostgreSQL observation")
	}
	o.Role = ha.Primary
	if fields[0] == "t" {
		o.Role = ha.Standby
	}
	o.SystemID = fields[1]
	o.Timeline, err = strconv.ParseInt(fields[2], 10, 64)
	if err != nil {
		return o, err
	}
	// The receiver row disappears on disconnect while the control checkpoint
	// can still describe an earlier restartpoint. Retain the last verified
	// received timeline for this same database incarnation.
	if o.Role == ha.Standby && o.SystemID == a.j.SystemID && o.Timeline < a.j.Timeline {
		o.Timeline = a.j.Timeline
	}
	o.ReplayLSN = fields[3]
	o.ReceiveLSN = fields[4]
	return o, nil
}
