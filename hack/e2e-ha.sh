#!/usr/bin/env bash
# Own cluster, kubeconfig, worker pauses and network rules only.
set -euo pipefail
project_dir=$(cd "$(dirname "$0")/.." && pwd)
cd "$project_dir"
kind_bin="$project_dir/bin/kind"
if [ ! -x "$kind_bin" ]; then GOBIN="$project_dir/bin" go install sigs.k8s.io/kind@v0.33.0; fi
test_dir=$(mktemp -d "${TMPDIR:-/tmp}/postgres-manager-ha.XXXXXX")
test_cluster="pgm-ha-$(date +%s)-$$"
kubeconfig="$test_dir/kubeconfig"
k=(kubectl --kubeconfig "$kubeconfig" --request-timeout=20s)
paused_node=""
blocked_node=""
blocked_pod_ip=""
cleanup() {
 result=$?; trap - EXIT
 if [ -n "$paused_node" ]; then docker unpause "$paused_node" || true; fi
 if [ -n "$blocked_node" ]; then docker exec "$blocked_node" iptables -D FORWARD -s "$blocked_pod_ip" -p tcp --dport 6443 -j DROP || true; fi
 if [ "$result" -ne 0 ]; then
  "${k[@]}" get pods -A -o wide || true
  "${k[@]}" -n databases get postgresclusters,postgresoperations -o yaml || true
  "${k[@]}" -n databases get configmaps -o yaml || true
  "${k[@]}" -n postgres-manager-system logs deployment/postgres-manager --tail=60 || true
  "${k[@]}" -n databases logs -l postgres.example.com/cluster=regional --tail=70 --prefix=true || true
 fi
 "$kind_bin" delete cluster --name "$test_cluster" || true
 rm -rf "$test_dir"
 exit "$result"
}
trap cleanup EXIT
docker build -t postgres-manager:dev .
docker build -f Dockerfile.postgres -t postgres-manager-postgres:dev .
cat > "$test_dir/kind.yaml" <<'YAML'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
- role: worker
- role: worker
- role: worker
YAML
"$kind_bin" create cluster --name "$test_cluster" --image kindest/node:v1.37.0 --config "$test_dir/kind.yaml" --kubeconfig "$kubeconfig" --wait 180s
"$kind_bin" load docker-image postgres-manager:dev postgres-manager-postgres:dev --name "$test_cluster"
"${k[@]}" apply -k config/default
"${k[@]}" -n postgres-manager-system patch deployment postgres-manager --type=merge -p '{"spec":{"template":{"spec":{"nodeSelector":{"node-role.kubernetes.io/control-plane":""},"tolerations":[{"key":"node-role.kubernetes.io/control-plane","operator":"Exists","effect":"NoSchedule"}]}}}}'
"${k[@]}" -n postgres-manager-system rollout status deployment/postgres-manager --timeout=180s
"${k[@]}" create namespace databases
for name in bootstrap replication; do
 openssl rand -hex 24 > "$test_dir/$name"
 "${k[@]}" -n databases create secret generic "regional-$name" --from-file=password="$test_dir/$name"
done
cat > "$test_dir/tls.cnf" <<'CONF'
[req]
distinguished_name=dn
x509_extensions=extensions
prompt=no
[dn]
CN=regional
[extensions]
basicConstraints=critical,CA:TRUE
keyUsage=critical,digitalSignature,keyEncipherment,keyCertSign
extendedKeyUsage=serverAuth
subjectAltName=DNS:*.regional-instances.databases.svc,DNS:regional-rw,DNS:regional-rw.databases.svc
CONF
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -config "$test_dir/tls.cnf" -keyout "$test_dir/tls.key" -out "$test_dir/tls.crt" 2>/dev/null
"${k[@]}" -n databases create secret generic regional-tls --from-file=tls.crt="$test_dir/tls.crt" --from-file=ca.crt="$test_dir/tls.crt" --from-file=tls.key="$test_dir/tls.key"
"${k[@]}" apply -f config/samples/postgrescluster-ha.yaml
"${k[@]}" -n databases wait postgrescluster/regional --for=condition=Ready --timeout=300s
primary() { "${k[@]}" -n databases get postgrescluster regional -o jsonpath='{.status.primaryInstance}'; }
term() { "${k[@]}" -n databases get postgrescluster regional -o jsonpath='{.status.authorityTerm}'; }
local_sql() { "${k[@]}" -n databases exec "$1" -- psql -X -U postgres -h /var/run/postgresql -At -v ON_ERROR_STOP=1 -c "$2"; }
writer_sql() {
 "${k[@]}" -n databases exec "$(primary)" -- sh -ec 'export PGPASSWORD="$(cat /etc/postgres-bootstrap/password)" PGSSLMODE=verify-full PGSSLROOTCERT=/etc/postgres-tls/ca.crt; exec psql -X -h regional-rw -U postgres -At -v ON_ERROR_STOP=1 -c "$1"' -- "$1"
}
wait_primary_change() {
 for attempt in $(seq 1 90); do
  next=$(primary)
  if [ -n "$next" ] && [ "$next" != "$1" ] && "${k[@]}" -n databases get postgrescluster regional -o jsonpath='{.status.conditions[?(@.type=="Available")].status}' | grep -q True; then return; fi
  sleep 2
 done
 echo 'FAIL: no available successor' >&2; return 1
}
wait_operation() {
 for attempt in $(seq 1 120); do
  phase=$("${k[@]}" -n databases get postgresoperation "$1" -o jsonpath='{.status.phase}')
  if [ "$phase" = Succeeded ]; then return; fi
  if [ "$phase" = Rejected ]; then echo "FAIL: operation $1 rejected" >&2; return 1; fi
  sleep 2
 done
 echo "FAIL: operation $1 did not finish" >&2; return 1
}
operation() {
 uid=$("${k[@]}" -n databases get postgrescluster regional -o jsonpath='{.metadata.uid}')
 cat <<YAML | "${k[@]}" apply -f -
apiVersion: postgres.example.com/v1alpha1
kind: PostgresOperation
metadata:
  name: $1
  namespace: databases
spec:
  clusterName: regional
  clusterUID: $uid
  type: $2
  targetInstance: $3
YAML
}
rejoin() {
 operation "$1" Rejoin "$2"
 wait_operation "$1"
 "${k[@]}" -n databases wait postgrescluster/regional --for=condition=Ready --timeout=180s
 test "$(local_sql "$2" 'SELECT pg_is_in_recovery()')" = t
 "${k[@]}" -n databases exec "$2" -- sh -ec 'test -n "$(find /var/lib/postgresql/data -maxdepth 1 -name "retained-*" -type d)"'
}
assert_ledger() { test "$(writer_sql 'SELECT count(*) FROM ledger')" = "$1"; }
old=$(primary)
old_uid=$("${k[@]}" -n databases get pod "$old" -o jsonpath='{.metadata.uid}')
writer_sql "CREATE TABLE ledger(id integer PRIMARY KEY); INSERT INTO ledger SELECT generate_series(1,100);"
"${k[@]}" -n databases patch postgrescluster regional --type=merge -p '{"spec":{"instances":3}}'
for attempt in $(seq 1 90); do
 if [ "$("${k[@]}" -n databases get postgrescluster regional -o jsonpath='{.status.readyInstances}')" = 3 ]; then break; fi
 sleep 2
done
test "$("${k[@]}" -n databases get postgrescluster regional -o jsonpath='{.status.readyInstances}')" = 3
test "$("${k[@]}" -n databases get pod "$old" -o jsonpath='{.metadata.uid}')" = "$old_uid"
test "$(writer_sql "SELECT count(*) FROM pg_stat_replication WHERE state='streaming'")" = 2
for pod in $("${k[@]}" -n databases get pods -l postgres.example.com/cluster=regional -o jsonpath='{.items[*].metadata.name}'); do
 if [ "$pod" != "$old" ]; then test "$(local_sql "$pod" 'SELECT count(*) FROM ledger')" = 100; fi
done
control=$("${k[@]}" -n databases get configmaps -o name | grep -- '-control$' | cut -d/ -f2)
if "${k[@]}" auth can-i update "configmap/$control" -n databases --as="system:serviceaccount:databases:$old"; then echo 'FAIL: instance can write authority' >&2; exit 1; fi
echo 'PASS: TLS streaming replication, 2-to-3 scale-up, stable data identities and restricted authority RBAC'

# The additional asynchronous replica cannot substitute for the named sync peer.
writer_sql 'CREATE TABLE durability_probe(id integer);'
sync_member=$(writer_sql "SELECT application_name FROM pg_stat_replication WHERE sync_state IN ('sync','quorum')")
paused_node=$("${k[@]}" -n databases get pod "$sync_member" -o jsonpath='{.spec.nodeName}')
docker pause "$paused_node"
local_sql "$old" "SET application_name='pgm-sync-probe'; INSERT INTO durability_probe VALUES (1);" > "$test_dir/sync-wait.log" 2>&1 &
probe_pid=$!
sleep 5
test "$(local_sql "$old" "SELECT count(*) FROM pg_stat_activity WHERE application_name='pgm-sync-probe' AND wait_event='SyncRep'")" = 1
docker unpause "$paused_node"; paused_node=""
wait "$probe_pid"
echo 'PASS: loss of the named synchronous standby blocks commits without weakening policy'

# Crash the postmaster while its independent manager remains alive.
writer_sql 'CHECKPOINT; INSERT INTO ledger SELECT generate_series(101,200);'
"${k[@]}" -n databases exec "$old" -- sh -ec 'kill -QUIT "$(head -n1 "$PGDATA/postmaster.pid")"'
wait_primary_change "$old"
test "$(term)" = 2
assert_ledger 200
if local_sql "$old" 'SELECT 1' 2>/dev/null; then echo 'FAIL: fenced source still accepts SQL' >&2; exit 1; fi
before_restart=$("${k[@]}" -n databases get pod "$old" -o jsonpath='{.status.containerStatuses[0].restartCount}')
"${k[@]}" -n databases exec "$old" -- sh -c 'kill -TERM 1' || true
for attempt in $(seq 1 60); do
 restarts=$("${k[@]}" -n databases get pod "$old" -o jsonpath='{.status.containerStatuses[0].restartCount}')
 if [ "${restarts:-0}" -gt "$before_restart" ]; then break; fi
 sleep 2
done
test "${restarts:-0}" -gt "$before_restart"
if local_sql "$old" 'SELECT 1' 2>/dev/null; then echo 'FAIL: restarting the manager revived the fenced writer' >&2; exit 1; fi
rejoin rejoin-crash "$old"
echo 'PASS: primary crash, verified fencing, lossless promotion and retained-data rejoin'

# Voluntary switchover followed by replacement of the already fenced old Pod.
old=$(primary)
target=$("${k[@]}" -n databases get pods -l postgres.example.com/cluster=regional -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | grep -v "^$old$" | head -n1)
operation switch Switchover "$target"
wait_operation switch
test "$(primary)" = "$target"
assert_ledger 200
"${k[@]}" -n databases delete pod "$old" --wait=true --timeout=90s
rejoin rejoin-replacement "$old"
echo 'PASS: switchover and explicitly authorized recovery of a fenced missing Pod'

# A paused primary node cannot acknowledge shutdown. Never promote on expiry.
old=$(primary); before_term=$(term)
paused_node=$("${k[@]}" -n databases get pod "$old" -o jsonpath='{.spec.nodeName}')
docker pause "$paused_node"
sleep 35
test "$(term)" = "$before_term"
test "$(primary)" = "$old"
test "$("${k[@]}" -n databases get postgrescluster regional -o jsonpath='{.status.conditions[?(@.type=="Blocked")].reason}')" = AwaitingFencing
docker unpause "$paused_node"; paused_node=""
wait_primary_change "$old"
assert_ledger 200
rejoin rejoin-paused "$old"
echo 'PASS: paused-node partition blocked takeover until the source resumed and verified shutdown'

# Disconnect only the primary from the Kubernetes API. Streaming stays up;
# local authority watchdog must stop writes, but takeover still needs receipt.
old=$(primary); before_term=$(term)
blocked_node=$("${k[@]}" -n databases get pod "$old" -o jsonpath='{.spec.nodeName}')
blocked_pod_ip=$("${k[@]}" -n databases get pod "$old" -o jsonpath='{.status.podIP}')
docker exec "$blocked_node" iptables -I FORWARD 1 -s "$blocked_pod_ip" -p tcp --dport 6443 -j DROP
sleep 35
if local_sql "$old" 'SELECT 1' 2>/dev/null; then echo 'FAIL: API-isolated primary still accepts SQL' >&2; exit 1; fi
test "$(term)" = "$before_term"
docker exec "$blocked_node" iptables -D FORWARD -s "$blocked_pod_ip" -p tcp --dport 6443 -j DROP
blocked_node=""
wait_primary_change "$old"
assert_ledger 200
rejoin rejoin-api-loss "$old"
echo 'PASS: API-loss watchdog stopped PostgreSQL; takeover waited for a verifiable fence'

old=$(primary)
"${k[@]}" -n databases exec "$old" -- sh -ec 'kill -KILL "$(head -n1 "$PGDATA/postmaster.pid")"'
wait_primary_change "$old"
assert_ledger 200
if local_sql "$old" 'SELECT 1' 2>/dev/null; then echo 'FAIL: a backend survived verified fencing' >&2; exit 1; fi
rejoin rejoin-killed-postmaster "$old"
echo 'PASS: killed postmaster and surviving backends are fenced before takeover'

"${k[@]}" -n postgres-manager-system scale deployment/postgres-manager --replicas=3
"${k[@]}" -n postgres-manager-system rollout status deployment/postgres-manager --timeout=180s
"${k[@]}" -n postgres-manager-system rollout restart deployment/postgres-manager
"${k[@]}" -n postgres-manager-system rollout status deployment/postgres-manager --timeout=180s
assert_ledger 200
"${k[@]}" -n databases delete postgrescluster regional --timeout=180s
test "$("${k[@]}" -n databases get pvc -l postgres.example.com/cluster=regional -o name | wc -l | tr -d ' ')" = 3
echo 'PASS: controller restart and serialized fenced deletion retain all three data volumes'

create_async() {
 sed -e 's/mode: Synchronous/mode: Asynchronous/' -e "s/dataLossPolicy: Lossless/dataLossPolicy: $1/" config/samples/postgrescluster-ha.yaml | "${k[@]}" apply -f -
 "${k[@]}" -n databases wait postgrescluster/regional --for=condition=Ready --timeout=300s
 writer_sql 'CREATE TABLE ledger(id integer PRIMARY KEY); INSERT INTO ledger SELECT generate_series(1,100);'
 old=$(primary)
 peer=$("${k[@]}" -n databases get postgrescluster regional -o jsonpath='{.status.instances[*].name}' | tr ' ' '\n' | grep -v "^$old$")
 for attempt in $(seq 1 30); do
  if [ "$(local_sql "$peer" 'SELECT count(*) FROM ledger')" = 100 ]; then break; fi
  sleep 2
 done
 test "$(local_sql "$peer" 'SELECT count(*) FROM ledger')" = 100
}
create_async Lossless
"${k[@]}" -n databases exec "$old" -- sh -ec 'kill -QUIT "$(head -n1 "$PGDATA/postmaster.pid")"'
for attempt in $(seq 1 60); do
 reason=$("${k[@]}" -n databases get postgrescluster regional -o jsonpath='{.status.conditions[?(@.type=="Blocked")].reason}')
 if [ "$reason" = DurabilityProofUnavailable ]; then break; fi
 sleep 2
done
test "$reason" = DurabilityProofUnavailable
test "$(term)" = 1
test "$(primary)" = "$old"
"${k[@]}" -n databases delete postgrescluster regional --timeout=180s
echo 'PASS: asynchronous lossless policy refuses unprovable recovery after a crash'

create_async AllowPotentialDataLoss
"${k[@]}" -n databases exec "$old" -- sh -ec 'kill -QUIT "$(head -n1 "$PGDATA/postmaster.pid")"'
wait_primary_change "$old"
assert_ledger 100
if local_sql "$old" 'SELECT 1' 2>/dev/null; then echo 'FAIL: async promotion left the old writer running' >&2; exit 1; fi
rejoin rejoin-async "$old"
"${k[@]}" -n databases delete postgrescluster regional --timeout=180s
echo 'PASS: explicit asynchronous loss acceptance permits fenced takeover and rejoin'
