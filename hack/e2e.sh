#!/usr/bin/env bash
# This test always creates its own cluster and kubeconfig. It never uses the
# caller's current Kubernetes context. Docker must already be running.
set -euo pipefail

project_dir=$(cd "$(dirname "$0")/.." && pwd)
cd "$project_dir"
kind_bin="$project_dir/bin/kind"
if [ ! -x "$kind_bin" ]; then
  mkdir -p "$project_dir/bin"
  GOBIN="$project_dir/bin" go install sigs.k8s.io/kind@v0.33.0
fi
test_dir=$(mktemp -d "${TMPDIR:-/tmp}/postgres-manager-e2e.XXXXXX")
test_cluster="pgm-e2e-$(date +%s)-$$"
kubeconfig="$test_dir/kubeconfig"
k=(kubectl --kubeconfig "$kubeconfig" --request-timeout=30s)
cleanup() {
  result=$?
  trap - EXIT
  if [ "$result" -ne 0 ] && [ -s "$kubeconfig" ]; then
    "${k[@]}" get pods,pvc,postgresclusters -A -o wide || true
    "${k[@]}" -n postgres-manager-system logs deployment/postgres-manager --tail=100 || true
    "${k[@]}" -n postgres-manager-system get leases -o yaml || true
    "${k[@]}" -n databases describe postgrescluster example || true
    "${k[@]}" -n databases get events --sort-by=.metadata.creationTimestamp || true
    "${k[@]}" -n databases logs -l postgres.example.com/cluster=example --tail=100 || true
  fi
  "$kind_bin" delete cluster --name "$test_cluster" || true
  rm -rf "$test_dir"
  exit "$result"
}
trap cleanup EXIT

docker build -t postgres-manager:dev .
"$kind_bin" create cluster --name "$test_cluster" --image kindest/node:v1.37.0 --kubeconfig "$kubeconfig" --wait 120s
"$kind_bin" load docker-image postgres-manager:dev --name "$test_cluster"
"${k[@]}" apply -k config/default
"${k[@]}" -n postgres-manager-system rollout status deployment/postgres-manager --timeout=120s
for operation in 'list secrets' 'delete persistentvolumeclaims' 'update nodes'; do
  # These permissions must stay absent even when the controller watches all namespaces.
  read -r verb resource <<< "$operation"
  if "${k[@]}" auth can-i "$verb" "$resource" -n databases --as=system:serviceaccount:postgres-manager-system:postgres-manager; then
    echo "FAIL: controller has unnecessary permission: $operation" >&2; exit 1
  fi
done
"${k[@]}" create namespace databases
openssl rand -hex 32 > "$test_dir/password"
"${k[@]}" -n databases create secret generic example-bootstrap --from-file=password="$test_dir/password"
"${k[@]}" apply --validate=strict -f config/samples/postgrescluster.yaml
"${k[@]}" -n databases wait postgrescluster/example --for=condition=Ready --timeout=300s

pod=$("${k[@]}" -n databases get postgrescluster example -o jsonpath='{.status.podName}')
pvc=$("${k[@]}" -n databases get postgrescluster example -o jsonpath='{.status.pvcName}')
pod_uid=$("${k[@]}" -n databases get pod "$pod" -o jsonpath='{.metadata.uid}')
pvc_uid=$("${k[@]}" -n databases get pvc "$pvc" -o jsonpath='{.metadata.uid}')
sql() {
  "${k[@]}" -n databases exec "$pod" -- sh -ec 'export PGPASSWORD="$(cat /etc/postgres-bootstrap/password)"; exec psql -h example-rw -U postgres -d postgres -At -v ON_ERROR_STOP=1 -c "$1"' -- "$1"
}
assert_database_identity() {
  test "$("${k[@]}" -n databases get pod "$pod" -o jsonpath='{.metadata.uid}')" = "$pod_uid"
  test "$("${k[@]}" -n databases get pvc "$pvc" -o jsonpath='{.metadata.uid}')" = "$pvc_uid"
  test "$("${k[@]}" -n databases get pods -l postgres.example.com/cluster=example -o jsonpath='{.items[*].metadata.name}')" = "$pod"
  test "$("${k[@]}" -n databases get pvc -l postgres.example.com/cluster=example -o jsonpath='{.items[*].metadata.name}')" = "$pvc"
}
sql 'CREATE TABLE smoke_test (id integer PRIMARY KEY, value text NOT NULL); INSERT INTO smoke_test VALUES (1, '\''persisted'\'');'
test "$(sql 'SELECT value FROM smoke_test WHERE id = 1')" = persisted
test "$(sql 'SHOW data_checksums')" = on
test "$(sql 'SHOW max_connections')" = 50
echo "PASS: bootstrap, password-authenticated Service connection, configuration, and SQL writes"

if "${k[@]}" -n databases patch postgrescluster example --type=merge -p '{"spec":{"instances":2}}'; then
  echo 'FAIL: replication was accepted without an implementation' >&2; exit 1
fi
if "${k[@]}" -n databases patch postgrescluster example --type=merge -p '{"spec":{"postgres":{"majorVersion":18}}}'; then
  echo 'FAIL: uncontrolled major upgrade was accepted' >&2; exit 1
fi
echo "PASS: unsafe spec changes rejected by the API server"

# Scale the controller deployment, not the unsupported Postgres instance count.
"${k[@]}" -n postgres-manager-system scale deployment/postgres-manager --replicas=3
"${k[@]}" -n postgres-manager-system rollout status deployment/postgres-manager --timeout=180s
assert_database_identity
lease=postgres-manager-regional.postgres.example.com
for handover in 1 2; do
  old_holder=$("${k[@]}" -n postgres-manager-system get lease "$lease" -o jsonpath='{.spec.holderIdentity}')
  test -n "$old_holder"
  # controller-runtime's lease identity is <hostname>_<UUID>.
  old_leader=${old_holder%%_*}
  "${k[@]}" -n postgres-manager-system delete pod "$old_leader" --timeout=90s
  test "$(sql 'SELECT value FROM smoke_test WHERE id = 1')" = persisted
  acquired=false
  for attempt in $(seq 1 60); do
    new_holder=$("${k[@]}" -n postgres-manager-system get lease "$lease" -o jsonpath='{.spec.holderIdentity}')
    if [ -n "$new_holder" ] && [ "$new_holder" != "$old_holder" ]; then acquired=true; break; fi
    sleep 2
  done
  test "$acquired" = true
  # Demand new work after acquisition, proving the new leader reconciles.
  "${k[@]}" -n databases patch service example-rw --type=merge -p '{"spec":{"publishNotReadyAddresses":true}}'
  repaired=false
  for attempt in $(seq 1 30); do
    publishes_unready=$("${k[@]}" -n databases get service example-rw -o jsonpath='{.spec.publishNotReadyAddresses}')
    if [ "$publishes_unready" != true ]; then repaired=true; break; fi
    sleep 2
  done
  test "$repaired" = true
  assert_database_identity
  sql "INSERT INTO smoke_test VALUES ($((10 + handover)), 'controller-handover');"
  "${k[@]}" -n postgres-manager-system rollout status deployment/postgres-manager --timeout=120s
done
test "$(sql "SELECT count(*) FROM smoke_test WHERE value = 'controller-handover'")" = 2
echo "PASS: three controller replicas, two leader handovers, reconciliation and writes preserved"

"${k[@]}" -n postgres-manager-system rollout restart deployment/postgres-manager
"${k[@]}" -n postgres-manager-system rollout status deployment/postgres-manager --timeout=120s
test "$(sql 'SELECT value FROM smoke_test WHERE id = 1')" = persisted
assert_database_identity
echo "PASS: operator restart preserved the existing database and Pod"

# Restart the database container, not its Pod. The kubelet owns this restart.
"${k[@]}" -n databases exec "$pod" -- sh -c 'kill -INT 1' || true
restarted=false
for attempt in $(seq 1 90); do
  restarts=$("${k[@]}" -n databases get pod "$pod" -o jsonpath='{.status.containerStatuses[0].restartCount}')
  if [ "${restarts:-0}" -gt 0 ]; then restarted=true; break; fi
  sleep 2
done
test "$restarted" = true
"${k[@]}" -n databases wait pod/"$pod" --for=condition=Ready --timeout=180s
test "$(sql 'SELECT value FROM smoke_test WHERE id = 1')" = persisted
test "$("${k[@]}" -n databases get pvc "$pvc" -o jsonpath='{.metadata.uid}')" = "$pvc_uid"
echo "PASS: container restart preserved committed data and PVC identity"

# SIGQUIT is PostgreSQL's immediate shutdown: next startup must replay WAL.
# See https://www.postgresql.org/docs/17/server-shutdown.html.
sql "CHECKPOINT; INSERT INTO smoke_test VALUES (2, 'crash-persisted');"
before_crash=$("${k[@]}" -n databases get pod "$pod" -o jsonpath='{.status.containerStatuses[0].restartCount}')
"${k[@]}" -n databases wait postgrescluster/example --for=condition=Available=false --timeout=120s > "$test_dir/unavailable.log" 2>&1 &
availability_wait=$!
"${k[@]}" -n databases exec "$pod" -- sh -c 'kill -QUIT 1' || true
wait "$availability_wait"
restarted=false
for attempt in $(seq 1 90); do
  restarts=$("${k[@]}" -n databases get pod "$pod" -o jsonpath='{.status.containerStatuses[0].restartCount}')
  if [ "${restarts:-0}" -gt "$before_crash" ]; then restarted=true; break; fi
  sleep 2
done
test "$restarted" = true
"${k[@]}" -n databases wait pod/"$pod" --for=condition=Ready --timeout=180s
"${k[@]}" -n databases wait postgrescluster/example --for=condition=Ready --timeout=90s
test "$(sql 'SELECT value FROM smoke_test WHERE id = 2')" = crash-persisted
"${k[@]}" -n databases logs "$pod" > "$test_dir/crash-recovery.log"
grep -Eq 'automatic recovery in progress|redo starts at' "$test_dir/crash-recovery.log"
assert_database_identity
echo "PASS: primary crash reported unavailable, replayed WAL, and preserved acknowledged data"

# Exercise a real admission rejection, not only an injected client error.
"${k[@]}" create namespace quota-test
"${k[@]}" -n quota-test create secret generic example-bootstrap --from-file=password="$test_dir/password"
"${k[@]}" -n quota-test create quota block-pods --hard=pods=0
sed 's/namespace: databases/namespace: quota-test/' config/samples/postgrescluster.yaml | "${k[@]}" apply -f -
requested=false
for attempt in $(seq 1 60); do
  quota_pvc=$("${k[@]}" -n quota-test get pvc -l postgres.example.com/cluster=example -o jsonpath='{.items[*].metadata.name}')
  quota_control=$("${k[@]}" -n quota-test get configmap -l postgres.example.com/cluster=example -o jsonpath='{.items[*].metadata.name}')
  if [ -n "$quota_pvc" ] && [ -n "$quota_control" ]; then
    state=$("${k[@]}" -n quota-test get configmap "$quota_control" -o jsonpath='{.data.state\.json}')
    # Require evidence of the rejection before lifting the quota.
    if [[ "$state" == *'"pvcUID":"'* ]]; then requested=true; break; fi
  fi
  sleep 2
done
test "$requested" = true
rejected=false
for attempt in $(seq 1 30); do
  "${k[@]}" -n postgres-manager-system logs -l app.kubernetes.io/name=postgres-manager --tail=100 > "$test_dir/quota.log"
  if grep -q 'exceeded quota: block-pods' "$test_dir/quota.log"; then rejected=true; break; fi
  sleep 2
done
test "$rejected" = true
test -z "$("${k[@]}" -n quota-test get pods -o name)"
quota_pvc_uid=$("${k[@]}" -n quota-test get pvc "$quota_pvc" -o jsonpath='{.metadata.uid}')
"${k[@]}" -n quota-test patch resourcequota block-pods --type=merge -p '{"spec":{"hard":{"pods":"1"}}}'
"${k[@]}" -n quota-test wait postgrescluster/example --for=condition=Ready --timeout=300s
test "$("${k[@]}" -n quota-test get pvc "$quota_pvc" -o jsonpath='{.metadata.uid}')" = "$quota_pvc_uid"
echo "PASS: quota-rejected Pod creation recovered without replacing its PVC"

"${k[@]}" -n databases delete pod "$pod" --timeout=120s
"${k[@]}" -n databases wait postgrescluster/example --for=condition=Blocked --timeout=90s
reason=$("${k[@]}" -n databases get postgrescluster example -o jsonpath='{.status.conditions[?(@.type=="Blocked")].reason}')
test "$reason" = RecoveryRequired
test -z "$("${k[@]}" -n databases get pods -l postgres.example.com/cluster=example -o name)"
"${k[@]}" -n databases wait postgrescluster/example --for=condition=Available=false --timeout=30s
no_writer=false
for attempt in $(seq 1 30); do
  endpoints=$("${k[@]}" -n databases get endpointslices -l kubernetes.io/service-name=example-rw -o go-template='{{range .items}}{{range .endpoints}}{{if .conditions.ready}}{{.targetRef.name}}{{end}}{{end}}{{end}}')
  if [ -z "$endpoints" ]; then no_writer=true; break; fi
  sleep 2
done
test "$no_writer" = true
echo "PASS: missing established Pod did not trigger an unverified replacement"

"${k[@]}" -n databases delete postgrescluster example --timeout=90s
test "$("${k[@]}" -n databases get pvc "$pvc" -o jsonpath='{.metadata.uid}')" = "$pvc_uid"
"${k[@]}" -n databases get secret example-bootstrap -o name
echo "PASS: cluster deletion retained its data PVC and bootstrap Secret"
echo "PostgresManager live controller scale, leader handover, and primary recovery tests passed."
