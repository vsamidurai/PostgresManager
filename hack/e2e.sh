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
k=(kubectl --kubeconfig "$kubeconfig")
cleanup() {
  result=$?
  trap - EXIT
  if [ "$result" -ne 0 ] && [ -s "$kubeconfig" ]; then
    "${k[@]}" get pods,pvc,postgresclusters -A -o wide || true
    "${k[@]}" -n postgres-manager-system logs deployment/postgres-manager --tail=100 || true
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

"${k[@]}" -n postgres-manager-system rollout restart deployment/postgres-manager
"${k[@]}" -n postgres-manager-system rollout status deployment/postgres-manager --timeout=120s
test "$(sql 'SELECT value FROM smoke_test WHERE id = 1')" = persisted
test "$("${k[@]}" -n databases get pod "$pod" -o jsonpath='{.metadata.uid}')" = "$pod_uid"
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

"${k[@]}" -n databases delete pod "$pod" --timeout=120s
"${k[@]}" -n databases wait postgrescluster/example --for=condition=Blocked --timeout=90s
reason=$("${k[@]}" -n databases get postgrescluster example -o jsonpath='{.status.conditions[?(@.type=="Blocked")].reason}')
test "$reason" = RecoveryRequired
test -z "$("${k[@]}" -n databases get pods -l postgres.example.com/cluster=example -o name)"
echo "PASS: missing established Pod did not trigger an unverified replacement"

"${k[@]}" -n databases delete postgrescluster example --timeout=90s
test "$("${k[@]}" -n databases get pvc "$pvc" -o jsonpath='{.metadata.uid}')" = "$pvc_uid"
"${k[@]}" -n databases get secret example-bootstrap -o name
echo "PASS: cluster deletion retained its data PVC and bootstrap Secret"
echo "PostgresManager live Kubernetes/Postgres smoke test passed."
