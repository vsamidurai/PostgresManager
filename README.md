# PostgresManager

A Kubernetes operator being developed for PostgreSQL with regional and global controllers in one codebase and release. The design targets separate Kubernetes clusters per region, one writable primary, and asynchronous or synchronous replication to other regions.

AWS is the first deployment target. Storage, backup, identity, endpoint, and fencing integrations must leave room for GCP, Azure, and on-premises Kubernetes.

The implementation manages regional PostgreSQL 17 clusters using direct Pods and retained PVCs. A single-instance profile uses the official PostgreSQL entrypoint; an opt-in replication profile uses our native Go instance manager for streaming replication, verified shutdown fencing, promotion and explicit recovery.

## Implemented scope

- A namespaced `postgres.example.com/v1alpha1` CRD with defaults, validation, and a status subresource.
- One writable primary. The single-instance profile uses `postgres:17-bookworm`; the regional HA profile uses the image built from `Dockerfile.postgres` and supports two to five instances, with scale-up only.
- Synchronous replication to one explicitly named standby, or asynchronous replication. Lossless promotion requires WAL evidence; accepting possible loss never waives fencing.
- Native instance supervision, a durable per-volume authority journal, serialized promotion, identity-bound fencing receipts, and an API-loss watchdog. An unreachable or paused writer blocks takeover until shutdown can be verified.
- `PostgresOperation` resources for switchover and explicit rejoin. Rejoin retains the old data directory and builds a fresh standby. Missing-Pod repair requires an unused verified fence; missing PVCs are never recreated.
- Same-namespace bootstrap and replication password Secrets. HA requires custom TLS certificates and verifies replication endpoints. Pods run as UID 999. Each HA instance has a ServiceAccount that can read authority and update only its own observation; the legacy profile has no API token.
- Data checksums by default, a restricted set of initial PostgreSQL parameters (up to 1,024 characters per value), and startup/primary readiness checks.
- Durable bootstrap records, Pod/PVC UID checks, and a data-volume identity marker. Reusing a cluster name creates a new storage identity.
- PVC expansion when the StorageClass allows it, without replacing the Pod. Status waits for reported capacity and filesystem expansion; this is not yet an AWS CSI certification.
- Readiness, availability and blocked conditions, plus replication health for HA; controller leader election, generated RBAC, and deployment manifests. Single-instance clusters reconcile on resource events; HA also polls for health. Watches cache only managed Pods, PVCs, Services, and ConfigMaps. Bootstrap and identity checks use direct API reads.
- Retained PVCs and user-owned Secrets when a cluster is deleted.

This remains a development implementation. Infrastructure fencing of unreachable nodes, global controllers, cross-region replication, scale-down, image/configuration updates, backup/restore, application roles and password rotation are not implemented. Initial configuration is immutable except storage growth and HA scale-up. Updating a password Secret does not rotate the database password. See [regional HA behavior and limits](docs/regional-ha.md).

If an established Pod, PVC, or authority record disappears, the controller holds recovery unless the explicit HA repair protocol has sufficient fencing evidence. It does not recreate an empty database or assume an old process has stopped. The kubelet can restart a container within its existing Pod; an HA writer manager restart fences the old term before any promotion. An interrupted initial resource-creation request can require investigation if its outcome cannot be established. Definitively rejected creates, such as quota or admission rejections, retry after a persisted 30-second delay. An uncertain response never authorizes recreating a missing Pod or PVC.

The API group is provisional and must be replaced with a project-controlled domain before a stable release. The broader design contains capabilities beyond this regional implementation.

## Build and test

Requirements: Go 1.26+, `make`, and network access for Go modules and test tools. The end-to-end test additionally needs a running Docker engine, `kubectl`, and `openssl`.

```sh
make build
make test
make test-e2e
make test-e2e-ha
make verify
make security
```

`make test` runs vet, race-enabled unit tests, and integration tests against a real Kubernetes API server and etcd using envtest. It covers admission, idempotency, resource identity, missing resources, capacity changes, retention, rejected and ambiguous API writes, concurrent reconciles, Service readiness drift, and watch-driven reconciliation of 21 clusters. It also verifies cache filtering, namespace scope, readiness transitions, and finalization through a running manager. Envtest does not run database containers or a CSI driver.

`make test-e2e` creates a disposable Kind cluster with a separate kubeconfig and checks:

- PostgreSQL bootstrap, authenticated Service connections, configuration, and SQL writes.
- Scaling the controller deployment from one to three replicas, two elected-leader deletions, and reconciliation by each successor without replacing the database.
- Operator rolling restart and PostgreSQL container restart with persistent Pod/PVC identities.
- An immediate PostgreSQL shutdown, WAL replay on restart, unavailable/ready transitions, and retention of acknowledged writes.
- Recovery from a real quota-rejected Pod create without replacing the PVC.
- Missing-primary recovery holds, removal of ready writer endpoints, deletion retention, and restricted service-account permissions.

`make test-e2e-ha` creates a separate four-node Kind cluster. It exercises TLS streaming, database scale-up, primary crash and promotion, retained-data rejoin, switchover, fenced Pod replacement, paused-node refusal, API-loss shutdown, controller restart and fenced deletion. Both scripts remove their clusters and never use the caller's current context. These tests do not certify cloud infrastructure fencing, multi-region failover or a CSI driver.

Hosts already running several clusters can exhaust their inotify instance limit before another Kind node starts. See [Kind's troubleshooting guidance](https://kind.sigs.k8s.io/docs/user/known-issues/#pod-errors-due-to-too-many-open-files) if node startup reports `Too many open files`.

### CI checks

[The CI workflow](.github/workflows/ci.yaml) runs on pushes, pull requests, merge-queue entries and manual dispatch. It uses the current patch of Go 1.26, read-only repository permissions, commit-pinned GitHub Actions and cancellation of superseded runs.

| Check | What it verifies |
| --- | --- |
| Verify and build | Go formatting, module tidiness/checksums, generated deepcopy/CRD/RBAC drift, vet, Staticcheck including integration tests, workflow and shell syntax, Kustomize rendering, and both binaries. |
| Unit tests | Race-enabled unit tests and a coverage profile. |
| Kubernetes API tests | Race-enabled envtest tests with controller/resource/protocol coverage. |
| Go vulnerability check | Reachable Go vulnerabilities reported by `govulncheck`; this does not scan operating-system packages in container images. |
| Kind (single-instance) | Builds the operator image and exercises bootstrap, leader changes, crash recovery and retention. |
| Kind (regional-ha) | Builds both images and exercises replication, fencing, failover, partitions and rejoin. |
| CI required | Passes only when every preceding check succeeds; skipped or cancelled dependencies do not pass. |

After this workflow is merged, require **`CI required`** in the protection rule or ruleset for `main`. Adding the workflow alone does not enable branch protection. No path filters suppress this check, and `merge_group` supports a future merge queue.

Run `make verify` and `make security` locally with a current Go 1.26 patch and `kubectl` installed. These checks install version-pinned analysis tools under `bin/tools/`; Kustomize rendering does not contact a cluster. `make verify-fmt` reports unformatted files; `make fmt` fixes them. Generated-file verification regenerates outputs and compares them with the Git index (the checked-out commit in CI), also rejecting unexpected untracked outputs. After intentional API changes, run `make manifests`, stage the API and generated files together, then verify before committing.

Unit and integration coverage profiles are written separately under `bin/test-results/`. CI retains coverage and test logs for 14 days, including the end-to-end scripts' failure diagnostics. Shell pipelines preserve test failures when collecting logs. Coverage is reported without an arbitrary percentage gate.

[Dependabot](.github/dependabot.yaml) proposes weekly updates for GitHub Actions, Go dependencies and Docker base images. Related Kubernetes modules are grouped. PostgreSQL major image updates are excluded because they require an explicit upgrade design; updates are never automatically merged.

## Run against a development cluster

Use a Kubernetes cluster with a default filesystem StorageClass. The following commands use your current kubeconfig context; only use a development cluster. The initial test target is Kubernetes 1.37.

```sh
make install
make run
```

In another terminal, create the namespace and bootstrap Secret. Replace `/path/to/password-file` with a file containing a strong password; do not commit it.

```sh
kubectl create namespace databases
kubectl -n databases create secret generic example-bootstrap \
  --from-file=password=/path/to/password-file
kubectl apply --validate=strict -f config/samples/postgrescluster.yaml
kubectl -n databases wait postgrescluster/example --for=condition=Ready --timeout=300s
kubectl -n databases get postgresclusters,pods,pvc,services
kubectl -n databases port-forward service/example-rw 5432:5432
```

Connect to `localhost:5432`, database `postgres`, user `postgres`, using the bootstrap password. This single-instance sample provides password authentication without TLS. Use strict manifest validation so unsupported fields from the broader design are not silently pruned.

For an in-cluster operator, build/publish an image or load it into Kind, update the image in `config/manager/manager.yaml`, and apply `config/default`. The deployment enables leader election; additional controller replicas are standbys for availability, and only the lease holder reconciles. Readiness checks cache synchronization without requiring leadership. `make run` disables election for local development; run only one such manager. The binary also accepts `--watch-namespace` to limit reconciliation.

Deleting a `PostgresCluster` retains its PVCs and user Secrets. The single-instance profile gracefully deletes its Pod; HA first requires verified shutdown of every member. Namespace deletion and storage-administrator actions can still delete storage. Inspect `Blocked` conditions and controller logs when initialization or recovery cannot proceed; do not remove identity records to bypass a hold.

## Design documents

| Document | Purpose |
| --- | --- |
| [System design](docs/design.md) | Architecture, ownership, safety rules, replication, operational workflows, security, and provider boundaries. |
| [API design](docs/api.md) | Proposed resources, field semantics, admission rules, status, and illustrative manifests. |
| [Postgres design](docs/postgres.md) | Database process management, HA behavior, and the evaluated implementation choices. |
| [Regional HA](docs/regional-ha.md) | Implemented protocol, deployment, recovery operations and limitations. |

The design documents describe the broader target architecture. Their global examples remain proposals. Use the [single-instance sample](config/samples/postgrescluster.yaml), [regional HA sample](config/samples/postgrescluster-ha.yaml), and [Go API types](api/v1alpha1/postgrescluster_types.go) for the supported subset.
