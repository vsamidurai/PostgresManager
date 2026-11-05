# PostgresManager

A Kubernetes operator being developed for PostgreSQL with regional and global controllers in one codebase and release. The design targets separate Kubernetes clusters per region, one writable primary, and asynchronous or synchronous replication to other regions.

AWS is the first deployment target. Storage, backup, identity, endpoint, and fencing integrations must leave room for GCP, Azure, and on-premises Kubernetes.

The initial implementation is a regional, single-instance `PostgresCluster` controller. It bootstraps PostgreSQL 17 and manages its Pod, retained PVC, headless Service, writer Service, and status. It uses direct Pod/PVC management.

## Implemented scope

- A namespaced `postgres.example.com/v1alpha1` CRD with defaults, validation, and a status subresource.
- Exactly one writable instance, using the official `postgres:17-bookworm` image contract. Compatible digest-pinned images can be supplied at creation.
- A same-namespace bootstrap Secret containing the `postgres` superuser password. The Pod reads it from a file, runs as UID 999, and has no Kubernetes API token.
- Data checksums by default, a restricted set of initial PostgreSQL parameters, and startup/primary readiness checks.
- Durable bootstrap records, Pod/PVC UID checks, and a data-volume identity marker. Reusing a cluster name creates a new storage identity.
- PVC expansion when the StorageClass allows it, without replacing the Pod. Status waits for reported capacity and filesystem expansion; this is not yet an AWS CSI certification.
- `Ready`, `Available`, and `Blocked` conditions, controller leader election, generated RBAC, and deployment manifests.
- Retained PVCs and user-owned Secrets when a cluster is deleted.

This is a development foundation. Replication, failover, automatic Pod replacement, image/configuration updates, backup/restore, TLS, application roles, password rotation, and global controllers are not implemented. `instances` must be `1`; initial configuration is immutable except storage growth. Updating the bootstrap Secret does not rotate the database password.

If an established Pod, PVC, or bootstrap record disappears, the controller reports `RecoveryRequired`. It does not recreate an empty database or assume an old process has stopped. A repair operation will be implemented separately. The kubelet can still restart a container within its existing Pod. An interrupted initial resource-creation request can also require investigation if its outcome cannot be established.

The initial database container uses the official image's entrypoint plus a bootstrap guard. The full HA instance-manager contract in the design is not implemented or selected by this bootstrap slice. The API group is provisional and must be replaced with a project-controlled domain before a stable release.

## Build and test

Requirements: Go 1.26+, `make`, and network access for Go modules and test tools. The end-to-end test additionally needs a running Docker engine, `kubectl`, and `openssl`.

```sh
make build
make test
make test-e2e
```

`make test` runs vet, race-enabled unit tests, and integration tests against a real Kubernetes API server and etcd using envtest. It covers admission, idempotency, resource identity, missing resources, capacity changes, and retention. Envtest does not run database containers or a CSI driver.

`make test-e2e` creates a disposable Kind cluster with a separate kubeconfig. It builds and deploys the operator, boots Postgres, connects through its writer Service, commits data, restarts the operator and database container, verifies persistence, checks missing-Pod behavior and deletion retention, then removes the test cluster. It never uses the caller's current Kubernetes context.

Hosts already running several clusters can exhaust their inotify instance limit before another Kind node starts. See [Kind's troubleshooting guidance](https://kind.sigs.k8s.io/docs/user/known-issues/#pod-errors-due-to-too-many-open-files) if node startup reports `Too many open files`.

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

Connect to `localhost:5432`, database `postgres`, user `postgres`, using the bootstrap password. This first implementation provides password authentication without TLS. Use strict manifest validation so unsupported fields from the broader design are not silently pruned.

For an in-cluster operator, build/publish an image or load it into Kind, update the image in `config/manager/manager.yaml`, and apply `config/default`. The deployment enables leader election. `make run` disables election for local development; run only one such manager. The binary also accepts `--watch-namespace` to limit reconciliation.

Deleting a `PostgresCluster` gracefully deletes its owned Pod and retains the PVC and bootstrap Secret. Namespace deletion and storage-administrator actions can still delete that storage. Inspect `Blocked` conditions and controller logs when initialization or recovery cannot proceed; do not remove identity records to bypass a hold.

## Design documents

| Document | Purpose |
| --- | --- |
| [System design](docs/design.md) | Architecture, ownership, safety rules, replication, operational workflows, security, and provider boundaries. |
| [API design](docs/api.md) | Proposed resources, field semantics, admission rules, status, and illustrative manifests. |
| [Postgres design](docs/postgres.md) | Database process management, HA behavior, and the choice between a native instance manager and Patroni. |

The documents describe the broader target architecture. Their multi-region and multi-instance examples are proposals, not deployable manifests for this initial controller. Use [the implemented sample](config/samples/postgrescluster.yaml) and [Go API types](api/v1alpha1/postgrescluster_types.go) for the supported subset.
