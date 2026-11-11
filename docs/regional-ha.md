# Regional replication, HA and recovery

The opt-in `spec.replication` profile runs the native Go instance manager as PostgreSQL's parent process. Regional controllers and the instance manager ship from this repository; no second PostgreSQL operator or Patroni deployment is required. This profile currently supports PostgreSQL 17 in one Kubernetes cluster, two to five instances, and scale-up. It must be selected at creation.

## Authority and fencing

The regional controller writes a UID-pinned ConfigMap using resource-version compare-and-swap. It records the primary, monotonically increasing term, synchronous member, database system identifier, timeline, member Pod/PVC UIDs and the current operation. Controller leader election reduces duplicate work; it does not grant database write authority.

Each instance reads this record directly and publishes to a separate observation ConfigMap. Its ServiceAccount can GET authority and its own observation and UPDATE only its own observation. It cannot change authority, another member's observation, Pods or Secrets. Secrets are projected files. Volume journals sit outside PGDATA, contain a checksum, and are committed with file and directory fsync. Loss or identity mismatch of authority or a journal blocks startup.

The manager refreshes authority every two seconds with a three-second API request deadline. A monotonic watchdog revokes writer permission after 15 seconds without a valid authority response and requests shutdown. A restarted writer manager also revokes its previously writable term. A revoked or journal-fenced term cannot become writable again just because connectivity returns.

The acknowledgement member is pinned in the volume journal for each term. An unexpected same-term membership change or a synchronous/asynchronous mode mismatch does not renew authority. Certificate trust, DNS names and expiry are validated before allocating HA resources.

Promotion follows these steps:

1. Reserve the operation and enter `Fencing`; remove the application writer selection.
2. The old manager durably inhibits restart, requests PostgreSQL shutdown, and verifies that every PostgreSQL process in its dedicated PID namespace has exited before publishing a bound fencing receipt.
3. Responsive standbys drain received WAL and durably record a paused replay position. Their observations must match the cluster, authority, Pod, PVC, term, system identifier and timeline.
4. Choose an eligible candidate at the required WAL boundary and check its actual Pod/PVC UIDs through the API. Persist a new term and its previous-writer fencing receipt before promotion.
5. The candidate rechecks authority and its prepared position, journals its writer term, promotes, and reports the observed new timeline. Other prepared standbys follow only if their history is within the promoted fork point.
6. Publish the writer Service only after observing the new primary. Readiness additionally requires the configured synchronous member to be streaming.

**A timeout, expired lease, missing Pod or NotReady node is never a fence.** A paused or unreachable writer blocks promotion with `AwaitingFencing`. There is no AWS/GCP/Azure/on-prem infrastructure fencer in this implementation. Do not force-delete a Pod or edit authority to bypass a hold. A separate PID namespace and exclusive supervision of the data volume are deployment requirements; sharing PostgreSQL processes with another container or host is unsupported.

## Durability

`Synchronous` (default) configures `synchronous_commit=on` and `ANY 1` with exactly one named member. Additional replicas are asynchronous. Failover needs the named member's prepared history or a clean-shutdown WAL boundary. It never silently falls back to asynchronous commits. With two instances, promotion can leave the new writer unavailable for commits until the old member is explicitly rejoined. Three instances allow a prepared third member to supply the new synchronous acknowledgement.

`Asynchronous` uses no synchronous acknowledgement. `Lossless` is the default data-loss policy and requires a proved clean source shutdown boundary that the candidate has replayed. After an unclean asynchronous primary crash, promotion can remain blocked because the lost WAL boundary is unknown. `AllowPotentialDataLoss`, selected explicitly at cluster creation, permits proceeding without that durability proof; it still requires verified old-writer fencing and matching candidate history. Policy changes during the cluster's lifetime are rejected.

Guarantees cover transactions using the configured durability policy. PostgreSQL superusers and applications that override `synchronous_commit` can weaken that policy. Operator administrators, database superusers and the managed images are trusted. This is crash/partition fault handling, not a Byzantine consensus protocol.

Physical slots reserve WAL for assigned replicas. `wal_keep_size=256MB` and `max_slot_wal_keep_size=1GB` bound portions of retention; they are not a total disk-usage cap. A replica that loses required WAL needs explicit rejoin. Archive recovery and WAL-volume management are future work.

## Deploy

Build/publish the operator and `Dockerfile.postgres` images. Set the sample image to your published PostgreSQL image; it must include `/usr/local/bin/postgres-manager-instance`, PostgreSQL 17 tools and UID/GID 999. At least one schedulable node per instance is required by hard Pod anti-affinity. Use a default filesystem StorageClass or set `storage.data.storageClassName`.

Create these same-namespace Secrets before applying [the HA sample](../config/samples/postgrescluster-ha.yaml):

- `regional-bootstrap`: `password`, used for the initial `postgres` role.
- `regional-replication`: a separate single-line `password` for `pgm_replicator`.
- `regional-tls`: `tls.crt`, `tls.key`, `ca.crt`. The server certificate must cover `*.regional-instances.databases.svc` and the writer Service DNS names clients use, such as `regional-rw.databases.svc`.

Replication and base backups use TLS with `verify-full`. Network clients must use TLS; plaintext host authentication is rejected. The internal headless Service resolves unready instances for bootstrap and recovery; applications should connect through the writer Service, which excludes unready Pods. Bootstrap and replication password rotation are not implemented. Keep referenced credentials stable; coordinate certificate updates and verify the resulting connections.

```sh
kubectl apply --validate=strict -f config/samples/postgrescluster-ha.yaml
kubectl -n databases wait postgrescluster/regional --for=condition=Ready --timeout=300s
kubectl -n databases patch postgrescluster regional --type=merge \
  -p '{"spec":{"instances":3}}'
kubectl -n databases get postgrescluster regional -o yaml
```

Status exposes `primaryInstance`, `authorityTerm`, `haPhase`, member identities, roles, timelines and replay positions. `Available` reports the current writer's readiness; `Ready` and `ReplicationHealthy` require all requested instances. `Blocked` describes missing evidence or resources. Healthy HA clusters poll every five seconds as well as responding to resource events. Observation age limits are 20 seconds; timestamps detect stale observations but never prove fencing.

## Switchover and rejoin

Create an immutable operation bound to the exact cluster UID and target instance. Replace the example values with `metadata.uid` and a name from `status.instances`:

```yaml
apiVersion: postgres.example.com/v1alpha1
kind: PostgresOperation
metadata:
  name: switch-to-standby
  namespace: databases
spec:
  clusterName: regional
  clusterUID: REPLACE_WITH_CLUSTER_UID
  type: Switchover
  targetInstance: REPLACE_WITH_STANDBY_INSTANCE
```

Use `type: Rejoin` with a new operation name to rebuild a non-primary. The operation first stops and inhibits that instance, retains its original PGDATA in `retained-<operation-hash>`, and obtains a new TLS base backup from the verified primary. An atomic install marker distinguishes an interrupted clone installation from old database data. Interrupted scratch backups can be retried; retained data is never automatically deleted. Ensure space for the original data plus the new base backup. `pg_rewind` is not implemented.

A missing former-primary Pod can be recreated on its existing PVC only through `Rejoin`, using an unused receipt proving that exact old Pod's writer stopped. The replacement journal migrates to its new Pod UID only under that recorded assignment. Once a member has been authorized to restart as a standby, its older receipt cannot authorize another replacement. A missing standby without adequate fencing evidence, a missing PVC, or an uncertain create outcome remains blocked for investigation.

Operations serialize through the authority record. Completion remains recorded until the operation's terminal status is durable. Deleting an operation does not cancel an already reserved transition. Do not edit the authority ConfigMap manually. A failed synchronous transition may require rejoining the fenced source before application writes resume.

Deleting the cluster first revokes all members, waits for operation-specific shutdown receipts, then deletes assigned Pods and removes the finalizer. PVCs and user Secrets remain. An unreachable member can deliberately block deletion; namespace or infrastructure administrators can still destroy storage outside this protocol.

## Verification and limits

`make test` includes real API admission/controller tests and race-enabled protocol/journal/instance tests. `make test-e2e-ha` uses real PostgreSQL in an isolated multi-node Kind cluster to exercise replication, strict synchronous commit waits, postmaster crashes and kills, manager restart, switchover, retained-data rejoin, fenced Pod replacement, partition refusal, both asynchronous loss policies and fenced deletion. CI runs the HA suite separately from the legacy single-instance suite.

These are development tests, not certification of arbitrary kernels, storage systems, hypervisors or infrastructure fences. Cross-region authority, planned membership-policy changes, image updates, major upgrades, backup archives, scale-down and provider fencing remain outside this implementation.
