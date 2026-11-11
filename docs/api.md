# PostgresManager API Design

Status: Draft schema and examples

API group: `postgres.example.com/v1alpha1` pending selection of a domain controlled by the project

The examples below specify the broader proposed API. Regional single-instance and native HA profiles now exist, with a small `PostgresOperation` API for switchover and rejoin. See the [implemented API types](../api/v1alpha1/postgrescluster_types.go), [HA deployment and operations](regional-ha.md), and [implementation limits](../README.md#implemented-scope). Registry digests, cluster identities, bucket names, and Secret references below are placeholders. Defaults and enums below describe the target design unless included in that implemented subset.

## Public resources

All resources are namespaced. A topology namespace belongs to a tenant or another explicit trust boundary. Cross-namespace and cross-cluster references require an allowed registration and cannot be supplied as arbitrary URLs.

| Kind | Location | Role |
| --- | --- | --- |
| `PostgresCluster` | Regional cluster | A standalone or globally managed regional database cluster. |
| `PostgresTopology` | Management cluster | Desired regional members, shared template, replication policy, and topology health. |
| `PostgresOperation` | Management or regional cluster | An immutable request for an auditable operation on a topology or standalone cluster. |
| `PostgresBackup` | Regional cluster | A backup request and its verified artifact metadata. |
| `PostgresBackupRepository` | Regional cluster | Backup-engine repository configuration and credential references. |
| `PostgresUpgradeCampaign` | Management cluster | A fleet upgrade request selecting whole topologies. |
| `KubernetesClusterRegistration` | Management cluster | A trusted regional API connection, cluster identity, and namespace scope. |

Use standard metadata, status subresources, observed generations, condition transition times, and server-side apply field ownership. Set list-map keys for regions, users, databases, conditions, and campaign targets. Preserve meaningful distinctions between absent values, zero, and false. Version conversion is required before incompatible schema evolution.

## PostgresCluster fields

| Field | Meaning and constraints |
| --- | --- |
| `spec.region` | Immutable logical region identity, validated against installation configuration. |
| `spec.postgres.majorVersion` | Desired PostgreSQL major; changes are staged and activated only through a matching authorized upgrade workflow. |
| `spec.postgres.image` | Digest-pinned Postgres image containing PostgreSQL and its instance manager, with matching, verified PostgreSQL and extension metadata. |
| `spec.instances` | Total number of PostgreSQL instances, including the primary or standby ingress. Minimum one. |
| `spec.storage.data` | Storage class, requested capacity, and volume options. Growth is allowed; class changes use migration. |
| `spec.resources` | PostgreSQL container requests and limits. |
| `spec.placement` | Node selectors, tolerations, affinity, and topology spread. |
| `spec.postgres.parameters` | User-manageable PostgreSQL settings; protected replication and HA parameters are rejected. |
| `spec.bootstrap` | Exactly one of `initdb`, `restore`, or `standby`; immutable once data initialization starts. |
| `spec.replication.local` | `mode: Asynchronous\|Synchronous` and `requiredStandbys` for local durable copies. |
| `spec.backup` | Repository reference, schedules, time zone, retention, and verification policy. |
| `spec.access.users` | Managed login roles and credential Secret references. Privilege defaults are restrictive. |
| `spec.access.databases` | Database names and owner-role references. |
| `spec.tls` | Server certificate and replication trust Secret references. |
| `spec.maintenance` | Disruption limits and recurring windows with explicit time zones. |
| `spec.deletionPolicy` | `Retain` by default; explicit destructive policy is separately authorized. |
| `spec.management` | Controller-owned topology binding for global members; omitted for standalone clusters. |

An image change within a major version uses the rolling Postgres update workflow. A major-version change records `UpgradeRequired` until a matching operation is authorized. Normal reconciliation continues from the last effective configuration; it does not prematurely start the new image. For global members, the topology planner controls when each regional desired version is advanced.

`bootstrap.restore` identifies a repository, immutable backup ID or permitted selection rule, source incarnation, and optional recovery target. Recovery target time, LSN, or restore point is a tagged union with explicit timeline and inclusivity. `bootstrap.standby` references an archive source, a streaming source, or both, and remains in recovery. An existing cluster cannot be changed into a fresh restore by editing bootstrap fields.

## Regional cluster example

```yaml
apiVersion: postgres.example.com/v1alpha1
kind: PostgresCluster
metadata:
  name: orders
  namespace: databases
spec:
  region: eu-west-1
  postgres:
    majorVersion: 17
    image: registry.example.com/postgres@sha256:0000000000000000000000000000000000000000000000000000000000000000
    parameters:
      max_connections: "200"
  instances: 3
  bootstrap:
    initdb:
      checksums: true
  storage:
    data:
      storageClassName: ebs-gp3-expandable
      size: 100Gi
  resources:
    requests:
      cpu: "2"
      memory: 8Gi
    limits:
      memory: 8Gi
  replication:
    local:
      mode: Synchronous
      requiredStandbys: 1
  backup:
    repositoryRef:
      name: orders-archive
    fullSchedule: "0 2 * * 0"
    incrementalSchedule: "0 */6 * * *"
    timeZone: Etc/UTC
    retention:
      fullBackups: 7
    verification:
      restoreSchedule: "0 4 * * 1"
  access:
    users:
      - name: orders_app
        credentialsSecretRef:
          name: orders-app-login
    databases:
      - name: orders
        owner: orders_app
  tls:
    serverSecretRef:
      name: orders-server-tls
    replicationTrustSecretRef:
      name: orders-replication-trust
  deletionPolicy: Retain
```

The referenced infrastructure and Secrets must exist. Scheduling, resource sizes, and backup frequency are illustrative, not capacity recommendations.

## Topology fields and ownership

`spec.template` contains regional cluster settings shared by members. `spec.regions` is a map keyed by stable region name with the registration reference, namespace, and a constrained override set. Region overrides initially permit instance counts, storage class/capacity, scheduling, resources, repository references, and local Secret references. They cannot change PostgreSQL major version or the topology's replication history independently.

`spec.bootstrap.primaryRegion` is used only when initializing the topology and is immutable afterward. Actual writer region is durable control state reflected in status. Later moves use operations; a reconciler never moves a writer back merely because an old bootstrap preference remains in Git.

A topology may have a single region, allowing ordinary regional databases to participate in global inventory and upgrade campaigns. Standalone clusters are adopted explicitly before global controllers assume authority over them.

`spec.replication.local` defines the local policy in the writable region. `spec.replication.crossRegion.mode` defines remote acknowledgements. A synchronous policy contains `whenPrimary` entries mapping every eligible primary region to its required standby regions. These mappings are validated before role changes and are not edited automatically during a failure.

Exactly one direct receiver per required remote region enters the initial exact acknowledgement set. Other replicas may be asynchronous. No two members from one region can satisfy a requirement for two different regions. Archive-only members cannot be required synchronous receivers.

`spec.failover.crossRegion` is `Requested` in the first release. `Automatic` remains rejected until the corresponding fencing and authority profile is certified. `spec.failover.defaultDataLossPolicy` defaults to `Lossless`; creating an individual operation can request explicit, privileged acceptance of potential data loss.

## Asynchronous topology example

```yaml
apiVersion: postgres.example.com/v1alpha1
kind: PostgresTopology
metadata:
  name: orders-global
  namespace: production
  labels:
    fleet: customer-facing
spec:
  bootstrap:
    primaryRegion: eu-west-1
  template:
    postgres:
      majorVersion: 17
      image: registry.example.com/postgres@sha256:0000000000000000000000000000000000000000000000000000000000000000
    instances: 3
    storage:
      data:
        size: 100Gi
    deletionPolicy: Retain
  regions:
    - name: eu-west-1
      clusterRef:
        name: eks-europe
      namespace: databases
      overrides:
        storage:
          data:
            storageClassName: ebs-gp3-expandable
        backup:
          repositoryRef:
            name: orders-archive
    - name: us-east-1
      clusterRef:
        name: eks-america
      namespace: databases
      overrides:
        storage:
          data:
            storageClassName: ebs-gp3-expandable
        backup:
          repositoryRef:
            name: orders-archive
  replication:
    local:
      mode: Synchronous
      requiredStandbys: 1
    crossRegion:
      mode: Asynchronous
      transport: StreamingWithArchiveFallback
  failover:
    crossRegion: Requested
    defaultDataLossPolicy: Lossless
```

The two repository objects must resolve to the intended shared archive identity with role-appropriate credentials. The global controller grants archive writes only to the authorized writer; a standby repository reference does not authorize a second archive writer. Access/TLS settings omitted here must be supplied through the template or approved regional references before production activation.

## Synchronous topology policy

The following replaces the previous example's `spec.replication.crossRegion` value:

```yaml
mode: Synchronous
transport: StreamingWithArchiveFallback
acknowledgement: RemoteFlush
onUnavailable: BlockCommits
whenPrimary:
  - region: eu-west-1
    requiredRegions:
      - us-east-1
  - region: us-east-1
    requiredRegions:
      - eu-west-1
```

Archive fallback helps recovery but cannot provide a synchronous commit acknowledgement. `RemoteFlush` maps to PostgreSQL `synchronous_commit=on`; `RemoteApply` is the alternative. Mode changes use the membership-transition workflow and have an explicit effective revision. A spec edit is not proof that the new durability policy is active.

In this two-region policy, losing Europe may leave the American copy with all acknowledged transactions, but the future American writer still requires a durable European copy. Write service remains blocked until that protection is restored or a separately authorized policy change chooses another outcome. Three-region configurations can name another surviving region as the future required receiver, which must catch up before write service opens.

## Registration and repository examples

```yaml
apiVersion: postgres.example.com/v1alpha1
kind: KubernetesClusterRegistration
metadata:
  name: eks-europe
  namespace: production
spec:
  identity:
    kubeSystemNamespaceUID: 00000000-0000-0000-0000-000000000001
  connection:
    kubeconfigSecretRef:
      name: eks-europe-operator-access
  allowedNamespaces:
    - databases
  region: eu-west-1
---
apiVersion: postgres.example.com/v1alpha1
kind: PostgresBackupRepository
metadata:
  name: orders-archive
  namespace: databases
spec:
  engine: PgBackRest
  type: S3
  s3:
    bucket: example-orders-backups
    region: eu-west-1
    prefix: postgres/orders
  authentication:
    workloadIdentity:
      serviceAccountName: orders-backup
  encryption:
    repositoryKeySecretRef:
      name: orders-backup-key
```

The repository prefix is a base prefix. The controller appends immutable topology and incarnation identifiers; a display name cannot cause archive reuse. A `kubeconfigSecretRef` is an initial connection option, restricted to administrator-managed content. The registration controller verifies endpoint, CA, actual cluster identity, and scope before enabling it. Production identity integrations should use renewable credentials rather than unmanaged long-lived tokens.

Repository `type` is a tagged union: S3, GCS, Azure, or POSIX. Unsupported provider capabilities are reported and rejected for dependent operations. Merely accepting GCS/Azure fields does not mark those providers certified. Static credentials, when necessary, are referenced in regional Secrets and never embedded in the custom resource.

## Operations

Operations target either a topology in the management cluster or a standalone regional cluster. Globally managed members reject user-created disruptive local operations; the global controller creates their authorized regional work.

| Field | Meaning |
| --- | --- |
| `spec.targetRef` | Same-namespace target kind, name, and UID. UID prevents name reuse from redirecting an operation. |
| `spec.type` | `Switchover`, `Failover`, `MajorUpgrade`, `Rejoin`, or `ChangeDurability`; additional types require typed parameters. |
| `spec.execution` | `Plan` performs validation and creates a plan only; `Execute` authorizes the requested workflow. |
| `spec.preconditions` | Expected epoch, effective revision, source version, and optional plan fingerprint. |
| `spec.parameters` | A tagged union specific to the operation type. |
| `spec.dataLossPolicy` | `Lossless` or `AllowPotentialDataLoss`, meaningful for recovery operations. |
| `spec.reason` | Human-readable audit reason. Mandatory for loss acceptance or policy reduction. |
| `spec.cancelRequested` | The only ordinarily mutable execution field; honored only at safe cancellation points. |

Creating an `Execute` operation is the authorization to run it, subject to RBAC, admission, maintenance, and Postgres management preconditions. There is no second implicit approval flag. A plan is a snapshot: execution must revalidate its target UID, effective revision, and state.

Admission checks operation permissions against the target. In particular, ordinary permission to create an operation does not authorize loss acceptance: the webhook issues a SubjectAccessReview for an additional `accept-data-loss` action on the target resource. Equivalent checks govern destructive deletion and reduced-durability changes. This is an explicit admission policy, not an assumption that Kubernetes RBAC understands arbitrary spec fields.

```yaml
apiVersion: postgres.example.com/v1alpha1
kind: PostgresOperation
metadata:
  name: orders-move-to-america
  namespace: production
spec:
  targetRef:
    kind: PostgresTopology
    name: orders-global
    uid: 00000000-0000-0000-0000-000000000010
  type: Switchover
  execution: Execute
  preconditions:
    topologyEpoch: 7
    effectiveRevision: "sha256:example-revision"
  parameters:
    switchover:
      targetRegion: us-east-1
  dataLossPolicy: Lossless
  reason: Planned regional maintenance
```

Each execution uses target UID plus operation UID as its durable identity. Controller-generated regional step identities also include epoch and step ID. Spec changes do not silently retarget an in-flight operation. Progress cannot be forged by editing a public status field; protected control state and evidence from the Postgres instance manager authorize each step.

## Backup and campaign requests

```yaml
apiVersion: postgres.example.com/v1alpha1
kind: PostgresBackup
metadata:
  name: orders-before-maintenance
  namespace: databases
spec:
  clusterRef:
    name: orders
    uid: 00000000-0000-0000-0000-000000000020
  type: Full
  repositoryRef:
    name: orders-archive
---
apiVersion: postgres.example.com/v1alpha1
kind: PostgresUpgradeCampaign
metadata:
  name: customer-facing-to-18
  namespace: production
spec:
  selector:
    matchLabels:
      fleet: customer-facing
  target:
    majorVersion: 18
    image: registry.example.com/postgres@sha256:1111111111111111111111111111111111111111111111111111111111111111
  execution: Plan
  rollout:
    canaryCount: 1
    maxConcurrentTopologies: 1
    failurePolicy: Pause
  upgrade:
    method: InPlace
    transferMode: ClonePreferred
    allowLinkMode: false
    requireProtectionBeforeWrites: true
```

A backup record reports repository artifact ID, incarnation, PostgreSQL version, start/end time and WAL locations, manifest checksum, checksum errors, and verification result. Retention holds reference immutable artifact IDs. Object-store retention is not driven by Kubernetes garbage collection.

Campaign selection is restricted to the namespace and frozen into target UIDs at planning. Executing a reviewed plan requires a new request with `spec.planRef` containing the plan's name, UID, and fingerprint, followed by revalidation of its frozen target set. Changing a plan spec into execution does not bypass immutable-operation semantics. `ClonePreferred` falls back to a certified copy path when clone is unavailable; link mode is never selected while `allowLinkMode` is false.

## Status and conditions

Cluster status includes `observedGeneration`, effective configuration hash, actual PostgreSQL and instance manager versions, bootstrap state, instance identities, writer/ingress identity, local term, volume state, and operation reference. Topology status additionally includes the authoritative writer region, epoch, per-region observation age, committed durability revision, and aggregate protection state.

| Condition | Meaning when true |
| --- | --- |
| `Available` | The intended client service is currently usable. Standby-only regional availability is read availability. |
| `Ready` | Desired configuration is reconciled and required operational checks pass. |
| `ReplicationHealthy` | Required replication paths meet the configured policy within fresh observations. |
| `DurabilitySatisfied` | The Postgres instance manager reports the configured acknowledgement set effective and available. This is not a per-transaction audit. |
| `BackupReady` | At least one verified recovery artifact meets the backup policy. |
| `ProtectionRestored` | Required replica placement, durability, and post-operation recovery protection are restored. |
| `Degraded` | Service or protection differs from the desired policy. |
| `Blocked` | An operation cannot advance; reason and required evidence are recorded. |

Missing observations are `Unknown`, never implicitly true. All conditions carry `observedGeneration`, reason, message, and transition time. A topology may be available while not ready; for example, it can serve writes under an asynchronous policy while an optional replica is rebuilding.

Operation status includes phase, current step, attempts, timestamps, source/target versions, plan fingerprint, fencing receipts, WAL evidence, and a recovery action. Jobs and logs are referenced by UID. High-frequency WAL samples belong in metrics; API status updates are coalesced with a documented freshness bound. Safety decisions query fresh evidence from the Postgres instance manager.

## Admission and defaulting

Structural schemas and CEL handle local shape/range rules. Webhooks perform authorization and cross-field checks that cannot be expressed reliably in schema. Controllers recheck external state at execution; admission success never proves an operation remains safe later.

Required checks include:

1. Positive capacity and instance count; synchronous local count strictly less than available instance count.
2. Exactly one bootstrap source; no bootstrap mutation after initialization.
3. No storage shrink or direct storage-class migration.
4. Unique stable regions and instances; valid registered destination and namespace.
5. Required synchronous regions exist, differ from the future primary, and support direct streaming.
6. Complete `whenPrimary` mappings for eligible writer regions; no automatic policy reduction.
7. No direct mutation of protected Postgres management, authority, or acknowledgement-set fields.
8. No new major image activation without a matching authorized upgrade operation.
9. No conflicting topology-level operations or independently requested disruptive work on a global member.
10. Secret references remain within approved regional scope; fields cannot carry arbitrary credentials or shell commands.

Defaults are versioned API behavior. Admission must not guess AWS storage classes, create broad cloud permissions, or choose an unverified fencing method. Provider capability checks remain explicit preconditions.
