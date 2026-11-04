# PostgresManager Delivery and Validation Plan

Status: Proposed implementation sequence

Delivery proceeds by verified capabilities. Milestone completion means the specified behavior has passed its tests on a declared compatibility matrix, not that a CRD field or provider enum has been added. Calendar estimates require team capacity, supported deployment scale, database sizes, and extension requirements.

## Milestones

| Milestone | Deliverables | Exit criteria |
| --- | --- | --- |
| M0 Runtime and protocol decision | Native-manager/Patroni evaluation, authority protocol, operation journal, API review, test harness. | One runtime selected; fencing and durability transitions reviewed; failure experiments identify no unresolved path to conflicting writers or lost policy-covered commits. |
| M1 Regional foundation | Go project, CRDs, schemas, webhook, scoped RBAC, Helm packaging, pod/PVC lifecycle, runtime integration, standalone clusters, status, TLS, SQL roles. | Bootstrap, reconcile, scale, restart, deletion retention, and authorized local failover pass on the initial runtime/storage profile. |
| M2 AWS operations and recovery | EBS expansion, minor updates, S3 backups, PITR, clones, archive standby, credential and certificate rotation, restore drills. | Verified recovery from a destroyed test cluster; live expansion preserves pod UID and server start time; interrupted operations resume safely. |
| M3 Global asynchronous topology | Registration, scoped remote clients, assignments, streaming/archive fallback, global status, requested switchover/failover, fencing, rejoin. | Two-cluster partitions, old-region return, control-plane loss, and uncertain command outcomes preserve single-writer authority. Accepted loss is reported accurately. |
| M4 Global synchronous topology | Exact acknowledgement sets, future-primary policies, durable membership transitions, local/global promotion evidence. | Client commit-ledger tests preserve acknowledged policy-covered transactions; required-copy loss blocks commits; no silent durability reduction. |
| M5 Major and fleet upgrades | In-place workflow, compatible image catalog, clone/link/copy capability checks, replica rebuild, archive incarnation changes, campaigns. | Certified version-pair and extension tests pass; failures at destructive boundaries have documented, exercised recovery paths; canary failure halts later waves. |
| M6 Provider certification | GCS and Azure restore/clone, GCS archive standby, cloud identity integrations, on-premises CSI/object-store/fencing profile. | Provider-specific recovery, permissions, outage, and fencing tests pass without changes to core operation state machines. |

AWS asynchronous deployments may be piloted before M4. A release cannot claim the complete requested multi-region synchronous feature set until M4 passes. Automatic cross-region failover is a later capability with separate approval of its protocol and fencing profile; explicit failover remains supported.

## Requirement traceability

| Requested feature | Implementation area | Evidence required |
| --- | --- | --- |
| Rolling cluster changes and quick minor updates | Change planner and regional operations. | Replica-first update, checked switchover, bounded disruption, restart recovery, and no skipped required receiver. |
| Live PVC and EBS resizing | Storage inspection and expansion reconciler. | Larger filesystem with unchanged pod UID and PostgreSQL start time on each certified driver/filesystem. |
| Fast in-place major upgrades | Upgrade operation and runtime task. | Compatible version pair, measured downtime, safe handling of transfer modes, replica recovery, and exercised rollback boundary. |
| Global upgrade of all clusters | Campaign controller. | Frozen selection of all intended topology UIDs, bounded concurrency, per-target results, and pause/recovery across controller restart. |
| AWS, GCS, and Azure restore/clone | Repository integration and restore bootstrap. | Recovery checks on each provider, source preservation, independent destination archive identity, and key/credential recovery. |
| Standby from S3 or GCS WAL | Archive standby bootstrap and runtime follow. | Continuous recovery across WAL arrival gaps; no accidental promotion when the archive is temporarily exhausted. |
| Credentials and user management | Access controller and credential distribution. | Correct SQL privileges, one topology-wide credential version, interrupted-rotation recovery, and no secret output. |
| Custom TLS | Runtime file delivery and certificate validation. | Trusted connections, expiry and hostname rejection, reload, CA overlap, and failed-update preservation. |
| Multi-region topology | Global controllers and runtime contract. | Both replication modes, fencing, timeline handling, rejoin, and failure behavior under independent regional outages. |
| Non-cloud extension | Capability-specific interfaces. | A Kubernetes/CSI/object-store deployment without AWS code in the core workflow, with declared fencing limitations. |

## Validation layers

### State-machine tests

Test operation planning and safety predicates with deterministic inputs and injected side effects. Generate event sequences including controller crashes, duplicate messages, stale epochs, partially completed mutations, and changing observations. Verify the invariants after every step.

High-value properties include no grant for two writer regions, no competing local promotion algorithms, no execution under an obsolete authority, no upgrade while ordinary HA can promote an old instance, and no release of a write hold while the durability revision is unresolved.

Model the acknowledgement-set transition before implementing automatic synchronous promotion. If the model cannot prove preservation of acknowledged history for a transition, the corresponding operation must remain blocked or be removed from the supported profile.

### Kubernetes integration

Use envtest for structural admission, defaulting, field ownership, operation immutability, authorization checks, and idempotent reconciliation. Use real Kubernetes test clusters for scheduling, pod lifecycle, network behavior, CSI, and admission outage behavior; envtest does not emulate those components.

Use multiple independent kind clusters for ordinary integration and controlled cross-cluster partitions. Do not use those results to claim EBS behavior or AWS node fencing. EKS test environments cover the provider-dependent paths.

### PostgreSQL correctness

Run real PostgreSQL with the selected runtime and backup engine. An external client records transaction IDs only after commit success, and keeps its ledger outside the injected failure domain. After recovery, check both the database contents and which transactions the client observed as acknowledged.

Separate acknowledged success, known failure, and unknown client outcomes. A disconnected client does not prove that a transaction rolled back. For asynchronous failover, compare actual losses with the declared acceptance and observations; do not manufacture a strict bound from stale lag samples.

### Recovery and compatibility

Certify specific PostgreSQL versions, runtime versions, extensions, Kubernetes versions, CSI versions, filesystems, backup-engine versions, and identity mechanisms. Pin runtime and upgrade images by digest. Initial minor and major version combinations are selected in M0 and exercised in CI; examples using 17 and 18 are not a compatibility certification.

Verify standalone and multi-region restores, old-backup compatibility after operator upgrades, and CRD/control-state migration. Back up and recover management metadata as well as PostgreSQL data. An operator rollback is allowed only within a declared control-state compatibility window.

## Required failure scenarios

| Scenario | Required result |
| --- | --- |
| Kill the primary process during writes. | Safe local takeover or explicit block; observed loss follows the active policy. |
| Suspend the old writer node beyond its authority deadline, then resume it. | No period where the resumed process and a replacement both accept writes. |
| Remove the old Pod object while its node is partitioned. | Object deletion does not authorize a new writer without adequate fencing. |
| Lose the regional Kubernetes API but retain client connectivity to PostgreSQL. | Runtime follows the certified authority-loss protocol; absence of API access does not prove the writer is dead. |
| Lose management API access. | Stable replication and data service continue where their policy permits; new global and gated sync transitions wait. |
| Partition both regions from each other while both remain reachable to applications. | Only the authorized writer region can write; no independent timeout-based remote promotion. |
| Lose the only required remote sync receiver. | Policy-compliant commits block; status identifies the missing acknowledgement. |
| Place remote durable WAL ahead of every local promotion candidate. | Candidate catches up from verified history or remains unpromoted. |
| Crash at each durability-membership transition boundary. | Writes remain held until the journal, runtime configuration, and member history are reconciled. |
| Return an old region with obsolete assignment and runtime state. | It is quarantined, then rejoined safely; it cannot revive its old writer role. |
| Fence API accepts a request but outcome cannot be observed. | Promotion remains `AwaitingFencing`; API acceptance is not success. |
| Lose the response after promotion. | Controller discovers the result and resumes the same operation. |
| Remove WAL needed by a lagging standby. | Archive recovery or explicit rebuild; no false healthy state. |
| Deny archive uploads until WAL storage approaches full. | Clear alerts and policy action; no false archive acknowledgement. |
| Supply a corrupt backup or an incomplete PITR chain. | Restore fails or blocks at the missing evidence; no early promotion. |
| Interrupt PVC expansion and restart the operator. | Expansion resumes; no gratuitous pod replacement. |
| Rotate a certificate with the wrong hostname or key. | Validation rejects activation and preserves usable configuration. |
| Interrupt password publication between database and Secret updates. | Reconciliation resumes the same credential version and reports the mismatch. |
| Crash an upgrade before and after its irreversible boundary. | No unsafe restart of old data; documented recovery succeeds. |
| Fail a campaign canary. | Later targets remain unchanged and the campaign pauses. |
| Restore an old management backup while a newer authority exists. | Recovered controller cannot issue effective stale commands. |

For every destructive workflow, inject faults before the mutation, after the mutation but before its acknowledgement, and after acknowledgement but before the next step. These positions exercise different recovery requirements.

## Performance and capacity experiments

Measure regional controllers and global controllers separately. Run increasing numbers of topologies, namespaces, and registered clusters until cache memory, API throttling, or queue latency establishes a limit. Use these measurements to decide when to split a combined deployment or shard by tenant/namespace.

Measure synchronous commit latency at representative WAN latency, jitter, packet loss, and WAL rates. Record backlog recovery time and the cost of required-receiver replacement. Performance targets must include the chosen durable-copy policy.

Upgrade benchmarks report database size, relation count, extensions, filesystem, image pull time, transfer method, outage duration, and time until required replicas and backups are restored. The word fast is substantiated by these measurements rather than a universal downtime promise.

Restore benchmarks include object-store retrieval, decryption, replay, replica bootstrap, and endpoint/client recovery. Volume expansion benchmarks include storage-provider acceptance, block-device growth, and filesystem growth without a database restart.

## Release and operational artifacts

Each release provides:

- CRDs, Helm chart, namespace-scoped RBAC profiles, and explicit controller deployment modes.
- Runtime and upgrade images with signed provenance, a software bill of materials, and compatibility metadata.
- Provider prerequisites and capability matrices, including unsupported online expansion or fencing profiles.
- Runbooks for regional disaster promotion, failed fencing, interrupted upgrades, archive failure, certificate rotation, metadata recovery, and safe finalizer recovery.
- Dashboards and alerts aligned with API conditions and operation states.
- Upgrade and rollback procedures for the operator, CRDs, runtime, and internal protocol state.

New controller versions first run compatibility checks against existing regional control state. Mixed versions exchange an explicit protocol version/capability set. Unsupported combinations hold mutations while retaining observation. Global rollout order and regional rollout order are specified per release rather than assumed interchangeable.

## Decisions to resolve

| Decision | Current position | Must resolve before |
| --- | --- | --- |
| PostgreSQL HA runtime | Contract fixed; native Go manager and Patroni integration remain candidates. | M0 completion and production failover implementation. |
| Project API group | Reserved example domain in drafts. | Publishing initial CRDs. |
| First PostgreSQL/extension matrix | Certify a small explicit matrix. | Building release runtime and upgrade images. |
| Supported AWS fencing profile | Dedicated database compute is the first prototype; shared-node blast radius is not assumed acceptable. | Enabling automatic local failover and requested disaster promotion on AWS. |
| Credential source across regions | External secret distribution preferred; global controllers handle references only. | Multi-region managed credential rotation. |
| Management cluster placement | Dedicated management cluster or combined installation both supported by design. | Production deployment and recovery planning. |
| Numeric scale, RTO, RPO, and latency targets | Define with workload and failure assumptions; no fixed SLA inferred. | Capacity and acceptance benchmarks. |
| Optimized standby major upgrade | Initial rebuild path; optimize only after certification. | Advertising faster restoration of full protection. |

These decisions do not reopen the selected one-operator architecture. They define the implementation and operating envelope within it.
