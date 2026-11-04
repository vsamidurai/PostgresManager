# PostgreSQL Runtime Decision

Status: Open decision with required behavior defined

The operator owns regional and global Kubernetes reconciliation. It still needs a process inside each database pod to supervise PostgreSQL, apply configuration, expose health, and execute a safe local HA protocol. That process is a runtime component, not a second Kubernetes operator.

The first release will certify one runtime. The contract below prevents controller code from depending on incidental command details; it does not imply support for arbitrary interchangeable HA plugins.

## Candidate implementations

| Candidate | Benefits | Engineering responsibility and risk |
| --- | --- | --- |
| Native Go instance manager | One language and release, direct integration with operation identities and global durability policy, full control over process ownership. | We must implement and validate process supervision, local authority loss, promotion, fencing integration, recovery, and membership transitions. |
| Patroni with a narrow integration layer | Reuses an established local HA implementation, PostgreSQL supervision, and local replication management. | Its local authority and synchronous membership rules must compose with global policy without a second competing election or configuration writer. |

pg_auto_failover and repmgr remain reference alternatives but are not additional first-release implementation tracks. Their different monitor or command orchestration models would require separate integration and validation work. See [pg_auto_failover architecture](https://pg-auto-failover.readthedocs.io/en/main/architecture.html) and [repmgr failover](https://www.repmgr.org/docs/current/repmgrd-automatic-failover.html).

Patroni's synchronous mode records membership in its distributed configuration store and constrains promotion within that protocol. Independent standby clusters do not automatically become part of the same election or durability history. We must demonstrate how remote acknowledgements and regional promotions compose, rather than manually overwriting settings that Patroni also manages. See [Patroni synchronous mode](https://patroni.readthedocs.io/en/latest/replication_modes.html) and [standby clusters](https://patroni.readthedocs.io/en/latest/standby_cluster.html).

## Runtime contract

| Capability | Required behavior |
| --- | --- |
| Observe | Report instance UID, database system identifier, timeline history, process state, role, WAL positions, config revision, authority term, and current maintenance holds. |
| Bootstrap | Initialize or restore only an empty, assigned data directory; reject mismatched volume and instance identity. |
| Configure | Validate and atomically install configuration; distinguish reload, restart, and unsupported changes. |
| Follow | Join a verified upstream or archive while remaining in recovery. |
| Hold writes | Enforce a maintenance state that covers existing sessions and durable restart inhibition; expose what has actually stopped. |
| Stop | Terminate PostgreSQL and report verified process exit, directory state, and recoverable WAL boundary. |
| Promote | Accept only the current authorized operation, expected term, verified durability evidence, and fencing receipt; persist the resulting transition. |
| Recover | Rewind or rebuild from a verified history with explicit destructive boundaries. |
| Upgrade | Run a certified upgrade task with old/new binaries, durable step markers, output artifacts, and safe retry rules. |
| Secure management | Authenticate callers, constrain actions, reject stale/replayed commands, and prevent arbitrary remote execution. |

Responses distinguish `Accepted`, `InProgress`, `Succeeded`, `Rejected`, and `OutcomeUnknown`. Every action takes a topology/cluster identity, operation UID, step ID, expected term, and preconditions. After a lost response the runtime exposes enough state to discover whether the action occurred.

Only the runtime starts PostgreSQL. A pod recreation checks current regional control state before starting writable PostgreSQL; a volume marker or old environment variable is not current authority. The runtime supervises the server for the full pod lifetime and handles termination signals, logs, and backup-engine integration.

## Native instance manager requirements

If selected, the native manager runs as the parent process of PostgreSQL. Regional controllers coordinate candidate selection; the runtime enforces process authority and performs the actual transition. A controller lease is not itself a database write lease.

The local protocol must define renewal deadlines, monotonic timing, restart behavior, process pause behavior, network partitions, and node reboot. Expiry causes the runtime to stop writable PostgreSQL, but safe promotion still requires proof that a stalled previous writer cannot resume. The proof may be confirmed shutdown with durable inhibition or an effective infrastructure fence.

The first implementation favors a conservative, serialized promotion workflow. Automatic takeover is disabled for installation profiles that cannot meet its fencing conditions. It does not claim safety merely because two Kubernetes processes believe they hold different lease terms.

The runtime keeps a local journal on persistent storage for step recovery, with checksummed records and durable writes at destructive boundaries. The regional API control record remains the authorization source. Conflicts between the local journal and API state cause a hold and investigation rather than choosing whichever has a larger integer.

## Patroni integration requirements

If selected, Patroni owns its local election protocol and its distributed configuration scope. Our regional controllers request supported operations and manage Kubernetes resources; they do not independently promote a standby using SQL or `pg_ctl`.

Global maintenance requires a verified Patroni state that inhibits conflicting promotions and restarts. Its ordinary pause behavior must be tested against this requirement, including pod recreation and node recovery. A setting named pause is not accepted as proof of fencing.

External synchronous acknowledgement support is a release gate. Any integration must specify the exclusive owner of `synchronous_standby_names`, the durability membership journal, and candidate eligibility. If meeting the contract requires unsupported mutation of Patroni internals or competing configuration ownership, the candidate is rejected for the required global synchronous profile.

## Decision experiment

Use the same two- or three-Kubernetes-cluster test harness and client commit ledger for each viable candidate. The ledger records transaction IDs acknowledged to clients outside the database failure domain. Run:

1. Primary process failure with a healthy local standby.
2. A node pause that outlives the local authority deadline, followed by resume.
3. Regional API loss while PostgreSQL and applications remain reachable.
4. A remote required synchronous receiver failure and replacement.
5. Local primary loss while a remote durable copy is ahead of the proposed local candidate.
6. A crash during every acknowledgement membership transition step.
7. A WAN partition followed by attempted regional promotion and return of the old region.
8. Runtime and operator restart during maintenance, rewind, and upgrade.

Reject a candidate that permits two writers, drops a policy-covered acknowledged transaction, silently weakens durability, or cannot distinguish uncertain command outcomes. Then compare implementation size, operational dependencies, recovery clarity, upgrade compatibility, and measured control-plane cost.

Select the runtime before implementing production automatic failover. API scaffolding, providers, observation, backups, and manually exercised prototype workflows can proceed while this decision is open. The native instance manager is a credible design option, but its safety properties must be established by the protocol review and tests rather than inferred from sharing the operator's codebase.
