# PostgresManager

A proposed Kubernetes operator for PostgreSQL with regional and global controllers in one codebase and release. Each region runs in a separate Kubernetes cluster. A database topology has one writable primary and supports asynchronous or synchronous replication to other regions.

AWS is the first deployment target. Storage, backup, identity, endpoint, and fencing integrations must leave room for GCP, Azure, and on-premises Kubernetes.

The repository currently contains the detailed design. The operator and CRDs are not implemented.

| Document | Purpose |
| --- | --- |
| [System design](docs/design.md) | Architecture, ownership, safety rules, replication, operational workflows, security, and provider boundaries. |
| [API design](docs/api.md) | Proposed resources, field semantics, admission rules, status, and illustrative manifests. |
| [Runtime decision](docs/runtime.md) | Required PostgreSQL runtime behavior and the decision between a native instance manager and an existing HA runtime. |
| [Delivery and validation](docs/delivery-plan.md) | Implementation milestones, failure tests, release gates, and unresolved decisions. |

The API group, images, credentials, cluster names, and infrastructure references in examples are placeholders. The examples describe the proposed API and cannot yet be applied to Kubernetes.
