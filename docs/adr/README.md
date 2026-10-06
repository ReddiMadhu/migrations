# Architecture decision records

| ADR | Decision | Status |
|---|---|---|
| [ADR-001](ADR-001-cordis-runtime.md) | Use upstream Cordis as runtime foundation | Accepted |
| [ADR-002](ADR-002-event-sourced-session.md) | Durable event log is authoritative | Accepted |
| [ADR-003](ADR-003-scope-model.md) | Tenant→migration→attempt→candidate→execution scopes | Accepted |
| [ADR-004](ADR-004-tool-middleware.md) | Guarded, canonical tool execution pipeline | Accepted |
| [ADR-005](ADR-005-reconciliation-independence.md) | Reconciliation cannot depend on translation/repair | Accepted |
| [ADR-006](ADR-006-databricks-execution.md) | Execute Spark in Lakeflow Jobs, not the App process | Accepted |
| [ADR-007](ADR-007-artifact-identity.md) | Immutable content-addressed artifacts | Accepted |
| [ADR-008](ADR-008-semantic-hashing.md) | Separate exact and semantic hashes | Accepted |
| [ADR-009](ADR-009-repair-branching.md) | Isolated candidate branches with deterministic selection | Accepted |
| [ADR-010](ADR-010-approval-binding.md) | Hash/policy/environment-bound approval | Accepted |
| [ADR-011](ADR-011-sas-reference-execution.md) | Reference CSV before live SAS providers | Accepted |
| [ADR-012](ADR-012-evidence-integrity.md) | Per-event hash chain and sealed evidence root | Accepted |
| [ADR-013](ADR-013-model-replaceability.md) | Provider-neutral advisory model service | Accepted |
| [ADR-014](ADR-014-ptc-boundary.md) | Code mode only for bounded diagnostics | Accepted |
| [ADR-015](ADR-015-multi-worker-topology.md) | Cordis workers over PostgreSQL/object storage | Accepted |
| [ADR-016](ADR-016-reconciliation-policy.md) | Immutable reconciliation policy and deterministic canonical evidence | Accepted |

The source assumptions and target-commit corrections behind these decisions are recorded in the [DSH target audit](../dsh-target-audit.md) and [verification record](../deepseek-harness-verification.md).
