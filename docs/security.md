# Security threat model

## Trust boundaries

1. **Control plane** — Cordis workers, API, scheduler, reconciler, and policy engine. Trusted to enforce policy, not to run untrusted code.
2. **Execution plane** — Databricks/Spark, future SAS runtime, and diagnostic runtime. Each has a real isolated substrate and least-privilege identity.
3. **Data plane** — PostgreSQL and object/UC Volume storage. Access is tenant-, migration-, candidate-, and classification-scoped.

## Principal threats and controls

| Threat | Control |
|---|---|
| Model writes or executes arbitrary host code | no host execution tool; generated code runs only in external Spark/SAS providers |
| Path traversal / artifact escape | scope-derived canonical paths; no caller-provided absolute path; production provider maps to tenant/candidate Volume prefix |
| Scope privilege widening | parent-chain validation; child policy may narrow only; approval pinned to snapshot |
| Secret leakage through model context | credential reference service; value-free model projection; SAS/PySpark secret scanner before event/model exposure |
| Secret leakage through execution environment | provider resolves references immediately before an authorized call; never copies all ambient environment |
| Stale approval | runtime compares semantic/source digest, policy, input snapshot, action, and environment fingerprint |
| False parity from translator gaming reconciler | independent reconciliation service/provider and artifact inputs; no reverse dependency |
| False parity from normalization drift | pinned policy fingerprint, decimal-string aggregates, ordered/unordered row parity, bounded deterministic report |
| False parity from partial Spark success | `SUCCESS_WITH_FAILURES` maps to failure; unknown remote states remain unknown |
| Duplicate remote run after crash | durable started event + deterministic idempotency token + external run verification |
| Candidate cross-contamination | isolated workspace/output path/provider identity and independent session fork |
| Event tampering | canonical payload snapshot, per-event SHA-256 hash chain, append-only durable table, certificate root |
| Evidence deletion through compaction | compaction only changes model surface; evidence events/objects retain retention policy |
| Denial due to missing policy provider | fail closed with `POLICY_DENIED` / `EXECUTION_UNKNOWN`; never default to unrestricted |
| Unbounded diagnostics | read-only/idempotent, strict resource, network, time, and output quotas in the diagnostic execution world |
| Repair oscillation/cost exhaustion | semantic patch/hypothesis/signature monotonic guards and explicit repair budget |
| API/UI information leak | operator/model projections differ; model receives safe denial codes, operator receives trace |
| Databricks app compromise | dedicated app identity, minimum UC/job grants, outbound restrictions, no app-side batch data processing |

## Credential rules

Models and durable events may contain `credential_ref` names such as `SAS_PROD_DB`, never values. Execution providers resolve a reference only after authorization and inject only the named secret. Source scanning must cover connection strings, passwords, tokens, SAS `LIBNAME`/ODBC DSNs, embedded URLs, and common cloud key patterns.

## Approval binding

An approval is valid only while all of these match:

```text
artifact semantic hash
artifact source hash
policy version/hash
environment fingerprint
input snapshot digest
action type
tenant/migration/candidate
```

Matching is lazy at the execution boundary. UI staleness is advisory.

## Audit and retention

- Hot retention: full event envelopes and policy/approval/tool metadata
- Object retention: source, IR, candidates, outputs, reports, patches, diagnostics, certificates
- Archive: sealed certificate and Merkle/hash-chain proofs
- Compaction: reasoning only
- Deletion: governed legal/retention workflow that cannot delete evidence referenced by an active certificate

## DSH-derived security qualifications

The target DSH audit does not justify weakening these controls:

- DSH's credential provider is reference-oriented and does not automatically inject values, but arbitrary file/SDK/subprocess paths can bypass the provider seam. The workbench still scans and constrains those paths.
- DSH Node PTC uses a fresh managed process and resource budgets; Python PTC has bounded process controls but no shipped filesystem confinement. Neither is evidence of a universal SAS/Spark network, filesystem, or resource boundary.
- DSH local registries, JSONL persistence, and filesystem leases are process/host mechanisms, not distributed tenant authorization or consensus.
- DSH's shipped OTel default is `FEEDBACK_ONLY`, but lack of built-in content redaction means the workbench must still classify, sample, and redact telemetry before export.
- DSH approval grants are not automatically bound to artifact/input hashes, policy version, or environment fingerprint; the workbench's stricter binding is an intentional extension.

## Non-goals

This foundation does not claim a kernel-grade sandbox, complete malware defense, or a production tenant authorization service. Those controls are deployment prerequisites and must be integration-tested before certification.
