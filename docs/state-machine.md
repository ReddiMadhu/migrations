# Deterministic state machines

## Migration state

```text
CREATED
→ ANALYZING
→ TRANSLATING
→ CANDIDATE_READY
→ EXECUTING
→ RECONCILING
  ├→ CERTIFIED
  └→ DIAGNOSING
       → REPAIRING
       → CANDIDATE_READY
```

Terminal/escalation outcomes:

```text
UNSUPPORTED
POLICY_BLOCKED
APPROVAL_REQUIRED
OSCILLATION
BUDGET_EXCEEDED
HUMAN_REQUIRED
FAILED
```

Only the coordinator transitions this state. A provider/model message can request a transition, but cannot assert one.

## Repair state

```text
READY
→ DIAGNOSING
→ PROPOSING
→ VALIDATING
→ EXECUTING
→ RECONCILING
  ├→ PASSED
  └→ FAILED → READY | ESCALATED
```

`VALIDATING` includes static analysis, policy, secret scanning, patch identity, monotonic guards, stale-approval checks, and expected-effect preconditions.

## Execution state

```text
NOT_STARTED
→ STARTED
→ {SUCCEEDED | FAILED | CANCELED | TIMED_OUT | PARTIAL_SUCCESS | UNKNOWN}
```

An open `STARTED` tail after process recovery is projected as `HUMAN_REQUIRED` with an `execution/unknown` event until external state is checked. `UNKNOWN` is not a retryable failure.

## Reconciliation state

```text
NOT_STARTED
→ STARTED
→ {COMPLETED | FAILED}
```

`COMPLETED` becomes `CERTIFIED` only when schema, aggregate, row, and statistical verdicts satisfy the pinned policy. A mismatch becomes `DIAGNOSING`; `reconciliation/failed` becomes `HUMAN_REQUIRED`.

## Work-unit lifecycle

```text
OPEN
→ ACTIVE
→ REPAIRING
→ ACTIVE | BLOCKED
→ CERTIFIED
→ CLOSED
```

A work unit is created from a bounded SAS module/dependency closure and, when available, a bounded structural IR and static macro-resolution snapshot. Its closure, IR/macro snapshots, and IR node list cannot widen. `ctx.modelContext.assemble()` derives a bounded prompt context from that immutable evidence. `BLOCKED` records reconciliation evidence that requires diagnosis or operator attention. Only the coordinator/runtime may advance the status; a model can propose a candidate but cannot mark a unit certified.

## Failure taxonomy

The workbench uses closed, typed codes:

```text
TRANSLATION_FAILED
UNSUPPORTED_CONSTRUCT
STATIC_VALIDATION_FAILED
POLICY_DENIED
APPROVAL_REQUIRED
EXECUTION_FAILED
EXECUTION_UNKNOWN
SCHEMA_MISMATCH
AGGREGATE_MISMATCH
ROW_MISMATCH
DIAGNOSIS_UNCERTAIN
REPAIR_REJECTED
REPAIR_OSCILLATION
REPAIR_BUDGET_EXCEEDED
CERTIFICATION_BLOCKED
```

`ERROR` may be a UI grouping label, never the only machine state.

## Guard rules

Before another candidate, the repair engine rejects:

- the same byte patch
- the same semantic patch
- the same hypothesis after a falsified result
- the same reconciliation signature
- an `A → B → A` semantic transition
- a candidate that does not improve its declared discrepancy
- a candidate that fixes a metric but creates a prohibited regression

A guard denial records `{guardId, reasonCode, policyVersion, evidenceSequence}`.
