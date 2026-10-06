# Milestones, testing, benchmarks, and operations

## Delivery sequence

1. Cordis foundation, service contracts, effects, typed events
2. Durable event log, projections, scope/fork, bounded work units, deterministic SAS include discovery, bounded structural IR, artifacts, dummy execution, replay
3. Independent deterministic dataset reconciliation and difference reports
4. Databricks provider using real APIs and a known PySpark program
5. SAS parser/semantic IR, sample translator, schema/aggregate/row reconciliation
6. Difference localization and diagnostic experiments
7. Guarded LLM repair, oscillation detection, expected-effect validation
8. Three isolated candidate branches and deterministic selection
9. Human approval, certificate, audit replay, batch dashboard

## Test strategy

### Runtime

- upstream Cordis service registration and dependency activation
- child context isolation and provider replacement
- all five dispatch semantics
- plugin unload disposes listeners/services/timers
- optional capabilities resolve with `ctx.get(name, false)`

### Session/evidence

- append/replay/project
- JSON serialization rejection before mutation
- contiguous sequence validation
- hash-chain tamper detection
- fork only from a safe prefix
- parent lineage and independent branch replay
- bounded work-unit create/update/close, durable replay, and scope revocation
- deterministic static SAS include discovery, graph replay, cycle/unresolved rejection, and graph-derived work-unit closure
- bounded SAS structural IR, source-span validation, durable replay, and IR-linked work-unit node selection
- ambiguous execution tail becomes `UNKNOWN` and requires external verification

### Artifacts

- SHA-256 exact-byte identity
- same digest deduplication
- scope path containment
- overwrite rejection
- metadata and parent lineage

### Tools/execution

- frozen arguments
- deny-only guards
- stale approval rejection
- canonical result after middleware
- policy/provider split
- deterministic dummy provider

### Reconciliation

- JSON and CSV dataset extraction
- exact schema parity and type profiles
- deterministic null/non-null/type/decimal-sum aggregates
- ordered and unordered row parity
- policy-aware deterministic statistical profile
- report artifact creation and durable completed/failed events
- non-certifiable mismatch leaves `DIAGNOSING` or `HUMAN_REQUIRED`

### Migration/regression (later)

- SAS semantic fixtures
- translation/IR spans
- SAS-aware schema/aggregate/row reconciliation
- mutation suite: `>↔>=`, `AND↔OR`, join kind, nulls, partition, order, aggregation, date/missing semantics, duplicates
- false-negative and false-positive parity metrics
- repair success, oscillation, and cost

## Benchmark strategy

Each benchmark case contains:

```text
program.sas
input snapshot
reference output
expected semantic IR
expected PySpark behavior
known mutations
policy/environment profile
```

The benchmark runner independently executes or replays the reference and candidate paths, records all evidence, and reports:

- first-pass parity
- eventual parity
- false-parity rate
- repair success
- mean repair attempts
- human escalation
- latency and provider-reported cost

Mutation tests are mandatory. A translator that looks correct but fails reconciliation is not a success.

## Deployment topology

```text
API/UI → queue → orchestration workers (Cordis contexts)
                    ├── PostgreSQL event/queue store
                    ├── object/UC Volume artifacts
                    └── external SAS/Spark/diagnostic execution worlds
```

Each worker is horizontally scalable. PostgreSQL advisory/row locks and unique external-run mappings protect cross-worker transitions. A local Cordis Context composes capabilities for one worker task and is disposed afterward.

## Operational runbook

### Worker start

1. Verify schema migration and PostgreSQL connectivity.
2. Verify artifact-store root/prefix and encryption configuration.
3. Load the pinned Cordis composition; fail if required safety services are absent.
4. Acquire/renew a migration lease.
5. Resume from the last committed event; inspect open operations before any retry.

### External submission

1. Append and flush `execution/started` with a deterministic idempotency key.
2. Submit or run-now the pinned provider job.
3. Persist the remote run ID.
4. Poll `status`; map unknown states to `UNKNOWN`.
5. Verify output manifest and artifact digest.
6. Append and flush `execution/completed`.

### Incident: worker died after submit

1. Locate the open started event.
2. Resolve the external run by saved idempotency token/run ID.
3. Poll and verify output.
4. Append the observed terminal or unknown result.
5. Never blindly submit a replacement.

### Incident: certificate verification fails

1. Freeze certification and dependent approvals.
2. Recompute the event chain from PostgreSQL.
3. Verify every referenced artifact digest and retention state.
4. Compare certificate policy/environment/input identities.
5. Escalate to an operator; never issue a replacement certificate over mutated history.

## Current validation snapshot

The current proof path is validated with:

```text
pnpm run verify   # strict typecheck + 49 tests + build
pnpm run demo     # 4 durable events, 1 output, replay succeeds
```

Milestone 2 additionally validates six reconciliation tests: exact JSON certification, ordered/unordered row parity, decimal-sum aggregate drift, unsupported-format failure, CSV parsing, and policy fingerprints.

The demo's local filesystem and JSONL providers are test adapters. The DSH target audit is recorded separately; it does not imply that DSH's local registries, PTC budgets, approval grants, or JSONL leases are production authorities.

## Current known limitations

- Milestones 1–2 use a local filesystem artifact provider and durable JSONL session store to prove contracts; production PostgreSQL/object-store providers are required before deployment.
- The dummy executor is deterministic and in-process only as a provider double. It does not represent Spark execution security.
- Deterministic reconciliation currently supports JSON/CSV evidence and dataset-level parity. It is not yet SAS/PySpark semantic reconciliation and does not prove query-plan equivalence.
- `MigrationWorkUnit` provides a durable bounded context and logical Databricks reference; static SAS `%include` discovery, bounded structural IR/source spans, static macro-resolution evidence, and deterministic model-context assembly are implemented, while macro expansion, autocall resolution, and complete data-flow semantics are not implemented yet.
- No full SAS parser, complete semantic IR, translation, repair, certificate implementation, API, or React UI is complete yet.
- The codebase has no Databricks credentials or live workspace, so the real provider remains a designed next milestone rather than a claimed integration.
- Cordis 4 is an RC; the exact version and composition are pinned and upgrade-tested.
- Hash chaining detects accidental/logical mutation but is not a signature. A production certificate needs KMS/HSM-backed sealing or an equivalent trust anchor.
