# Databricks execution design

**Verified:** 2026-09-24 against current Databricks documentation.

## Decision

The control plane uses Lakeflow Jobs APIs. It never executes generated PySpark in the Cordis/FastAPI/Node process.

```text
artifact/control plane
→ submit or run-now
→ durable run_id mapping
→ poll GET /api/2.2/jobs/runs/get
→ retrieve output manifest
→ independently verify artifact digest
→ execution/completed event
```

## Current API facts

- One-time submission is `POST /api/2.2/jobs/runs/submit` and is GA.
- One-time runs are not shown in the Jobs UI and do not automatically retry.
- `idempotency_token` is limited to 64 characters and guarantees at most/exactly one run for that token according to the API contract.
- Saved production jobs are preferable for visibility, governance, retry policy, and stable task definitions.
- Run monitoring is `GET /api/2.2/jobs/runs/get`.
- The old `state` object is deprecated; read `status`.
- `status.life_cycle_state` and `status.result_state` are open-ended API vocabularies. Unknown values must become `UNKNOWN` internally and trigger re-check, not optimistic success.
- `SUCCESS_WITH_FAILURES` is not clean `SUCCESS`.
- Cancellation is asynchronous: after `POST /jobs/runs/cancel`, the run may still be running.
- `spark_submit_task` is deprecated; use an appropriate modern task (for example `spark_python_task` for a controlled Python file) or a reviewed wheel/notebook task.

## Provider contract

```ts
interface SparkExecutor {
  submit(request, signal): Promise<{ runId, submissionIdempotencyKey }>
  status(runId, signal): Promise<ExternalRunStatus>
  output(runId, signal): Promise<ArtifactReference[]>
  cancel(runId, signal): Promise<{ accepted: boolean }>
}
```

`ExternalRunStatus` normalizes provider states into:

```text
SUBMITTING | QUEUED | RUNNING | TERMINATING
SUCCEEDED | FAILED | CANCELED | TIMED_OUT
PARTIAL_SUCCESS | UNKNOWN
```

The adapter retains raw provider state/result/message as evidence.

## Crash and ambiguity

Before the remote call, durably append `execution/started` with:

- migration/candidate/attempt IDs
- artifact digest
- input snapshot digest
- environment fingerprint
- policy fingerprint
- provider and job identity
- deterministic idempotency key

Then `ctx.parallel('evidence/flush', ...)` is a hard barrier.

If the process dies before the response is durable, the started event remains. Recovery enters `UNKNOWN`/`HUMAN_REQUIRED` and queries the remote run using the saved idempotency/run mapping. It never blindly submits again.

If Spark completed but its output manifest was not persisted, the outcome also remains `UNKNOWN` until the provider or object store proves the expected output exists.

## Artifacts and paths

Per migration/candidate:

```text
MIG-.../candidates/CAND-.../execution/INPUT
MIG-.../candidates/CAND-.../execution/CODE
MIG-.../candidates/CAND-.../execution/OUTPUT
MIG-.../candidates/CAND-.../execution/LOGS
MIG-.../candidates/CAND-.../execution/MANIFEST
```

The Spark job receives artifact locations and digests as parameters. It writes output plus a manifest. The control plane verifies the manifest and stores the output as an immutable artifact reference.

## Databricks App boundary

Current documentation recommends:

- asynchronous request patterns,
- Databricks-native compute for batch/data processing,
- dedicated per-app service principals,
- least privilege,
- outbound-network restrictions,
- graceful shutdown within 15 seconds of `SIGTERM`.

Therefore the App is a control/UI plane. It stores only metadata and small evidence records locally; large datasets and generated program execution stay in Jobs and governed storage.

## Production submission modes

### Saved Lakeflow Job (production default)

Use `jobs/run-now` with a controlled Job definition, UC policy, job parameters, and explicit permissions. A normalized run ID is durably mapped to the migration execution identity.

### One-time run (development/controlled dynamic execution)

Use `runs/submit` with a deterministic idempotency token. The run is not visible in the Jobs UI and has no automatic failure retry, so the control plane must own monitoring, evidence capture, and operator visibility.

## Required permissions

Grant the app/service principal only the needed catalog/schema/volume/job permissions. Do not assume a deployment profile grants all volume access; verify the actual UC grants. Use app authorization for consistent background identity, and user authorization only when row/column-level user policy is required.

## Primary sources

- [Create and trigger a one-time run](https://docs.databricks.com/api/jobs/v2/submit-run)
- [Get a single run](https://docs.databricks.com/api/jobs/v2/get-run)
- [Cancel a run](https://docs.databricks.com/api/jobs/v2/cancel-run)
- [Databricks Apps best practices](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/best-practices)
- [Databricks Apps authorization](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/auth)
- [Databricks Apps environment](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/system-env)
- [Unity Catalog volumes](https://docs.databricks.com/aws/en/files/volumes/)
