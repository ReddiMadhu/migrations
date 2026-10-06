# Databricks Deployment-Target Verification

**Verification date:** 2026-09-23
**Subject:** the pasted document *"End-to-End Architecture Verification: FastAPI Databricks App → Databricks Job → PySpark Execution"*
**Method:** every verbatim claim re-fetched against the cited primary source. Verbatim quotes are reproduced from the live pages; the rest is labeled.
**Pages fetched and confirmed live (HTTP 200):** `system-env`, `best-practices`, `uc-volumes`, `lakeflow`, `auth`, `compute-size`, `app-development`, `api/jobs/v2/submit-run`.
**Could not verify:** `resources/limits` (page returned "unable to convert safely"); `web_search` endpoint returned HTTP 401 (search API credential/endpoint misconfiguration — a user-side setting, not retried).

---

## Verdict

**VERIFIED WITH CONDITIONS — same verdict as the source document, but the condition list must be amended.** The load-bearing verbatim claims are genuine. However the document (a) incompletely enumerates one closed vocabulary that a state machine depends on, (b) cites one deprecated field as the monitoring basis, (c) overstates one security conclusion, and (d) drops three preconditions/consequences that change deployment behavior. Details below.

---

## 1. Claims that verify VERBATIM

| # | Claim | Verbatim quote from live page | Status |
|---|---|---|---|
| 1 | App runtime is Ubuntu 22.04 / Python 3.11 | "Operating system: Ubuntu 22.04 LTS" / "Python environment: Python 3.11, running in a dedicated virtual environment" (`system-env`) | ✅ exact |
| 2 | Default 2 vCPU / 6 GB | "By default, each app can use up to 2 virtual CPUs (vCPUs) and 6 GB of memory" (`system-env`) | ✅ exact |
| 3 | FastAPI + Uvicorn auto-configured | `FastAPI: UVICORN_PORT, UVICORN_HOST=0.0.0.0` / `Uvicorn: UVICORN_PORT, UVICORN_HOST=0.0.0.0` (`system-env`, Framework environment variables) | ✅ exact |
| 4 | Compute sizes and cost | Medium "Up to 2 vCPUs / 6 GB / 0.5 DBU"; Large "4 / 12 GB / 1 DBU"; XLarge "12 / 48 GB / 3 DBU" (`compute-size`) | ✅ exact |
| 5 | Async pattern required | "Use asynchronous request patterns for long-running operations. Avoid synchronous requests that wait for operations to complete, which can time out." (`best-practices`) | ✅ exact |
| 6 | Don't process data in the app | "Access data through appropriate compute. Don't configure your app to access or process data directly." (`best-practices`, *Security*) | ✅ exact |
| 7 | App compute is for UI, not batch | "App compute is optimized for UI rendering. Use Databricks SQL … Lakeflow Jobs for batch processing" (`best-practices`) | ✅ exact |
| 8 | Dedicated, unique service principal | "Each Databricks app has a dedicated service principal … unique to the app instance and can't be reused across apps" (`auth`) | ✅ exact |
| 9 | Credentials auto-injected | `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` ("Service principal OAuth client ID/secret") (`auth`) | ✅ exact |
| 10 | SDK auto-detects | "The SDKs follow unified authentication and automatically detect credentials in the environment" (`auth`) | ✅ exact |
| 11 | UC Volume privilege triad | "`USE CATALOG` … `USE SCHEMA` … and `READ VOLUME` or `WRITE VOLUME` … Databricks automatically grants these privileges" (`uc-volumes`) | ✅ exact |
| 12 | Volume permission levels | "Can read" / "Can read and write" (`uc-volumes`) | ✅ exact |
| 13 | Lakeflow permission ladder | "Can view: … view job details, run history, and status" / "Can manage run: … trigger and cancel job runs" / "Can manage: … full administrative access" (`lakeflow`) | ✅ exact |
| 14 | 15-second shutdown | "Your app must shut down within 15 seconds after it receives a `SIGTERM` signal" (`best-practices`) | ✅ exact — **but see §2.5** |
| 15 | 10 MB app-file limit | "App files can't exceed 10 MB. If any file in the app directory exceeds this limit, deployment fails with an error." (`app-development`) | ✅ exact |
| 16 | pip/uv/npm dependencies | "Define Python dependencies using `pip` or `uv`, and Node.js dependencies using `npm`" (`app-development`) | ✅ exact |
| 17 | `runs/submit` exists and is one-time | "Create and trigger a one-time run — `POST /api/2.2/jobs/runs/submit`" (`api/jobs/v2/submit-run`) | ✅ exact |
| 18 | Run lifecycle vocabulary | `run_id` "unique across all runs of all jobs"; `life_cycle_state` ∈ PENDING, RUNNING, TERMINATING, TERMINATED, SKIPPED, INTERNAL_ERROR, BLOCKED, WAITING_FOR_RETRY, QUEUED | ✅ exact |
| 19 | Both compute paths | `cluster_spec.existing_cluster_id` **and** `new_cluster` both defined (`submit-run`) | ✅ exact |

---

## 2. Corrections — where the pasted document is wrong or incomplete

### 2.1 `result_state` is a closed vocabulary and the document under-enumerates it

The document lists `SUCCESS, FAILED, TIMEDOUT, CANCELED, MAXIMUM_CONCURRENT_RUNS_REACHED`. The live page defines **five more**:

- `UPSTREAM_CANCELED`
- `UPSTREAM_FAILED`
- `EXCLUDED`
- **`SUCCESS_WITH_FAILURES`**
- `DISABLED`

**Why this matters to us specifically:** a parity state machine that maps `result_state === 'SUCCESS'` to "execution succeeded, safe to reconcile" will silently mis-handle `SUCCESS_WITH_FAILURES` — a run that completed with partial failure. That is precisely the class of state that must route to `OUTCOME_UNKNOWN`-style verification, not to reconciliation. **Fix: exhaustively switch on `result_state`; treat anything that is not a clean `SUCCESS` as "verify external state before trusting."**

### 2.2 The `state` field itself is deprecated

The live page labels the `state` object: *"Deprecated. Please use the `status` field instead."* The document's entire monitoring design reads `state.life_cycle_state` / `state.result_state`. The fields are still "always available in the response," so the design works today, but **§8's polling architecture should target `status`** to avoid building a durable dependency on a deprecated shape.

### 2.3 "No `jobs` scope ⇒ jobs must use app authorization" is overstated

The document's §24 item 4 concludes that job operations *must* use app (service principal) authorization because no `jobs` scope exists. It is true that no `jobs` scope is listed. But:

- The supported list is larger than the document's "sql, files, apps" enumeration: `ai-gateway`, `apps`, `files`, `genie`, `model-serving`, `postgres`, `sql`, `vector-search`, `sql:restricted-query`.
- **Critically, it also includes SDK scopes**, one of which is **`workspace.workspace`** — a scope over the Workspace API surface that plausibly covers on-behalf-of job operations.
- If no scopes are selected, the defaults are `iam.access-control:read` + `iam.current-user:read` (identity only, no data/compute).

**So the honest statement is:** there is no dedicated `jobs` scope; on-behalf-of job triggering is not cleanly modeled by the scope list, and **app authorization is the simpler and better-supported path for our control plane** — but "must" is not established by the cited page. The document's conclusion is a good design choice presented as a platform constraint.

### 2.4 `DATABRICKS_HOST` is cited to the wrong table

The document places `DATABRICKS_HOST` in the auth-page credentials table. That table lists **only** `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET`. `DATABRICKS_HOST` does exist — but in `system-env`'s *Default environment variables* table. The claim is true; the citation is wrong. (Also genuinely useful and unmentioned: `DATABRICKS_APP_NAME`, `DATABRICKS_WORKSPACE_ID`, `DATABRICKS_APP_PORT`.)

### 2.5 The 15-second rule has a consequence the document drops

The full sentence is: "Your app must shut down within 15 seconds after it receives a `SIGTERM` signal, **or it forcibly terminates with `SIGKILL`**."

**Design consequence:** the SIGKILL means there is no "slow drain" option. Any durability barrier we hold open during shutdown (the report's §11 / B5 flush-before-side-effect barrier) **must be completable in well under 15 seconds**, or we take a hard kill mid-flush — which is exactly the torn-write case the barrier exists to prevent. This is a real constraint on the control plane's shutdown path, not a footnote.

---

## 3. Preconditions and behaviors the document drops (all verified verbatim)

### 3.1 UC Volume auto-grant is conditional, not unconditional

`uc-volumes` states the triad is granted automatically **only if**, for each privilege:

> "Either all account users have the … privilege on the [object], or you have the `MANAGE` privilege on the [object]."

**Impact:** if the app creator lacks `MANAGE` on the catalog/schema/volume and the privilege isn't account-wide, **the auto-grant fails and the app silently lacks Volume access.** The document presents the auto-grant as automatic. This must be a preflight check (and a runbook item), not an assumption.

### 3.2 Removing a Lakeflow resource does not stop in-flight runs

`lakeflow`: "Any ongoing job runs initiated by the app continue to completion, but the app can no longer trigger new runs or access job details."

**Impact:** revoking the app's job resource mid-migration does **not** abort the Spark execution. Combined with the report's §35.3 `OUTCOME_UNKNOWN` semantics, this means a permission change is not a safe "kill switch" — cancelling requires `runs/cancel`, and even then the run's already-written output must be verified externally.

### 3.3 The service principal is immutable and mortal

`auth`: "You can't change the service principal assigned to an app or specify an existing service principal during app creation" … "When you delete the app, Databricks deletes the service principal."

**Impact:** the app's identity is coupled to the app's lifetime. Any grant model that points at the service principal dies with the app. This argues for the report's design: **certificates and artifacts must be owned by the migration/tenant, never by the app identity.**

---

## 4. Capabilities the document missed that directly serve our architecture

All verbatim from `best-practices` unless noted. Each closes a gap the report had to design around.

| Capability | Verbatim | Report gap it closes |
|---|---|---|
| **Egress allow-list** | "Restrict outbound network access. Allow only the domains that your app needs … Use dry-run mode and denial logs to validate your configuration." | §34.1 / C8: DSH has **no** network isolation; we must build it. The platform provides it — with a dry-run mode for validating the policy before enforcing it. |
| **No system packages** | "Apps run as non-privileged users and can't perform actions that require elevated permissions such as root access. You can't install system-level packages using package managers like `apt-get`, `yum`, or `apk`." | Removes an entire local-privilege-escalation class from the control-plane threat model (§34.1). |
| **Environment isolation** | "Use different workspaces to separate development, staging, and production apps. This prevents accidental access to production data during development." | §35.2's "one execution world per tenant" is reinforced at the workspace boundary. |
| **H2C, no custom TLS** | "requires apps to support HTTP/2 cleartext (H2C). Don't implement custom TLS handling." | A concrete implementation constraint on the control plane. |
| **Bind host/port** | "must listen on `0.0.0.0` and use the port specified in the `DATABRICKS_APP_PORT` environment variable." | The document mentioned the port only in a proof-table condition; this is a hard requirement. |
| **Cluster logs to UC Volumes (GA)** | `cluster_log_conf` → `volumes` destination; "logs will be delivered to the destination every `5 mins`"; driver logs at `$destination/$clusterId/driver`. (`submit-run`) | **Strong fit for §33 evidence:** Spark driver/executor logs become content-addressable artifacts on a 5-minute cadence without any app-side collection. |
| **Horizontal scaling (Beta)** | "To run an app across multiple instances for higher availability and concurrency" (`compute-size`) | The control plane can be HA; relevant to §35 batch reliability. |
| **Real OTel spans** | `OTEL_BSP_*` (span) **and** `OTEL_BLRP_*` (log) queues, batch 512, schedule delay 1000 ms, `OTEL_SERVICE_NAME` set (`system-env`) | §19 noted DSH's OTel backend emits log records **not** spans. On this platform both are wired — we can get real traces for the control plane. |

Also verified: "We suggest running jobs and tasks on new clusters for greater reliability" (`submit-run`) — this independently supports the document's "Job cluster (new_cluster) ✅ Best" recommendation.

---

## 5. Still UNVERIFIED (do not build cost or latency plans on these)

The document sourced these only to "web research against official documentation" with no verbatim quote, and I could not confirm them:

| Unverified claim | Why it matters | Blocker |
|---|---|---|
| Serverless environment spec `dependencies` list (§5, §13, §14) | Determines how generated-PySpark deps are declared | Not fetched |
| `spark_python_task` / `python_file` (§5, §9, proof rows 9/15) | The core execution mechanism | `submit-run` content truncated before the task-type section |
| `idempotency_token` max 64 chars (§18) | Underpins the safe-resubmission strategy | Same truncation |
| Jobs/hour = 10,000; concurrent runs ≈ 1,000; API throttling "hundreds/sec" (§17) | Feeds the repair-loop capacity model | `resources/limits` page would not convert |
| **Classic cluster startup 3–10 min; serverless ≈ 30 s (§17)** | **This single claim drives the §17 recommendation "use serverless for repair iterations."** | `resources/limits` page would not convert; `web_search` returned HTTP 401 |
| HTTP request timeout for app endpoints (§24 item 6) | Bounds how long a synchronous call may block | Not documented on the pages fetched — absence, not proof |

**The §17 recommendation is the one I would flag hardest:** its central latency argument rests on an unverified figure. Until the serverless startup number is confirmed, treat "serverless minimizes repair-iteration latency" as a **hypothesis to measure in Phase 0**, not an established fact. The platform direction is plausible (serverless is generally faster-starting), but the specific ~30 s vs 3–10 min comparison should be measured in the target workspace before it informs iteration budgets.

---

## 6. Convergence with the main report

Two independent lines of reasoning reach the same architectural verdict, which is worth stating plainly:

**The control plane is not a place for untrusted code.** The report reached this from DSH's own words — the fs-sandbox is verbatim *"containment, not a security boundary"* (§C1, §18). Databricks reaches it from the opposite direction — *"Don't configure your app to access or process data directly"* and *"App compute is optimized for UI rendering."* Two different platforms, two different vocabularies, one conclusion: **generated PySpark belongs in isolated job compute; the FastAPI process is an orchestrator, never an executor.** The pasted document's §16 is correct and well-supported.

**Where the pasted document is silent and the report is not.** The document verifies only the Spark half of the dual-execution architecture. It never addresses:
- **Where SAS runs** — its Volume layout has `sas_output.csv` "uploaded by App," silently assuming SAS executes *outside* Databricks. The report's §28.2/§35.2 treats the SAS world as an equal execution world requiring its own isolated runtime and license reachability. That half is entirely unverified by this document.
- **Snapshot identity as a certification precondition** (report §35.3 / Q38) — the document's paths are migration-ID-scoped but not content-addressed; `candidate-003.py` is a *name*, not a digest. A file at a fixed path can be swapped between submit and run — exactly the tampering class the report's §33.2 Merkle chain closes.
- **`get_run_output` vs `spark_python_task`** — the document's §19 failure matrix says "read error from `get_run_output`" for a Python-task failure, but §9/§24.5 correctly note that endpoint serves *notebook* exit values. For a `spark_python_task`, failure detail comes from the run's error field in `runs/get`, not `get_run_output`. This internal inconsistency should be resolved before the failure matrix is implemented.
- **Conservative retry semantics** — §19 marks app-crash recovery "Retry? Yes (idempotent)," but idempotency only prevents duplicate *submission*. If the run succeeded and wrote output the app then can't see, the correct behavior is the report's borrowed DSH rule: **verify external state, never blindly retry.**

---

## Bottom line

The architecture holds. The plumbing is genuinely verified — FastAPI on Apps, service-principal auth, Volume read/write, `Can manage run`, `runs/submit`, both compute paths, async polling. Amend the condition list with: exhaustive `result_state` handling (incl. `SUCCESS_WITH_FAILURES`), monitor via `status` not deprecated `state`, treat the Volume auto-grant as conditional on `MANAGE`, budget the shutdown barrier under the 15-second SIGKILL, and **measure** the serverless startup figure before letting it set repair-loop budgets. And verify the SAS half separately — this document doesn't touch it.
