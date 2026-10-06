# Audit → `prod_hardening` Coverage Analysis

Maps the findings in `AUDIT-REPORT.md` (A1–A30 + recommendations R1–R24) to
`.codeartsdoer/specs/prod_hardening/spec.md` and `design.md`.

**Baseline caveat:** the spec is built on a *second-pass source-grounded audit* and the code
has moved since `AUDIT-REPORT.md` was written. Two of my findings are now stale (see §3) and
the spec is more current on those.

---

## 1. Covered (spec addresses the finding with a requirement + acceptance criteria)

| # | Audit finding | Spec section | Notes |
|---|---|---|---|
| A2 | `ReconciliationService` constructed, never wired by the graph | `spec §5.2.1.9`; `design §1.1.2` | Spec goes further: identifies the **interface mismatch** (`compare()` vs the graph's `reconcile()` kwargs) and prescribes an adapter (`InMemoryReconciler`) that keeps `compare()` pure |
| A5 | Zero guards registered → allow-all | `spec §4.3.5`, `§5.5.1.1` (P0) | Baseline guard set named (7 guards), `fail_closed=True` default, `NO_GUARDS_REGISTERED` denial |
| A6 | `FixedClock` in the API path; `:02d` overflow past 59 | `spec §4.6.3-4`, `§5.12.1.1-2` (P0) | `create_app` gains an `environment` param; production fails fast if `FixedClock` resolves |
| A8 | No fsync/lock; `flush()` is `pass` | `spec §4.2.5`, `§5.3.1.2/3/6/7` (P0) | `SessionPersistence` Protocol; Postgres (prod, row-level lock) or hardened JSONL (fsync + file lock) |
| A9 | Unlocked read-modify-write on sequence/previousHash | `spec §5.3.1.3/5/6` | Durable per-session lock + 409 on duplicate execution |
| A10 | `MemorySaver` default checkpointer | `spec §5.1.1.1-4`; `design §1.1.2` | **Spec is more current.** It catches the new `graph/checkpointer.py:13,17-18,26-29` production fallbacks to `MemorySaver` that did not exist at my snapshot, and prescribes `CheckpointerFactory` as the single fail-closed authority |
| A12 | 2048-node IR cap raises | `spec §5.8.1.11` | Made configurable; explicit error affirmed |
| A13 | Whole-dataset materialization / `json.loads` whole file | `spec §4.1.1`, `§5.2.1.10` | `ThresholdReconciler` dispatches to `SparkReconciler` at `spark_threshold_bytes` (64 MiB); bounded summary only |
| A14 | `repr(float)`, no epsilon | `spec §5.2.1.6`, `§6.1` | `float_tolerance` applies to floats only; ints/decimals/strings exact |
| A15 | NaN / empty-string not null-equivalent | `spec §5.2.1.7`, `§6.1` | `nan_equality` ∈ {`equal`, `distinct`}, deterministic across CSV/JSON/Spark/Python |
| A18 | Static PAT, no OAuth refresh | `spec §4.3.2`, `§5.4.1.1` | `DatabricksCredentialProvider`; SDK credential chain (OAuth M2M) in production |
| A19 | Timeout never cancels the run | `spec §5.4.1.6` | **Spec deliberately inverts my recommendation** — see §3 |
| A20 | `provider_message` / `state_message` discarded | `spec §5.4.1.9` (P1), `§5.14.1.4` | Both added to `execution/completed` **and** `execution/unknown`, redacted |
| A21 | SIGTERM handler is `pass` | `spec §5.14.1.3` | 15 s drain via lifespan shutdown |
| A22 | Whole migration blocks an HTTP request | `spec §5.4.1.13` | `202` + `GET /jobs/{jobId}`; `graph.ainvoke` retained only as a synchronous test path |
| A23 | `cost_units` always 0; no budget enforcement | `spec §5.6.1.1/2/4/5`, `§6.3` | `TokenBudgetPolicy` (4 dimensions) + `check_budget` → `BudgetExceeded` → `HUMAN_REQUIRED` |
| A24 | Guard list incomplete; `builtins.eval`/alias bypasses | `spec §5.5.1.6/7/8/9`, `§6.10` | Adds `importlib`, `sys`, `pickle`, `ctypes`, `shutil`, `pathlib`, `marshal`, `multiprocessing`; `ast.Attribute` detection |
| A26 | `ensure_ascii=False` diverges from the canonical form | `spec §4.6.1`, `§5.11.1.1` (P0) | Aligns to `AGENTS.md:22`; routes `canonical_json` through `snapshot_json` to reject NaN/Inf |
| A30 | `difflib` O(n²) offset map | `spec §5.8.1.10` | Linear incremental offset map |
| — | Timezone comparison | `spec §5.2.1.8` | UTC normalization; same-instant equality |
| — | Driver logs absent | `spec §5.4.1.7/12` | Bounded tail (`max_log_bytes`/`max_log_lines`), content-addressed, redacted |
| — | Failure taxonomy absent (OOM ≡ pre-emption ≡ user bug) | `spec §5.4.1.8` | 9-value `FailureTaxonomy` enum, evidence-based, unrecognized → `UNKNOWN` |
| — | `Retry-After` / jitter | `spec §5.4.1.14`, `§5.6.1.6` | `max(backoff, retry_after)` ± 25 % jitter, on both Databricks and LLM retries |
| — | Two-parser divergence | `spec §5.8.1.5` | Consistency gate; divergence treated as a defect |
| — | Macro bodies unstructured; `&1`; substitution scope; body span | `spec §5.8.1.6/8/9` + `§7.2.24` | P2 work items |
| — | DATA-step semantics (RETAIN/FIRST./LAST./ARRAY/DO) | `spec §5.8.1.4` | Structured IR nodes with spans |
| — | SAS function catalog (SCAN/COMPRESS/INTNX…) | `spec §5.8.1.2` | ~20 functions as `FunctionCall` nodes; no invented 1:1 mappings |
| — | Bounded `%DO %WHILE`/`%UNTIL`/`%EVAL`/`%SYSFUNC` | `spec §5.8.1.1` | `MacroPolicy.max_macro_iterations` → `MACRO_LOOP_LIMIT` |
| — | Repair budget hardcoded at 3 | `spec §5.7.1.1` | Configurable (default 5) |
| — | Oscillation bounded, not detected | `spec §5.7.1.2/3` | Exact signature **plus** bounded Jaccard near-oscillation |
| — | `truncated_tail` ignored; corrupt lines silently dropped | `spec §4.2.6`, `§5.3.1.8/9` (P0) | Surfaced as `SESSION_CORRUPTION` with a sequence range |
| — | `_migration_artifacts` in-process | `spec §5.9` | Derived from durable session events |
| — | SSE reads an in-memory list; no heartbeat/disconnect detection | `spec §5.10.1.1/4` | Streams from the durable log; 15 s heartbeat; `request.is_disconnected()`; `Last-Event-ID` resume |
| — | Frontend: no auth header; first-50 events only | `spec §5.10.1.3/5` | `Authorization: Bearer`; offset/limit "load more" |
| — | `routes.py` reaches into `runtime.sessions._sessions` | `spec §5.3.1.10` | New `find_by_migration_id()` public API |
| — | `src/` absent, `AGENTS.md` stale | `spec §5.13`, `§5.14`; `design §2.4` | Full doc-update task list + a doc/code contract verification gate |
| — | UDF / dynamic-SQL / hallucinated-API detection | `spec §5.5.1.3/4/5` | Explicitly required; inconclusive → `HUMAN_REQUIRED`, never a false pass |

---

## 2. Missing — findings the spec does **not** address

Grouped by severity. "Anchor only" means the finding appears in the spec's evidence-anchor
list (§7) but has **no business rule or acceptance criterion**, so it will not be implemented.

### Critical

| # | Finding | Status |
|---|---|---|
| **A3** | `graph/subgraphs/reconciliation.py:31-41` — `_write_report` returns a **hardcoded `certifiable: True`**; `build_execution_subgraph` is equally stubbed | **Not mentioned anywhere.** The highest-severity correctness defect in the codebase (a stub that certifies) is absent from both spec files. Nothing prevents it being wired in as-is. |
| **A4 / R3** | `ReferenceCsvSasExecutor` copies caller-supplied bytes instead of executing SAS → verification is circular | Anchor only (`spec §7.3.17`). No requirement for reference provenance, attested golden output, or non-circular reference execution. |
| **A11 / R15** | `parse.py:50-56` converts **any** diagnostic into `HUMAN_REQUIRED`, so `%do`, `%if`, `%sysfunc` and every macro hard-stop the pipeline | Anchor only (`spec §7.2.13-14`). The spec adds bounded macro *support* (§5.8.1.1) but never requires a **severity model** (error/warning/info) so non-fatal diagnostics stop blocking the pipeline. Until the constructs in §5.8.1.1 land, the target workload still cannot pass parse. |
| **A17** | `maxDifferences` caps detail richness but the `differences` list itself grows unbounded → a 50M-row mismatch allocates 50M objects | Anchor only (`spec §7.4.10`). No requirement bounds the list length or separates `differenceCount` as an aggregate. |
| **—** | Approvals are in-memory (`approvals.py:95`); grants vanish on restart | Anchor only (`spec §7.6.11`). No durable-approval requirement, despite the approval being the security-critical artifact and `docs/security.md` claiming audit retention. |
| **—** | Scopes, policies and provider mounts are in-memory (`scopes.py:104`); every scope 403s after restart | Anchor only (`spec §7.6.12`). No durable scope/registry requirement. |

### High

| # | Finding | Status |
|---|---|---|
| **A16** | Unkeyed diffs report a **whole row hash** with no offending column; a 200-column table must be hand-diffed | Partially covered. `spec §5.2.1.12` fixes `row_index` (a counter, not a real index) but never requires **column-level localization** — the difference report still cannot name the offending column. |
| **—** | `spark_profile_job.py` reads **Excel only** | Not covered. The prescribed `SparkReconciler` wraps `profile_pair` as-is, so the hardened pushdown path still cannot read Parquet/Delta/CSV — the formats that matter at 50M rows. |
| **—** | `decimal(38,12)` sum cast overflows to NULL and rounds doubles | Anchor only (`spec §7.4.16`). No requirement on accumulator semantics or overflow handling in the Spark path. |
| **A27 / R23** | `progress_error` requires the model's self-predicted `minimumReduction ≥ 1`, evaluated *after* a full cluster run → a correct patch is discarded for a misprediction | Anchor only (`spec §7.5.19-20`). `design §1.1.2` reworks the *signature* but leaves the `minimum_reduction` gate intact. |
| **A28 / R17** | One work unit of 512 truncated nodes for an entire script; no per-macro/per-DATA-step decomposition | Not covered. The "bounded repair context" claim is unaddressed. |
| **R21** | Full SAS `source` sent to the LLM unbounded on every attempt | Anchor only (`spec §7.5.9`). No prompt-size bound or chunking strategy. |
| **—** | Cross-file macro resolution / autocall libraries — definitions are collected from one file only; included-file macros are unresolvable | Not covered. `spec §5.8.1.13` covers `%include` *non-literal paths* but not resolving macros *across* the include closure. |
| **A1** | Dummies constructed but never mounted → no migration can execute out of the box | Partially covered. `spec §5.2.1.1` fixes this for the **reconciler** only; there is no symmetric "default composition must mount an executor or fail closed" rule. |
| **A7** | Auth disabled by default (`auth_token=None` → allow-all) | Partially covered. `spec §5.10.1.3` says the UI must "follow the existing backend authentication architecture" but never requires the **backend** to fail closed when unconfigured. The fail-open default persists. |

### Medium

| # | Finding | Status |
|---|---|---|
| **A25 / R13** | Artifacts store absolute host paths → non-portable; `read()` reads+hashes the file twice; `read_reference` re-hashes on every call; `model_context` re-reads per node (up to 128×) | Not covered. No portability or read-amplification requirements. |
| **—** | `_fold` fully reconstructs every IR/graph/work-unit object on replay and retains them forever → unbounded memory | Not covered. |
| **—** | `create()` partial write (header written, events file missing) → permanently wedged session with no recovery path | Not covered. |
| **—** | CORS `allow_origins=["*"]` with `allow_credentials=True` | Not covered. |
| **—** | `relaxed` schema mode returns `NOT_APPLICABLE`, and the validator requires three `PASS` verdicts → a relaxed policy can never certify | Not covered. The escape hatch stays welded shut. |
| **R24** | `propose.py:45-47` assumes `.content` is a string; list-valued content blocks (Anthropic-style) silently escalate | Anchor only (`spec §7.5.12`). No normalization requirement. |
| **A29** | Unresolved macros pass through **verbatim** into the LLM prompt | Anchor only (`spec §7.2.17`). No requirement to fail loudly rather than ship unexpanded text to the model. |
| **—** | `DEFAULT_EXECUTION_POLICY` is defined, imported, and unused; `_local_policy` uses a local FS path so `/Volumes/` governance can never be satisfied by default | Not covered. |
| **—** | Too-few macro args silently substitute `""`; unsupported PROCs; nested DATA/PROC block stacking; tokenizer re-tokenization O(n·m) | Anchor only (`spec §7.2`). No business rules. |
| **—** | `llm/tools.py` `find_spec` reports linter availability but never runs them | Anchor only (`spec §7.5.7`). |

---

## 3. Where the spec corrects my report

My audit was written against an earlier snapshot. The spec is more current on three points, and
`AUDIT-REPORT.md` should be amended accordingly:

1. **A10 is stale in mechanism.** I wrote "`MemorySaver` is the default checkpointer"
   (`migration_graph.py:159`). The code has since gained `graph/checkpointer.py`, which in
   *production* mode tries SQLite but silently falls back to `MemorySaver()` on a missing path
   (`:13`), a missing module (`:17-18`), or **any** construction exception (`:26-29`). The
   conclusion (fail-open) is unchanged and arguably worse; the location is different.
2. **A2 is stale in mechanism.** `workbench.py:126` now wires
   `runtime.reconciler = config.reconciler or runtime.reconciliation`. The outcome is still
   `HUMAN_REQUIRED`, but the cause is the **interface mismatch** (`ReconciliationService`
   exposes `compare(Dataset, Dataset, policy)`, the graph calls `reconcile(...kwargs)`) — not
   an unset default. The spec's adapter resolution is the right fix.
3. **A19 / R12 — my recommendation was wrong.** I recommended auto-cancelling the run on
   timeout. `spec §5.4.1.6` explicitly decides the opposite: *"Polling timeout shall not
   automatically cancel the Databricks run; cancellation shall be an explicit, separate policy
   action."* This is better — it preserves the run for re-poll and diagnosis
   (`spec §5.4.1.3-5`). My R12 should be withdrawn in favour of the spec's
   `cancellation_deadline` policy.

The spec's own scorecard (`design §0`) is also harsher than mine: it scores **1 on production
readiness for every service**, versus my mix of 1–3. Given the fail-closed guard gap, the
`FixedClock` production path, the no-op `flush()`, and the unwired reconciler, the spec's
uniform 1 is the more defensible reading.

---

## 4. Bottom line

- **Of my 30 indexed defects:** 17 are fully covered by requirements, 5 are partially covered,
  and **8 are absent as requirements** (A3, A4, A11, A17, A27, A28, A29, A25) — of which A3
  (auto-certify stub), A4 (circular reference) and A11 (diagnostics hard-stop) are the ones that
  would bite first in production.
- **Of my 24 recommendations:** 19 are covered, 5 are missing (R3, R15, R17, R21, R23, R13 —
  plus the durable approvals/scopes point from R8).
- **Net:** the spec is a near-superset of my audit on execution, persistence, reconciliation
  and guard mechanics, and is more precise on two points. Its blind spots are the
  *verification-trust* defects (the auto-certify stub and the circular reference executor), the
  *parse-gate severity model*, and *memory bounds* in the difference report, prompt assembly,
  and replay projection.

The single highest-value addition to the spec is a requirement that **no code path may produce
a passing verdict without performing a comparison** — which covers both A3 and A4 at once.
