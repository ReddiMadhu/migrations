# sas-pyspark — Code Review & Reality-Check Audit

**Scope:** Reference TypeScript implementation (`src/**/*.ts`), target Python implementation
(`backend/src/sas2pyspark/**/*.py`), operator frontend (`frontend/src/**/*.{ts,tsx}`),
technical specifications (`docs/**/*.md`, `AGENTS.md`).

**Method:** Static audit of the implementation files only. No test commands were run.

---

## 0. Blocking finding: the audit's primary baseline does not exist

The brief asks me to read `src/semantic-ir.ts`, `src/macro-resolution.ts`, `src/executors.ts`,
`src/execution.ts`, `src/reconciliation.ts`, `src/guards.ts` and `src/session/`.

**These files are not present in the working tree.** `src/` and `test/` do not exist
(verified: root contains only `.codeartsdoer`, `.github`, `backend`, `docs`, `frontend`,
`node_modules`, `profiles`, `research-briefs`, `scripts`). There is no `package.json`, no
`tsconfig.json`, and **no `.git` directory** — so the deleted reference cannot even be
recovered from history.

This directly contradicts `AGENTS.md:9`:

> "Until parity is proven, the TypeScript code in `src/` is the reference implementation —
> do not delete or rewrite it before the Python equivalent passes the same proofs."

and `AGENTS.md:13`: "New Python package lives alongside `src/` (e.g. `backend/`). Coexist
during migration; do not replace wholesale."

Consequences for this audit:

1. **Section 3 (TypeScript vs. Python discrepancies) cannot be completed as specified.** There
   is no `src/` to compare against. Every remaining discrepancy finding in this report is
   therefore *internal* to the Python code, or between the Python code and the surviving
   documentation (`AGENTS.md`, `docs/databricks-execution.md`, `docs/security.md`).
2. **The parity proof is unverifiable.** AGENTS.md defines the validation gate as
   `pnpm run verify` (check → test → build) over `src/` + `test/` (11 suites, ~49 tests).
   That gate no longer exists. "Parity proven" is currently an unfalsifiable claim.
3. The `node_modules` tree still contains vitest, Cordis-related and React tooling, so the
   TypeScript stack's dependencies are present while its sources are gone — the deletion was
   not a clean removal.

**This is the single most important finding in the audit.** Everything below is a gap in a
port whose reference baseline has been destroyed before the port was proven. Re-establishing
the reference (or formally accepting Python as the new baseline and rewriting AGENTS.md) is a
prerequisite for any of the recommendations in Section 4 being meaningful.

---

## 1. Executive Codebase Reality Scorecard

Ratings: **1** = toy/demo only · **2** = works on curated inputs · **3** = handles realistic
subset with manual intervention · **4** = handles enterprise scale with gaps · **5** =
production-ready.

| Service | Production Readiness | Real-Scale Viability | Architectural Robustness | One-line verdict |
|---|---|---|---|---|
| SAS parser / semantic IR (`sas/*`, `services/semantic_ir.py`) | 2 | 1 | 3 | Honest structural IR, but ~5 of the 6 named enterprise constructs hard-stop the pipeline. |
| Macro resolution (`sas/macro_expander.py`, `services/macro_resolution.py`) | 2 | 1 | 2 | Single-file, flat-namespace, verbatim-passthrough on failure; silently wrong rather than loud. |
| Databricks execution (`providers/databricks.py`, `services/execution.py`, `executors.py`) | 2 | 2 | 3 | Good retry/idempotency design; no auth refresh, no log streaming, no cancel-on-timeout, dummy is the default. |
| Reconciliation (`services/reconciliation.py`, `providers/spark_profile_job.py`) | 2 | 1 | 2 | Deterministic and well-shaped, but pure-Python full materialization → certain OOM at 50M rows. |
| LLM proposal + guards + repair (`llm/*`, `graph/nodes/guard.py`, `graph/subgraphs/repair.py`) | 2 | 2 | 3 | Guard scaffold is real but unpopulated; no SQL/UDF/API checks; oscillation bounded, not detected. |
| Session / event log / persistence (`session/*`, `services/evidence.py`) | 3 | 2 | 3 | Hash chain is genuinely sound; durability, locking, and multi-worker safety are not. |
| API + frontend (`api/*`, `frontend/src/**`) | 1 | 1 | 2 | Authless by default, in-process job state, no async job model; UI is a 50-event toy. |
| **Composite** | **2** | **1** | **2** | A well-architected skeleton with load-bearing stubs at every execution boundary. |

The through-line: the **deterministic core** (hash chain, content addressing, canonical JSON,
policy monotonicity, approval binding) is genuinely good. The **execution, parsing, and
human-in-the-loop boundaries** are stubs, and several of those stubs return *success* rather
than *not implemented*.

---

## 2. Deep-Dive Gap Analysis by Scenario

### Scenario 1 — Real-world SAS legacy complexity vs. parser & IR capability

**Verdict: the described 3,000-line script never reaches translation.** It is stopped at the
`parse` node by design.

#### What works

- `sas/tokenizer.py:31-137` — a real position-preserving tokenizer: comment forms
  (`/* */` at `:59-66`, `%*...;` at `:67-74`, `* ...;` statement comments at `:75-85`),
  SAS doubled-quote string escaping (`:91-93`), macro/variable triggers (`%`, `&` at
  `:102-110`), two-char operators (`:129-130`).
- `sas/parser.py:62-74` statement grouping, `:134-186` block stacking for `%macro/%mend`,
  `DATA`, `PROC`, `RUN`/`QUIT`, `:181-186` unterminated-block diagnostics.
- `sas/macro_expander.py:164-203` genuinely **iterates** macro expansion (up to 8 passes,
  `:181-186`), which is more than a single-pass substitutor.
- Diagnostics, not exceptions, for unsupported constructs — the failure mode is reviewable,
  not a crash. Good discipline.

#### Where it breaks

**(a) `%do` — the scenario's own named construct — is a hard stop.**
`sas/parser.py:114-120`:

```python
if token.kind == "MACRO" and token.value.lower() in {"%do", "%if", "%sysfunc"}:
    diagnostics.append(SasParseDiagnostic("UNSUPPORTED_MACRO_CONSTRUCT", ...))
```

Any `%do`/`%end` iterative loop, any `%if`, and any `%sysfunc` emits an unsupported-construct
diagnostic. `graph/nodes/parse.py:50-56` then converts *any* diagnostic into
`HUMAN_REQUIRED`:

```python
if snapshot.diagnostics or parser_errors:
    return {"state": "HUMAN_REQUIRED", ...,
            "error": "SAS source contains dynamic or unsupported constructs requiring review"}
```

So DATA-step iterative loops — explicitly named in the scenario — terminate the migration
before a single line is proposed.

**(b) Every macro definition and every `&var` reference also hard-stops the pipeline.**
`services/semantic_ir.py` emits `MACRO_EXPANSION_REQUIRED` for every `%macro` (`:406-413`),
every statement containing `&name` (`:375-380`, `:450-455`). Since `parse.py:50` treats *all*
`snapshot.diagnostics` as a stop condition, **any macro-bearing script is routed to
HUMAN_REQUIRED**. This is the defining characteristic of the scenario's input, so the pipeline
is structurally incompatible with its own target workload.

Note the internal inconsistency this exposes: `parse.py:43-47` carefully filters
`UNRESOLVED_MACRO` / `UNRESOLVED_MACRO_VARIABLE` out of `parser_errors` (implying those are
tolerable), but `:50` checks the **unfiltered** `snapshot.diagnostics`, which contains the
analogous `MACRO_EXPANSION_REQUIRED` codes. The filter is dead.

**(c) The IR has no DATA-step semantics at all.** `sas/parser.py:171-179` classifies `SET`,
`MERGE`, `BY`, `WHERE`, `RETAIN`, `ARRAY`, `OUTPUT` etc. only as `operation` vs `statement`,
capturing raw text truncated to 1024 chars (`:175`):

```python
node = _Draft(kind, first.lower(), visible[0], end_token, [("text", statement_text[:1024])])
```

There is no node for `FIRST.`/`LAST.` group processing, no `RETAIN` accumulation model, no
`DO WHILE`/`DO UNTIL`, no array model, no `KEEP`/`DROP` projection, no format/informat
binding. `services/semantic_ir.py:335-336` mirrors this: `kind = "operation" if re.match(
r"^(set|merge|join|by|where|keep|drop|format|sort|group|summary|retain|array|output)\b", ...)`.
The tokenizer even folds `first.x` into a single identifier (`:111`, the class includes `.`),
so `FIRST.`/`LAST.` cannot be recognized as constructs at all.

Concretely: nothing downstream can distinguish a by-group accumulation from a projection. The
LLM is handed a text blob per statement and must infer the semantics unaided.

**(d) SAS functions named in the scenario are unhandled.** `SCAN`, `COMPRESS`, `INTNX` appear
nowhere in the parser, IR, or prompts. `llm/prompts.py:11-16` instructs the model to
"Translate only the supported SAS semantics represented in the input" — but the input declares
no supported-function vocabulary, so this instruction is unenforceable and the model will
hallucinate freely.

**(e) Macro bodies are never parsed into the IR.** `sas/macro_expander.py:173` strips
`%macro...%mend` blocks from the parse source; `sas/parser.py:92-105` then re-adds each macro
as a **leaf** draft with no children. So for a 3,000-line script where most logic lives inside
macros, the IR is mostly opaque leaves. Work units (`graph/nodes/work_unit.py:24-29`) select
nodes of kind `data-step`/`procedure`/`macro`/`operation` — a `macro` leaf contributes its
name and span, not its body.

**(f) Autocall libraries and cross-file macro resolution are unsupported.**
`sas/macro_expander.py:168` collects definitions with a single regex over **one source
string**. `_BUILTIN_MACROS` (`:29`) is a 12-entry hardcoded list. When a call has no static
definition, `:116-122` appends the call **verbatim** to the expanded source:

```python
if macro is None:
    if name.lower() not in _BUILTIN_MACROS:
        issues.append(MacroIssue("UNRESOLVED_MACRO", ...))
    output.append(source[index:end])   # <- unresolved text passes through unchanged
```

The unresolved macro text is therefore still present when `graph/nodes/propose.py:22-35` reads
the source and ships it to the LLM. The pipeline does not fail; it silently asks the model to
translate text containing an unexpanded `%foo(...)`.

`services/macro_resolution.py:158-170` only detects macro calls that begin a line
(`re.match(r"^%([A-Za-z_][\w]*)...")`) and only resolves against definitions seen **earlier in
the same file** (`def_by_name` is populated in file/line order, `:150-157`). A macro defined
in an included file, or called before its definition, or invoked mid-line — all mis-resolve.

**(g) Parameterization is fragile.** `sas/macro_expander.py:124-132` rejects too-many-args but
on too-few-args silently substitutes `""` (`:131`), producing syntactically valid but
semantically wrong code. Positional references `&1`…`&n` are not even tokenized as macro
variables (`:103` requires a letter/underscore after `&`).

**(h) `%LET`/`&var` substitution ignores lexical context.** `:180` and `:195` run regex
substitution over the whole text, including comments and string literals. A `&` inside a
quoted SQL string is replaced. There is no `%LOCAL`/`%GLOBAL` scoping — `variables` is one
flat dict (`:175`) applied globally at the end.

**(i) Two divergent parsers produce two incompatible IRs.** `graph/nodes/parse.py:26-31` calls
`runtime.semantic_ir.create(...)` (a **line-oriented** parser,
`services/semantic_ir.py:222-301`), and `parse.py:34-47` separately calls `parse_sas(...)` (a
**tokenizer-based** parser, `sas/parser.py`). Both are stored in state:
`ir_snapshot` (line-oriented) and `parser_results` (tokenizer-based). They have different node
ids, different node shapes, and different span semantics. `graph/nodes/work_unit.py:24-29`
selects `ir_node_ids` from `ir_snapshot`; `llm/tools.py:11-19` `inspect_ir_nodes` reads
`ir_snapshot` **and** `parser_results`, merging both by node id. Two id spaces merged into one
map is a latent collision and a guaranteed confusion source for the model.

**(j) Scale ceiling is a hard exception, not a diagnostic.** `services/semantic_ir.py:14`
`MAX_SEMANTIC_IR_NODES = 2048`, enforced in `_flatten` at `:480-481` by **raising**
`WorkbenchError`. A 3,000-line script with ~1,500 statements and nesting routinely exceeds
this. The migration dies with an input-validation error rather than degrading.

**(k) Severe performance pathology.** `sas/macro_expander.py:144-161` `_offset_map` runs
`difflib.SequenceMatcher` over the **entire original vs. expanded source as character
sequences** with `autojunk=False`. That is roughly O(n²) on file length. On a 100 KB script
this can take minutes; on a 3,000-line script with many expansions it is worse. Compounding
this, `sas/parser.py:96-97` calls `tokenize(source)` **inside a loop** over every macro match,
and again at `:109-110` inside a loop over every expansion issue — quadratic re-tokenization.

---

### Scenario 2 — Real Databricks cloud execution & failure modes

**Verdict: the control-plane contract is well designed but the implementation is a
synchronous blocking adapter with no auth refresh, no log streaming, no OOM classification,
and a provider that is not mounted by default.**

#### What works

- `providers/databricks.py:122-126` enforces Unity Catalog Volume governance before submit.
- `:127-137` passes a deterministic `idempotency_token` to `jobs.run_now`, so retry-caused
  double submission is genuinely mitigated. This matches `docs/databricks-execution.md:23`.
- `:205-219` `_retry` correctly classifies 429/5xx/timeout/connection as retryable and uses
  exponential backoff.
- `_LIFECYCLE_STATES`/`_RESULT_STATES` (`:19-36`) correctly treat `SUCCESS_WITH_FAILURES` as
  `FAILED`, satisfying `docs/security.md:21`, and unknown states become `UNKNOWN` rather than
  optimistic success — matching `docs/databricks-execution.md:27`.
- `services/execution.py:82-101` appends `execution/started` **before** the remote call and
  marks `UNKNOWN` on submit/status failure — the crash-ambiguity pattern from
  `docs/databricks-execution.md:53-69` is implemented at the seam level.

#### Where it breaks

**(a) The default executor is a fake that always succeeds.** `services/executors.py:1` is
explicit: *"Dummy executors — deterministic in-process providers for tests and local proofs."*
`DummySparkExecutor.status` (`:63-68`) returns `SUCCEEDED` unconditionally with no polling.
`workbench.py:115-116` constructs `dummy_spark` and `reference_sas` but **never mounts either
into any scope** (verified: `mount` is only called from `api/routes.py:96` for an
explicitly-configured `spark_executor`). `graph/migration_graph.py:173-175` requires a mounted
provider or returns `HUMAN_REQUIRED`. **Out of the box, no migration can execute.**

**(b) `ReferenceCsvSasExecutor` makes the verification circular.** `services/executors.py:91-96`
does not execute SAS:

```python
def execute(self, *, scope, execution_id, source, input_snapshot_id, reference):
    output = self._artifacts.create(scope=scope, data=reference, media_type="text/csv", ...)
    return {"runId": f"reference-{execution_id}", "output": output}
```

It copies caller-supplied bytes into an artifact. The "reference" side of reconciliation is
therefore whatever the operator pasted in. The five-step rule's "reconciler verifies" step can
be satisfied by feeding the candidate's own output as the reference, and nothing in the
pipeline detects this. This is the deepest correctness problem in the codebase.

**(c) Token expiry / OAuth refresh: not handled.**
`providers/databricks.py:101-104` builds the `WorkspaceClient` **once**, at construction, with
a static token:

```python
token = secret_resolver.resolve(credential_ref)
client = WorkspaceClient(host=host, token=token)
```

There is no refresh path, no re-auth on 401, no credential rotation. A Databricks OAuth token
expires in roughly an hour; `max_wait_seconds` defaults to 900 s (`:61`) and cold cluster
starts are 5–10 min, so a single run can straddle an expiry boundary. On expiry the SDK raises
401, `_retry` (`:205-219`) does **not** classify 401 as retryable, and the run dies
permanently with no re-auth attempt.

**(d) Cold-start latency: the polling loop holds an HTTP request.**
`services/execution.py:94-98` awaits `wait_for_terminal` via `asyncio.to_thread`, blocking the
request for up to 900 s. `api/routes.py:159-162` runs the **entire migration** inside
`graph.ainvoke` within the HTTP handler:

```python
result = await graph.ainvoke(initial_state, config={"configurable": {"thread_id": session.id}})
```

There is no job queue, no `202 Accepted` + polling model, no background task. Any gateway with
a 60 s timeout (nginx/ALB defaults) kills the request mid-migration. This directly answers the
scenario's question: **execution is fully synchronous.**

**(e) No remote driver log streaming.** Nothing in the tree reads Spark driver/executor logs.
`providers/databricks.py:154,163` captures `status.state_message`, but
`services/execution.py:116-124` builds `completed_data` **without `provider_message`** — it is
discarded. `docs/databricks-execution.md:51` claims "The adapter retains raw provider
state/result/message as evidence." The adapter does; the event log does not. The operator sees
only an opaque `errorCode`.

**(f) OOM and pre-emption are indistinguishable from user bugs.** `_LIFECYCLE_STATES` maps both
`TERMINATED` and `INTERNAL_ERROR` to `FAILED` (`:24,26`); a driver OOM, a node pre-emption, and
a `NullPointerException` in generated code all normalize to `FAILED`. With `state_message`
discarded (point (e)), there is **no way to tell a transient infrastructure failure from a
deterministic code defect** — and therefore no basis for deciding whether to retry.

**(g) Timeouts leak running clusters.** `providers/databricks.py:167-181` returns `UNKNOWN` when
the deadline expires. `services/execution.py:102-107` appends `execution/unknown` and returns.
**Neither calls `provider.cancel(run_id)`** — `ExecutionService.cancel` exists
(`:128-133`) but is never invoked from the timeout path. The remote job keeps running and
burning DBUs after the harness has given up.

**(h) Inconsistent UNKNOWN vocabulary.** Timeout/sync-failure produces `execution/unknown`
(`execution.py:106`); submit/status/output failure produces `execution/completed` with
`outcome: UNKNOWN` (`_mark_unknown`, `:135-141`). Two event shapes for one semantic concept.

**(i) Idempotency is not durably mapped.** `docs/databricks-execution.md:13` requires a
"durable run_id mapping". `services/execution.py:71` derives the key as
`sha256_text(f"{session.id}:{execution_id}")[:64]` and passes it to the API, but the mapping
run_id ↔ execution_id is **never persisted**. `DummySparkExecutor._requests`
(`services/executors.py:52`) is the only run store, and it is an unbounded in-process dict.

**(j) Retry backoff is unsuitable for Databricks rate limiting.** `_retry` uses
`0.5 * 2**attempt` → 0.5 s, 1.0 s (`:218`) with no `Retry-After` handling. Databricks 429s
typically require seconds to minutes. The same too-fast backoff is duplicated in
`llm/translation.py:92`.

**(k) The Spark profiler is unreachable and reads the wrong format.**
`providers/spark_profile_job.py:19-20` reads **Excel workbooks**:

```python
reference = spark.read.excel(reference_path, headerRows=1)
```

It also requires the `spark-excel` connector and is meaningless for 50M rows. It is a free
function never referenced by `ReconciliationService` or the graph — dead code that cannot
serve the reconciliation path. Its aggregate cast is `decimal(38,12)` (`:35`), which overflows
to NULL on large sums and rounds doubles, reintroducing the float-drift problem it was meant to
solve.

**(l) SIGTERM "graceful shutdown" is a no-op.** `api/app.py:43-46`:

```python
if sys.platform != "win32":
    def _handle_sigterm(signum, frame): pass
    signal.signal(signal.SIGTERM, _handle_sigterm)
```

The module docstring (`:5`) claims "SIGTERM handler drains in-flight nodes". Nothing is
drained; an in-flight `graph.ainvoke` is simply killed. `docs/databricks-execution.md:94`
requires graceful shutdown within 15 s. Note also that this is skipped entirely on Windows —
the documented primary dev platform (`AGENTS.md:81`).

**(m) The default policy can never reach Databricks.** `workbench.py:75-86` `_local_policy`
sets `allowedVolumePrefixes=(root_dir,)` — a local filesystem path. `providers/databricks.py:122`
requires `/Volumes/`. So the shipped default configuration is structurally incapable of the
production path, while `DEFAULT_EXECUTION_POLICY` (`services/policy.py:41`) — which *does* use
`/Volumes/migration` — is imported at `workbench.py:10` and **never used**.

---

### Scenario 3 — Enterprise-scale dataset reconciliation & data drift

**Verdict: deterministic and correctly shaped, but it materializes entire datasets into Python
process memory and profiles them with pure-Python loops. At 50M rows it is not slow — it is
dead.**

#### What works

- The four-verdict decomposition (schema / aggregate / row / statistical) with an explicit
  `certifiable` flag is a genuinely sound design, and `graph/reconciliation_validation.py:11-28`
  cross-checks that `certifiable` is consistent with the verdicts and `differenceCount`.
- Row comparison is **hash-based**, not value-based (`services/reconciliation.py:115-117`), so
  comparison cost is bounded per row.
- Ordered and unordered (`rowOrder`) modes both exist; unordered uses multiset membership
  (`:422-436`).
- Decimal sums use `Decimal` with `localcontext(prec=50)` (`:134-142`) to avoid binary-float
  accumulation error — the *intent* is right.
- Deterministic policy fingerprinting (`services/policy.py:105-110`) pins the comparison
  semantics, satisfying `docs/security.md:20`.

#### Where it breaks

**(a) Whole-dataset materialization — the core scaling failure.**
`services/reconciliation.py:349-360`:

```python
text = self._artifacts.read_text(scope, artifact)     # entire file into one string
if "json" in artifact.media_type: return _summarize_dataset(_json_rows(text), policy)
```

For JSON, `json.loads(text)` (`:294`) builds the full object tree. For CSV, `_parse_csv`
(`:223-274`) builds a list of lists — and then `:354-359` **parses the file twice**, once for
headers and once via `_csv_rows(text[nl:])`. There is no chunking, no streaming, no Spark/Delta
pushdown. A 50M-row dataset is tens of GB: this is a guaranteed OOM, not a slowdown.

**(b) Profiling is O(rows × columns) in pure Python.** `_summarize_dataset` (`:192-199`) calls
`_profile_column` per column (`:197`), each iterating all rows (`:166-179`). 50M × 200 = **10
billion** cell visits in interpreted Python. Additionally `_dataset_hash` (`:182-185`)
recomputes `_row_key` for every row, so **every row is hashed twice** (`:183` and `:198`).

**(c) No floating-point tolerance exists.** `_normalize_decimal` (`:70-79`) uses `repr(value)`:

```python
text = repr(value)
```

`repr(0.1 + 0.2)` is `'0.30000000000000004'`. `_value_key` (`:100-112`) then embeds that string
in the row hash. There is **no epsilon field anywhere in `ReconciliationPolicy`** (`:22-29`:
version, schema, rowOrder, nullEquality, decimalNormalization, maxDifferences, statistical).
So `0.1 + 0.2 != 0.3` produces a row-hash mismatch → `row` FAIL → the migration is escalated.
Any SAS/Spark difference in float evaluation order (which is common and legitimate) is a
permanent, unfixable false negative.

**(d) `NaN` vs `NULL` vs empty string is unresolved.** `_non_null` (`:62-67`) treats only `None`
and the literal string `"NULL"` as null:

```python
if null_equality == "sql-like" and isinstance(value, str) and value.strip().upper() == "NULL":
    return False
```

- A Spark `NaN` is a float, so `_non_null` returns `True` → it is a **value**, hashed as
  `number:nan`. SAS missing-numeric `.` should be NULL; NaN is not.
- Empty string `""` → `string:""` — also a value. SAS blank and Spark `None`/`""` are three
  representations; only two are null-equivalent.

So a column that is NULL on one side and NaN/empty on the other fails schema typing and row
parity simultaneously.

**(e) Timestamps across timezones: no handling whatsoever.** `_value_key` has no datetime
branch; a `datetime` object falls through to `:112`
`f"json:{canonical_json(snapshot_json(value))}"`, and `snapshot_json`
(`core/json.py:23-49`) raises `TypeError` on non-JSON types. In practice datasets arrive as
JSON, so timestamps are **strings compared lexicographically**:
`2024-01-01T10:00:00Z` ≠ `2024-01-01T15:30:00+05:30` even though they are the same instant.
There is no timezone normalization anywhere in the file.

**(f) Unkeyed comparison cannot localize root cause.** In unordered mode (`:422-436`) each
difference is:

```python
differences.append(ReconciliationDifference(
    kind="row", path=f"rows#{h}", row=row_index,
    reference=h if count > 0 else None, candidate=h if count < 0 else None,
    detail="row multiset membership differs"))
```

`h` is the **full canonical row** and `row_index` is a running counter over the difference
dict — not a row number, not a column. For a 200-column table the operator receives a whole
row and must diff all 200 columns by hand. The difference report does not localize.

**(g) `maxDifferences` does not bound memory.** `:409-436` caps *detailed* differences at
`max_diff` (`:410`), but past the cap it keeps appending stub entries (`:420`, `:434`) with
nulled values. On a 50M-row mismatch the `differences` list grows to **50M objects**;
`difference_count = len(differences)` (`:373`). The cap bounds information, not allocation.

**(h) Other defects.**
- `_add_decimal` swallows `InvalidOperation`/`ValueError` and returns `"0"` (`:140-141`),
  silently zeroing a column sum on parse failure — which then *passes* if the other side is
  also broken.
- CSV header detection `text.index("\n")` (`:358`) raises `ValueError` on `\r`-only line
  endings (classic Mac) and is wrong if a BOM precedes the header.
- `_typed_value` (`:277-290`) types `1234.50` as float `1234.5`, and refuses exponent forms
  (`1E5` stays a string). A column typed numeric on one side and string on the other fails
  schema typing — a spurious, data-dependent failure.
- `relaxed` schema mode returns `NOT_APPLICABLE` (`:377-378`), and
  `reconciliation_validation.py:21-25` requires the first three verdicts to be `PASS` for
  `certifiable`, so a relaxed policy can never certify. The escape hatch is welded shut.
- The Spark pushdown that *would* fix (a)–(c) — `providers/spark_profile_job.py` — is Excel-only
  and unwired (Scenario 2(k)).

---

### Scenario 4 — LLM hallucination, guardrails & repair loops

**Verdict: there is a real guard pipeline, but the shipped guard set is empty, the static
analysis is cosmetic, and oscillation is bounded by a budget rather than detected.**

#### What works

- `graph/nodes/guard.py` is a genuine deterministic gate, not a policy assertion:
  `ast.parse` syntax validation (`:48-51`), forbidden imports (`:53-61`), dynamic-execution
  calls (`:62-63`), a 7-pattern secret scanner including JDBC/ODBC/LIBNAME/DSN
  (`_SECRET_PATTERNS`, `:15-23`).
- The guard is **deny-only**: `GuardService.evaluate` (`services/guards.py:75-93`) raises on the
  first denial and can never authorize. This matches `docs/security.md` intent.
- Repair history accumulates correctly. `graph/state.py:46` declares
  `repair_history: Annotated[list, _append_list]` with a concatenation reducer, so
  `graph/nodes/guard.py:69-72` appending the current digest **extends** rather than replaces.
  Combined with `graph/subgraphs/repair.py:270-286` appending the attempt, history is
  preserved across iterations.
- Three independent anti-loop mechanisms: digest repeat (`guard.py:36-37`), hypothesis repeat
  (`guard.py:40-41`), and reconciliation-signature repeat
  (`graph/repair_policy.py:24-25`, applied at `repair.py:271-273`).
- `progress_error` (`graph/repair_policy.py:28-46`) rejects repairs that introduce a new FAIL
  or under-deliver on the model's own predicted reduction — a real anti-flailing mechanism.
- `TokenMeter` (`core/logging.py:96-133`) accumulates prompt-cache hit/miss, completion tokens,
  and cost.

#### Where it breaks

**(a) Zero guards are registered.** `workbench.py:105`:

```python
runtime.guards = GuardService()
```

`GuardService.__init__` sets `self._guards: list = []` (`services/guards.py:58`). A grep across
`backend/src` for `guards.register` / `.register(` returns **no matches**. `evaluate`
(`:75-78`) iterates an empty list and always returns. The graph's `guard` node
(`graph/nodes/guard.py`) does its own AST checks, but the **service-level guard layer — the
one `docs/security.md:26` calls "fail closed ... never default to unrestricted" — defaults to
allow-all.** This directly contradicts the documented security posture.

**(b) The static analysis that *does* run is bypassable.**
- `_FORBIDDEN_IMPORTS` (`guard.py:14`) omits `sys`, `importlib`, `pickle`, `ctypes`, `shutil`,
  `pathlib`, `tempfile`, `pty`. `import importlib; importlib.import_module("os").system(...)`
  passes cleanly.
- The dynamic-execution check (`:62`) tests `isinstance(node.func, ast.Name)`. Attribute access
  (`builtins.eval`, `__builtins__.exec`) and aliasing (`c = compile; c(...)`) evade it.
- The `diagnostics` tool (`llm/tools.py:38-61`) computes
  `{"pylint": find_spec("pylint") is not None, "mypy": ...}` — it reports whether the linters
  **exist** and never runs them. `externalDiagnosticsAvailable` is cosmetic.

**(c) No SQL-injection guard.** `_SECRET_PATTERNS` has no pattern for dynamic SQL construction —
no f-string/`.format()`/`%` interpolation into SQL, no `spark.sql(...)` with concatenated
input. The generated code runs on Databricks with `spark.sql` available and nothing inspecting
how the SQL string was built.

**(d) No non-vectorized UDF detection.** Nothing flags `udf`, `pandas_udf`, `applyInPandas`, or
`.applyInPandas`. The scenario's named failure — an LLM emitting a row-wise Python UDF — passes
every guard in the system.

**(e) No PySpark API validation.** Nothing checks that the output imports `pyspark`, that
referenced functions exist, or that DataFrame operations are well-formed. Hallucinated
functions (e.g. `df.transform_columns(...)`) are accepted and only fail at runtime on the
cluster — after a 5–10 min cold start, billed.

**(f) Oscillation is bounded, not detected.** `signature_seen`
(`graph/repair_policy.py:24-25`) requires a **byte-identical** signature recurrence. Because
`reconciliation_signature` (`:14-21`) hashes the full `differences` list — including the
`rows#{h}` path with its running counter — an A→B→A cycle only matches if the difference
report reproduces exactly. Genuine two-state oscillation with different-but-equal-wrong outputs
is invisible. The effective guard is `_MAX_REPAIR_ITERATIONS = 3`
(`graph/subgraphs/repair.py:26`). Oscillation therefore *terminates* but each iteration costs a
full cluster submit + reconciliation.

**(g) Progress gating rejects correct patches for bad predictions.** `repair.py:80-91` requires
the repairer to supply `expectedEffect.minimumReduction >= 1`, and `reconcile` checks it
*after* execution (`:260-266`). If the LLM predicts a reduction of 50 and delivers 5, the patch
is discarded to `HUMAN_REQUIRED` — **even if 5 was the correct, complete fix.** A mispredicted
metric kills a correct repair.

**(h) Token limits and spend are metered, never enforced.** `TokenMeter` has no ceiling and no
check. Worse, `graph/nodes/propose.py:38` calls `runtime.token_meter.record(usage)` **without**
`cost_units=`, so `TokenMeter.cost_units` is permanently `0.0` (`core/logging.py:116` adds
`max(0.0, float(cost_units))`). `maximum_cost_units` in `ExecutionPolicy`
(`services/policy.py:29`) is validated for monotonicity (`:74`) and **never consumed anywhere**
— a grep shows no execution-side reference. There is no per-migration budget, no circuit
breaker, no alert.

**(i) Source is sent unbounded.** `llm/prompts.py:24-31` serializes the **entire** `source`
into the prompt with no truncation; `llm/translation.py:40-44` passes it through. For a
3,000-line script this is one oversized prompt per attempt, repeated on every repair iteration
(`graph/subgraphs/repair.py:69` re-reads the current candidate, which grows). A context-window
overflow surfaces as a provider 400 — non-retryable in `_invoke_with_retry`
(`translation.py:85-91`) — so the migration dies with no chunking fallback.

**(j) Provider-fragile content extraction.** `propose.py:45-47`:

```python
if hasattr(candidate, "content"):
    candidate = candidate.content
if not isinstance(candidate, str) or not candidate.strip(): ...
```

Anthropic-style models return `content` as a **list of content blocks**, not a string. The
`isinstance(candidate, str)` check fails and the migration escalates with "translation provider
returned no PySpark source" — a silent misdiagnosis of a serialization issue.

**(k) Tool results are Python repr, not JSON.** `translation.py:73-76`:
`ToolMessage(content=str(result), ...)` stringifies dicts with Python syntax, degrading the
model's ability to use the returned evidence.

---

### Scenario 5 — Persistence, concurrency & worker failure recovery

**Verdict: the hash chain is cryptographically sound. Everything around it — durability,
locking, process isolation, checkpointing — is missing or in-memory.**

#### What works

- `services/evidence.py:48-80` `verify` is correct and complete: it checks **sequence
  contiguity** (`:52-57`), **previous-hash linkage** (`:58-63`), and **content hash**
  (`:64-78`) for every event. This is a real tamper-evidence implementation.
- Content-addressed artifacts are correct: `services/artifacts.py:144-211` uses exclusive
  `"xb"` creation (`:172`), re-verifies the digest on collision (`:174-182`), rejects
  conflicting metadata (`:201-209`), and `read_reference` (`:253-274`) re-verifies stored bytes
  against the digest on every read.
- Canonical JSON with sorted keys and compact separators (`core/json.py:59-69`) — deterministic.
- Scope policy monotonicity (`services/policy.py:64-85`) and the 8-field approval binding
  (`services/approvals.py:16-53`) are both sound, with lazy matching at the execution boundary
  and explicit `STALE_APPROVAL` detection (`graph/nodes/approve.py:96-106`,
  `graph/subgraphs/repair.py:177-185`) — genuine TOCTOU protection.

#### Where it breaks

**(a) "Atomic writes" is not true.** `session/persistence.py:1`:
*"JSONL session persistence — append-only event log with atomic writes."* `append`
(`:134-140`) opens in `"a"` mode, writes one line, and closes — **no `flush()`, no `os.fsync()`,
no lock**. `flush()` (`:142-143`) is literally:

```python
def flush(self, session_id: str) -> None:
    pass
```

A process or host crash loses the unflushed tail. Worse, callers believe they have durability:
`services/execution.py:81` calls `await session.flush()` as a *hard barrier* exactly as
`docs/databricks-execution.md:65` prescribes — but the barrier is a no-op.

**(b) No locking — split-brain within one process.** `append_event`
(`session/service.py:232-251`) is a read-modify-write:

```python
previous = session._current_projection()
expected_sequence = previous["lastSequence"] + 1
previous_hash = previous["lastHash"]
```

There is no `threading.Lock` and no `asyncio.Lock`. Two concurrent appends to the same session
(two FastAPI requests, or the SSE poller racing a graph node) both compute the same sequence
and the same `previousHash` → two events with identical sequence numbers and a forked chain.
The scenario's "two operators on the same session" produces a corrupt log by construction.

**(c) No multi-process safety.** `MigrationSessionService._sessions` (`session/service.py:168`)
is a plain in-process dict. With multiple uvicorn workers, each worker calls `open(session_id)`
and caches its own handle (`:195-204`); both then append to the same JSONL with interleaved
writes and duplicate sequences. On Windows — the documented dev platform — append-mode writes
are not guaranteed atomic and can interleave mid-line.

**(d) Silent corruption swallowing.** `persistence.read` (`:112-132`):

```python
try:
    events.append(_parse_event(json.loads(line)))
except (json.JSONDecodeError, WorkbenchError):
    truncated = True
```

Unparseable lines are **dropped**. `open()` (`session/service.py:195-204`) and `replay()`
(`:206-210`) call `evidence.verify(...)` which will catch a broken *middle* link — but
`PersistedSession.truncated_tail` is **never inspected by any caller**. A truncated *tail*
(tampered or crashed) verifies cleanly, since the remaining chain is internally consistent, and
the loss is never surfaced. The tamper-evidence proof has a silent hole exactly where crash
recovery needs it most.

**(e) `MemorySaver` is the default — HITL state is ephemeral.**
`graph/migration_graph.py:149-159`:

```python
saver = checkpointer or MemorySaver()
...
return graph.compile(checkpointer=saver)
```

`api/app.py:38-39` calls `build_migration_graph(runtime)` with no checkpointer. So
`interrupt()` state for both the execution approval (`graph/nodes/approve.py:108`) and the
certificate decision (`migration_graph.py:59`) lives **in process memory**. A worker restart
loses all pending approvals; with multiple workers, `POST /approve` hitting worker B cannot
find worker A's thread state. `api/routes.py:66-88` `checkpoint_values` then returns `{}`, and
the approve endpoint 422s with "session has no pending operator decision". The module docstring
at `migration_graph.py:7` concedes this: *"The default in-memory checkpointer is for local runs;
production should inject a durable saver."* Nothing does.

**(f) Approvals, scopes, and migration inputs are all ephemeral.**
- `ApprovalService._records` (`services/approvals.py:95`) — in-memory; grants vanish on restart.
- `ScopeService._scopes` (`services/scopes.py:104`) — in-memory; every scope, provider mount,
  and policy assignment is lost. `require_by_id` post-restart raises `POLICY_DENIED` for
  **every** scope.
- `api/routes.py:64` `_migration_artifacts: dict` — in-process; after restart `POST
  /migrations/{id}/run` cannot find its own input (`:138-140`) and 404s. The create→run sequence
  is not restart-safe.

**(g) Session resumption rebuilds state, but unboundedly.** `_fold`
(`session/service.py:96-100`) re-applies every event, and `_apply_event` (`:79-93`) **fully
reconstructs** each `dependency_graph`, `semantic_ir`, `macro_resolution`, and `work_unit`
object from JSON on every replay. Cost is O(events × payload). These objects are then **held in
the projection forever** (`:81,85,89,93`), so a long-running migration accumulates unbounded
memory. `open()` also does `f.read()` of the whole JSONL (`persistence.py:124`).

**(h) Wedged sessions have no recovery path.** `persistence.create` (`:101-110`) does
`os.makedirs(d)`, writes the header, then writes the events file. A crash between the two
leaves a session directory that exists with no events file. The next `create` hits
`os.path.exists(d)` → `SESSION_ALREADY_EXISTS` (`:103-104`) — permanently unrecoverable, with
no repair or purge path.

**(i) Authentication is disabled by default.** `api/app.py:27` `auth_token=None` default.
`api/routes.py:58-61`:

```python
async def require_auth(authorization: str | None = Header(default=None)) -> None:
    token = getattr(api_config, "auth_token", None)
    if token is not None and authorization != f"Bearer {token}":
        raise HTTPException(status_code=401, ...)
```

`auth_token is None` → the dependency always passes. Anyone who can reach the port can create,
run, and approve migrations. Note `routes.py:52-54` defines `_check_auth` which is **never
called** — dead code. And because `EventSource` cannot send headers, `api/sse.py:25-28` has the
same hole plus a structural one: even when a token *is* set, the browser cannot authenticate
the SSE stream, so live updates break.

**(j) The deterministic clock is broken in production.** `api/app.py:35-36`:

```python
from sas2pyspark.services.clock import FixedClock
runtime = await create_workbench(WorkbenchConfig(root_dir=..., clock=FixedClock()))
```

The **API server defaults to `FixedClock`**. `services/clock.py:29-31`:

```python
def now(self) -> str:
    self._tick += 1
    return f"2026-01-01T00:00:{self._tick:02d}.000Z"
```

`_tick` increments on both `now()` and `uuid()` (`:33-35`) and is shared. After 59 events the
format produces `...T00:00:60.000Z`, `...100.000Z` — **invalid ISO-8601** (seconds > 59). Every
event in the production API carries a fabricated constant date, and after ~59 appends the
timestamps are malformed — in the field that feeds the evidence hash. This violates
`AGENTS.md:24` ("production uses ISO-8601 UTC") and silently corrupts audit ordering.

**(k) CORS wildcard with credentials.** `api/app.py:56-62`:
`allow_origins=["*"]` with `allow_credentials=True`. Browsers reject that combination, and if
any layer reflects the origin it becomes a credential-theft vector.

**(l) SSE is single-worker only and leaks.** `api/sse.py:38` opens the session once and caches
it; the live loop (`:53-59`) polls `session.events` — the **in-memory list**. On worker B the
list never grows, so the stream silently emits nothing. There is no heartbeat (proxies with a
60 s idle timeout kill the stream), no `Cache-Control: no-cache` / `X-Accel-Buffering: no`, and
no `request.is_disconnected()` check, so dead connections accumulate.

**(m) Artifacts are not portable.** `services/artifacts.py:193` stores `storage_location` as an
**absolute host path**, and `get()` (`:224-229`) rejects any record whose stored location is not
canonical for the current host. Moving the workbench directory, or running across hosts, breaks
every artifact reference. `_assert_inside` (`:77-94`) uses `os.path.relpath` without
`os.path.realpath`, so a symlink inside the workspace pointing outside passes containment.

**(n) I/O amplification in the hot path.** `read()` (`:241-246`) calls `get()` — which reads and
hashes the whole file — and then reads the file again. `read_reference` (`:253-274`) re-hashes
the full artifact on **every** call. `services/model_context.py:147` calls `read_reference`
**inside the per-node loop**, so a 128-node context re-reads and re-hashes the same file up to
128 times.

---

## 3. TypeScript vs. Python discrepancies

**As stated in Section 0, this section cannot be executed as specified: `src/` does not
exist.** The reference implementation has been deleted, and there is no `.git` with which to
recover it. The findings below are the discrepancies I *can* substantiate — between the Python
implementation and the surviving specifications, and internally within the Python code.

### 3.1 Python vs. the surviving specifications

| # | Specification says | Python does | Location |
|---|---|---|---|
| 1 | `src/` is the reference; do not delete before parity is proven | `src/` and `test/` are absent; no `.git` to recover them | `AGENTS.md:9,13` |
| 2 | Canonical JSON is `json.dumps(..., sort_keys=True, separators=(",", ":"))` | `ensure_ascii=False` is set, so any non-ASCII payload produces **different bytes and a different SHA-256** than the documented form. Any surviving TS digest would not match. | `core/json.py:69` vs `AGENTS.md:22` |
| 3 | Adapter "retains raw provider state/result/message as evidence" | `state_message` is captured into `ExternalRunStatus` but never written to the event | `providers/databricks.py:154,163`; `services/execution.py:116-124` vs `docs/databricks-execution.md:51` |
| 4 | SparkExecutor takes a `signal` for cancellation | The Python `SparkExecutor` Protocol has **no signal parameter**, and there is no cancellation propagation | `docs/databricks-execution.md:36` vs `services/executors.py:40-44` |
| 5 | "Durable run_id mapping" | run_id ↔ execution_id is never persisted | `docs/databricks-execution.md:13` vs `services/execution.py:71` |
| 6 | "Graceful shutdown within 15 seconds of SIGTERM" | Handler is `pass`; skipped on Windows | `api/app.py:43-46` vs `docs/databricks-execution.md:94` |
| 7 | Guards "fail closed ... never default to unrestricted" | `GuardService` registers **zero** guards → allow-all | `services/guards.py:58,75-78`; `workbench.py:105` vs `docs/security.md:26` |
| 8 | "Append-only durable table" | JSONL with no fsync, no lock; `flush()` is `pass` | `session/persistence.py:1,134-143` vs `docs/security.md:24` |
| 9 | "Independent reconciliation service/provider" | `ReconciliationService` is constructed and **never wired**; the shipped `build_reconciliation_subgraph` hardcodes a PASS verdict | `workbench.py:113`; `graph/subgraphs/reconciliation.py:31-41` vs `docs/security.md:19` |
| 10 | "No host execution tool" | `DummySparkExecutor` fabricates output in-process; `ReferenceCsvSasExecutor` copies caller bytes rather than executing SAS | `services/executors.py:63-68,91-96` vs `docs/security.md:13` |
| 11 | `pnpm run verify` is the gate | The gate's sources are gone; `pnpm`, vitest and React tooling remain in `node_modules` | `AGENTS.md:39-47` |
| 12 | Tests in `test/` (11 suites, ~49 tests), port assertions verbatim | `test/` is absent; `backend/tests/` exists separately and cannot be compared against the originals | `AGENTS.md:55-58` |

### 3.2 Internal Python drift (latent parity hazards)

These are the places where two Python implementations of "the same thing" disagree. If a TS
reference ever reappears, these are where silent divergence will hide.

1. **Two SAS parsers, two IRs, two id spaces.** `services/semantic_ir.py:222-301` (line-oriented)
   vs `sas/parser.py:81-203` (tokenizer-based). Both stored in graph state
   (`graph/nodes/parse.py:26-47`) and merged by node id in `llm/tools.py:11-19`. Different node
   ids, different span semantics, different diagnostic vocabularies.
2. **Two reconciliation implementations.** `services/reconciliation.py` (in-memory, used by
   nobody) vs `providers/spark_profile_job.py` (Spark, Excel-only, free function, used by
   nobody). They share no code and no numeric normalization — `decimal(38,12)` casts in one,
   `repr(float)` in the other.
3. **Two canonical-JSON paths with different NaN semantics.** `snapshot_json`
   (`core/json.py:33-35`) **raises** on non-finite floats; `canonical_json` (`:67-69`) does not
   call `snapshot_json` and would emit invalid JSON `NaN`.
4. **Two UNKNOWN event shapes.** `execution/unknown` (`services/execution.py:102-107`) vs
   `execution/completed` with `outcome: UNKNOWN` (`:135-141`).
5. **Two dead stub subgraphs.** `build_execution_subgraph`
   (`graph/subgraphs/execution.py:38-51`) and `build_reconciliation_subgraph`
   (`graph/subgraphs/reconciliation.py:44-56`) are defined and never called. The latter's
   `_write_report` returns hardcoded `certifiable: True` — a stub that **certifies**.
6. **`DEFAULT_EXECUTION_POLICY` is defined, imported, and unused.** `services/policy.py:37-47`
   uses `/Volumes/migration` and `approvalRequired=True`; `workbench.py:75-86` `_local_policy`
   uses a local FS path and `approvalRequired=False`. Production defaults never apply.
7. **`maximum_runtime_ms` / `maximum_cost_units` are validated but never enforced.** Monotonicity
   is checked (`services/policy.py:72-74`) and `maximumRuntimeMs` is set to 15 min — but
   `max_wait_seconds=900.0` is an independent hardcoded default
   (`providers/databricks.py:61`), numerically coincident and causally unrelated.
8. **Approval disabled by default.** `workbench.py:84` `approvalRequired=False`, so
   `graph/nodes/approve.py:80-81` short-circuits past `interrupt()` — the "operator decides"
   step of the five-step rule is off by default.
9. **`error_to_http_code` has no mapping for most execution codes.** `EXECUTION_UNKNOWN`,
   `PROVIDER_UNAVAILABLE`, `APPROVAL_REQUIRED`, `STALE_APPROVAL`, `UNKNOWN_FORMAT`,
   `ARTIFACT_SCOPE_VIOLATION`, `DUPLICATE_ARTIFACT`, `UNSAFE_FORK_BOUNDARY` all fall through to
   `400` (`api/utils.py:8-18`).

---

## 4. Hard architectural recommendations

Ordered by severity. Each is a design change, not a patch.

### P0 — Correctness and safety (block any production use until done)

**R1. Restore or formally retire the TypeScript reference.**
Recover `src/` and `test/` from version control, or record an ADR formally accepting Python as
the baseline and rewrite `AGENTS.md:9-13,27-58`. Without this, "parity" is unfalsifiable and
every downstream guarantee is unprovable. This is a prerequisite for all other work.

**R2. Replace the stubbed auto-certify paths.**
- Delete or gate `graph/subgraphs/reconciliation.py:31-41` — a stub returning `certifiable: True`
  is a loaded gun. Stubs must return `NOT_IMPLEMENTED` / route to `HUMAN_REQUIRED`, never PASS.
- Delete `graph/subgraphs/execution.py` or implement it.
- Wire `ReconciliationService` and a real `SparkExecutor` in `workbench.py`, or make
  `create_workbench` **fail closed** when no executor/reconciler is configured. Today the
  runtime silently assembles a pipeline that cannot execute or reconcile.

**R3. Make the reference execution non-circular.**
`ReferenceCsvSasExecutor` must actually run SAS (or interface with a real SAS runtime /
published golden dataset), and the API must require a reference provenance it can verify
(content-addressed golden set, signed manifest, or operator-attested input with a recorded
chain of custody). Copying caller-supplied bytes cannot be the reference half of a
verification proof.

**R4. Register a real guard set; make the default deny.**
`create_workbench` must register guards for: forbidden imports (including `sys`, `importlib`,
`pickle`, `ctypes`, `shutil`, `pathlib`, `tempfile`), dynamic execution via attribute/alias
paths, dynamic SQL construction (f-string/`format`/`%` into `spark.sql`), non-vectorized UDFs
(`udf`, `pandas_udf`, `applyInPandas`), PySpark API surface validation, and large-credential
detection. `GuardService` must fail closed when empty. Critically, **enforce the guard before
the LLM output is ever persisted as a candidate artifact**, so a rejected proposal cannot enter
the evidence chain.

**R5. Fix the deterministic clock in the API path.**
`api/app.py:35-36` must use `SystemClock()` by default; `FixedClock` must be test-only (fail
fast if instantiated outside tests). `FixedClock.now()` must clamp seconds to 59 or use a real
duration offset.

**R6. Enable authentication by default; fix SSE auth.**
Require a configured token (fail to start if absent), enforce it on both routers, and move SSE
to a short-lived signed query token since `EventSource` cannot send headers.

**R7. Add durability and locking to the event log.**
- `append`: write to a temp file then `os.replace`, or open with line buffering and issue
  `os.fsync(f.fileno())` per event (group-commit if throughput demands). Make `flush()` real.
- Add a per-session `asyncio.Lock` in `MigrationSessionService.append_event`, plus an
  inter-process lock file (or move to SQLite/WAL) so multi-worker deployments cannot fork the
  chain.
- Surface `truncated_tail` from `persistence.read` as a `SESSION_CORRUPTION` error rather than
  silently dropping events.

**R8. Inject a durable LangGraph checkpointer.**
`build_migration_graph` must accept a `SqliteSaver`/`PostgresSaver` and `api/app.py` must
construct one. HITL state must survive restarts and be addressable across workers. Approvals,
scopes, and `_migration_artifacts` must move to the same durable store.

### P1 — Real-scale viability

**R9. Rebuild reconciliation around pushdown.**
The comparison must execute *inside* Spark/Delta, returning only bounded aggregates and a
bounded difference sample:
- Schema: compare `df.schema` / `DESCRIBE` output, not Python type-counting.
- Aggregates: `sum`, `count`, `count_nulls`, plus `approx_count_distinct`, per column, with
  `Decimal(38,18)` accumulator semantics and explicit overflow handling.
- Row parity: `EXCEPT ALL` / `MINUS ALL` semantics, or a group-by-full-row count-diff — never
  Python-side multiset hashing.
- Difference report: emit `(key_columns, offending_column, ref_value, cand_value)` triples by
  joining the two frames on key, so the report localizes rather than dumping whole rows.
- Formats: Parquet/Delta/CSV — not Excel. Retire `spark_profile_job.py` or rewrite it.

**R10. Add numeric and temporal comparison policy.**
`ReconciliationPolicy` needs `floatTolerance` (compare `abs(a-b) <= tol * max(1,|b|)` in
`Decimal`), explicit `nanHandling` (`NULL`/`DISTINCT`/`FAIL`), `emptyStringAsNull`, and a
`timezoneNormalization` (parse to UTC with an explicit session zone). Without these, every
float-bearing and timestamp-bearing column produces false failures.

**R11. Bound the difference report by construction.**
Cap the `differences` list itself (not just detail richness), so a 50M-row mismatch cannot
allocate 50M objects. Report `differenceCount` as a separate aggregate.

**R12. Make execution asynchronous and observable.**
- Introduce a job model: `POST /migrations/{id}/run` returns `202` with a job id; a
  `/jobs/{id}` poll endpoint and the existing SSE stream deliver progress. Never block an HTTP
  request for a cluster run.
- Persist `run_id ↔ execution_id ↔ idempotency_key` in the durable store and recover from it on
  restart (query the remote run rather than resubmitting).
- On timeout, **cancel the run** (`provider.cancel`) before recording `UNKNOWN`.
- Stream and persist driver logs and `state_message` into the event so OOM/pre-emption are
  distinguishable from code defects. Classify outcomes (`TRANSIENT_INFRASTRUCTURE` vs
  `DETERMINISTIC_CODE`) to drive retry policy.
- Add OAuth refresh: construct the `WorkspaceClient` per call via a credential provider that
  handles 401, or use the SDK's built-in auth with refresh, and make `_retry` honor
  `Retry-After`.

**R13. Fix artifact portability and read amplification.**
Store `storage_location` as a **relative** path (or drop it and derive from digest), use
`os.path.realpath` in containment checks, cache per-file bytes + digest within a request scope,
and make `read()` verify once rather than twice.

### P2 — SAS coverage (required for the stated enterprise workload)

**R14. Build a real DATA-step IR.**
The IR must model `SET`/`MERGE` inputs, `BY` group processing with `FIRST.`/`LAST.` semantics,
`RETAIN` accumulation, `DO`/`END` loops (including `%do`), arrays, `KEEP`/`DROP`/`RENAME`
projection, and format/informat binding. Until then, translation is text-to-text guesswork.

**R15. Separate "unsupported" from "blocking."**
Introduce a severity model: `ERROR` (blocks), `WARNING` (reviewable, continues), `INFO`.
`%do`, `%if`, `%sysfunc`, and macro expansion requirements must not hard-stop the pipeline —
they should mark regions for the operator and let the rest translate. Today
`graph/nodes/parse.py:50` treats all diagnostics as fatal, which makes the pipeline
structurally incompatible with macro-bearing SAS.

**R16. Implement macro resolution across the include closure.**
Collect definitions transitively over the whole `%include` graph (already discovered by
`WorkspaceDiscoveryService`), resolve in dependency order, honor `%LOCAL`/`%GLOBAL` scoping,
and **fail loudly** (do not pass verbatim) on unresolved calls. Support positional parameters
(`&1`) and quoted/comment-aware substitution.

**R17. Raise the scale ceilings and make them degrade.**
`MAX_SEMANTIC_IR_NODES` (2048) and friends should shard into multiple work units rather than
raise. A 3,000-line script should produce per-macro/per-DATA-step work units — the "bounded
repair context" the docs describe — not one truncated 512-node unit
(`graph/nodes/work_unit.py:35`).

**R18. Replace `difflib` offset mapping.**
`sas/macro_expander.py:144-161` `SequenceMatcher` over whole-file character streams is
O(n²)-ish and will not scale. Build the offset map incrementally during expansion (record
replacement spans as you substitute), which is O(n).

**R19. Collapse the dual parser.**
One parser, one IR, one id space. Either make `sas/parser.py` the sole producer and have
`SemanticIrService` consume it, or delete one. Merging two id spaces in `llm/tools.py:11-19` is
a standing defect.

### P3 — LLM cost and loop control

**R20. Enforce token and cost budgets.**
`TokenMeter` needs a ceiling checked before each model call, wired to
`ExecutionPolicy.maximumCostUnits`. Compute `cost_units` (it is currently always `0.0` because
`propose.py:38` omits the argument) using a per-model price table. Hard-fail the migration on
budget exhaustion rather than logging.

**R21. Chunk the source.**
Bound `source` in `llm/prompts.py` and add a chunking + per-chunk-assembly strategy for large
scripts. Send IR spans plus bounded excerpts (which `model_context` already computes) instead of
the raw file.

**R22. Detect oscillation by state, not byte equality.**
Track a bounded signature history keyed on (verdict vector, difference *kinds* and counts per
column) rather than the exact serialized difference list, and detect cycles of length ≥ 2. Add
"no improvement over N iterations" as a first-class escalation reason independent of the
model's own prediction.

**R23. Decouple progress gating from the model's prediction.**
`graph/repair_policy.py:44` should require *strict improvement* (differenceCount decreased), not
attainment of a self-reported `minimumReduction`. Today a correct patch is discarded because the
model mispredicted its impact.

**R24. Normalize model output extraction.**
Handle list-valued `content` blocks and tool-call-only responses explicitly in
`graph/nodes/propose.py`, and emit tool results as JSON rather than Python `repr`.

---

## Appendix — highest-impact defects (quick index)

| # | File:line | Defect |
|---|---|---|
| A1 | `workbench.py:115-116` | Dummy/reference executors constructed, never mounted → no migration can execute |
| A2 | `workbench.py:113` | `ReconciliationService` never wired → reconcile always HUMAN_REQUIRED |
| A3 | `graph/subgraphs/reconciliation.py:31-41` | Stub returns hardcoded `certifiable: True` |
| A4 | `services/executors.py:91-96` | Reference "execution" copies caller bytes → verification is circular |
| A5 | `workbench.py:105` + `services/guards.py:58` | Zero guards registered → allow-all by default |
| A6 | `api/app.py:35-36` + `services/clock.py:29-31` | API runs on `FixedClock`; timestamps invalid after 59 events |
| A7 | `api/routes.py:58-61` + `api/app.py:27` | Auth disabled by default; `_check_auth` is dead code |
| A8 | `session/persistence.py:134-143` | No fsync/lock; `flush()` is `pass` — "atomic writes" is false |
| A9 | `session/service.py:232-251` | Unlocked read-modify-write on sequence/previousHash → split-brain |
| A10 | `graph/migration_graph.py:159` | `MemorySaver` default → HITL state ephemeral, not multi-worker |
| A11 | `graph/nodes/parse.py:50-56` + `sas/parser.py:114-120` | `%do`/`%if`/macros → unconditional HUMAN_REQUIRED |
| A12 | `services/semantic_ir.py:480-481` | 2048-node cap raises instead of degrading |
| A13 | `services/reconciliation.py:349-360` | Whole-dataset materialization → certain OOM at 50M rows |
| A14 | `services/reconciliation.py:70-79` | `repr(float)` with no epsilon → `0.1+0.2` false failure |
| A15 | `services/reconciliation.py:62-67` | NaN/empty-string not null-equivalent |
| A16 | `services/reconciliation.py:431-434` | Unkeyed diffs report whole rows; no column localization |
| A17 | `services/reconciliation.py:417-434` | `maxDifferences` caps detail, not list length → 50M objects |
| A18 | `providers/databricks.py:101-104` | Static token, no OAuth refresh |
| A19 | `services/execution.py:102-107` | Timeout does not cancel the run → leaked DBUs |
| A20 | `services/execution.py:116-124` | `provider_message` (OOM/pre-empt detail) discarded |
| A21 | `api/app.py:43-46` | SIGTERM handler is `pass`; "drains in-flight nodes" is false |
| A22 | `api/routes.py:159-162` | Entire migration runs synchronously inside an HTTP handler |
| A23 | `core/logging.py:107-116` + `graph/nodes/propose.py:38` | `cost_units` always 0; no budget enforcement |
| A24 | `graph/nodes/guard.py:14,62` | Guard list incomplete; attribute/alias paths bypass it |
| A25 | `services/artifacts.py:193,224-229` | Absolute `storage_location` → non-portable artifacts |
| A26 | `core/json.py:69` | `ensure_ascii=False` diverges from the documented canonical form |
| A27 | `graph/subgraphs/repair.py:88-89` + `graph/repair_policy.py:44` | Correct patches rejected for mispredicted reduction |
| A28 | `graph/nodes/work_unit.py:35` | One 512-node truncated work unit for an entire script |
| A29 | `sas/macro_expander.py:116-122` | Unresolved macros pass through verbatim to the LLM |
| A30 | `sas/macro_expander.py:144-161` | `difflib` whole-file offset mapping is O(n²) |

---

*Prepared as a static audit of the implementation files. No test commands were executed.*
