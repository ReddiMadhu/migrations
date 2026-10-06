# LangGraph Python Migration — Technical Specification

> Status: DRAFT · Derived from an architecture interview on 2026-09-29 plus official LangGraph docs and SAS→PySpark migration domain experience.
> Reference implementation: the TypeScript/Cordis code in `src/` (34 files, ~5229 lines). This spec defines its Python/LangGraph replacement. The TS code is **not deleted** — it is archived to `reference/` once Python is authoritative (see §16).

---

## 1. Goals & scope

**Goal:** Replace the Cordis-native TypeScript backend with a **LangGraph Python** environment that preserves the same expected functionality and all deterministic invariants, then extend it to a production-scale SAS→PySpark migration workbench with a real Databricks execution provider and an operator UI.

**In scope (initial port):**
- All 14 TS services ported 1:1 (sessions, evidence, artifacts, scopes, policy, approvals, guards, execution, reconciliation, workUnits, workspaceDiscovery, semanticIr, macroResolution, modelContext).
- A full SAS parser (PROC/DATA/macro expansion).
- LLM-based SAS→PySpark translation via a guarded LangGraph node.
- A real Databricks execution provider (databricks-sdk-python) plus the deterministic dummies.
- A FastAPI REST API + React/Vite operator UI with SSE live updates.
- Human-in-the-loop approval via LangGraph `interrupt()`/resume.

**Out of scope (initial port):** multi-worker deployment, PostgreSQL, Docker, CI/CD, OTel. These are documented as future milestones.

**First shippable milestone:** Databricks end-to-end — operator provides SAS source + reference I/O Excel in Databricks → translate → run PySpark on same input in Databricks → reconcile candidate output vs reference output Excel.

---

## 2. Decisions summary (from interview)

| Area | Decision |
|---|---|
| Graph topology | Nested LangGraph subgraphs (migration / repair / execution / reconciliation), per state-machine.md |
| LLM placement | LLM is a **guarded node** inside the graph; every transition out of it passes through a deterministic guard/reconciler node before any side effect |
| Parity bar | **Semantically equivalent verdicts** (same PASS/FAIL + certifiable decisions); report artifacts/hashes need not be byte-identical to TS |
| Decimal arithmetic | `decimal.Decimal` with a fixed precision context (28 digits, `ROUND_HALF_EVEN`) |
| DI / composition | Pydantic Settings + a typed service registry/container; dependencies resolved by key |
| Immutability | Pydantic v2 frozen models (`model_config(frozen=True)`) for all payloads, policies, artifacts |
| Event typing | Pydantic discriminated union keyed by a `type: Literal[...]` field (26 event types) |
| Persistence | SQLite (WAL mode) append-only events table; no torn-tail logic needed |
| Scope/provider isolation | Each `ScopeRef` carries its own `dict[str, Executor]`; lookup goes through the scope |
| Async model | **Sync** services; LangGraph handles concurrency via its thread pool / async execution |
| Deterministic clock | Injectable `Clock` protocol; `SystemClock` (prod) / `FixedClock` (tests, replicates TS tick sequence) |
| Canonical JSON | Pydantic `model_dump_json(sort_keys=True)`; reject NaN/Inf, normalize -0.0→0.0 via validators |
| Operator surface | FastAPI REST API + React/Vite SPA (no CLI) |
| Repair loop | LangGraph subgraph with conditional edge back to `CANDIDATE_READY`; checkpointed; guard node rejects regressions; budget = max-iteration check |
| Execution providers | Dummies + **real Databricks** (databricks-sdk-python) |
| Test strategy | Port TS suites 1:1; `conftest.py` provides `with_workbench`/`fixed_clock`/`make_scopes` fixtures |
| REST framework | FastAPI (Pydantic-native, OpenAPI auto-docs) |
| Frontend | React + Vite SPA |
| Frontend state | TanStack Query (server state + SSE) + Zustand (UI state) |
| Databricks auth | databricks-sdk + a `SecretResolver` protocol (env vars in dev, KMS/Vault impl in prod) |
| Errors/telemetry | Typed exception hierarchy (`WorkbenchError` + subclasses per reason code) + structlog with redaction filters |
| LangGraph checkpointer | `SqliteSaver` (file-local, pairs with WAL session store) |
| UI realtime | Server-Sent Events (FastAPI streams event-log appends; React `EventSource`) |
| LLM provider | LangChain model abstraction (`init_chat_model()`), config-driven, swappable |
| Translation | LLM-based: propose node sends SAS + bounded IR + model context → PySpark candidate |
| SAS parsing | **Full SAS parser** (PROC/DATA/macro expansion) — research best implementation approach (see §11) |
| Concurrency | Single worker / single process; one graph invocation at a time |
| Propose prompt | LangChain `ChatPromptTemplate` + tools (LLM can inspect IR nodes/macros/dependencies on demand) |
| Scale | Single migration at a time; very large datasets (GBs); Spark does all heavy lifting; backend orchestrates only |
| Large interdependent SAS | Unbounded dependency graph + lazy loading (remove TS hard limits; discover/load on demand) |
| Deployment | Bare venv + static files (uvicorn serves built React bundle from `backend/static/`); no Docker |
| CI/CD | None initially (local `ruff` + `mypy` + `pytest`); add later |
| TS retirement | Never delete; archive `src/test/` → `reference/` once Python is authoritative |
| Python version | 3.12 |

---

## 3. Deterministic invariants (must survive the port)

These are the load-bearing guarantees from the TS reference. Every one must hold in Python.

1. **Hash-chained event log** — sessions append ordered events; each event hashes the previous; `GENESIS_HASH = "sha256:" + "0"*64` is the chain root. `EvidenceService.verify()` checks contiguous sequence, `previousHash` linkage, and recomputes each `hash` from canonical content. Tamper detection depends on this.
2. **Content-addressed artifacts** — SHA-256 over exact bytes; same digest deduplicates; overwrite rejected (TS uses `flag: 'wx'`; Python uses exclusive-create). Metadata + object stored under `scope.workspace/objects/<hh>/<hash>` and `scope.workspace/metadata/<hash>.json`.
3. **Canonical JSON before hashing/freezing** — sorted keys, compact separators, reject `undefined`/non-finite, normalize `-0 → 0`. Python: Pydantic `model_dump_json(sort_keys=True)` + validators that reject NaN/Inf and normalize -0.0.
4. **Frozen inputs/policies** — arguments and policies are immutable before execution. No in-place mutation of event payloads. Enforced via Pydantic frozen models.
5. **Deterministic clock** — tests inject a fixed clock; production uses ISO-8601 UTC. Domain code **never** calls `time.time()` / `uuid.uuid4()` directly — always through the injected `Clock`.
6. **Five-step rule** — model proposes → harness executes → reconciler verifies → event log proves → operator decides. The LLM is advisory only; the graph enforces this structurally (every LLM-node exit passes through a deterministic guard before side effects).
7. **Policy monotonicity** — a child scope's policy can only narrow its parent (network, hosts, volumes, runtime, cost, data classification, actions). `approvalRequired` is monotonic-OR. `assertPolicyNoWider()` must be ported.
8. **Approval binding** — an approval is valid only while artifact semantic hash + source hash + policy version/hash + environment fingerprint + input snapshot digest + action + scope all match. Stale approval → `STALE_APPROVAL` error.
9. **Deny-only guards** — guards can deny an already-approved request but can never authorize one. Guard denials are logged with `{guardId, reasonCode, policyVersion, evidenceSequence}`.
10. **Scope hierarchy** — `tenant → migration → attempt → candidate → execution`. Child scopes narrow policy. Candidate/execution scopes isolate provider registries.
11. **Reconciliation verdicts** — `schema`, `aggregate`, `row`, `statistical` → `PASS`/`FAIL`/`NOT_APPLICABLE`. `certifiable = all verdicts PASS`. A mismatch → `DIAGNOSING`; `reconciliation/failed` → `HUMAN_REQUIRED`.
12. **UNKNOWN is not retryable** — unresolved external execution outcome → `execution/unknown` event → `HUMAN_REQUIRED`. Never blindly resubmit.

---

## 4. Target architecture — LangGraph graph topology

Per the interview and LangGraph docs (which recommend mixing deterministic + agentic steps in one graph, with subgraphs for distinct state spaces), the system is a **top-level migration graph** that calls **repair / execution / reconciliation as compiled subgraphs**.

### 4.1 Top-level migration graph

```
START
  → discover_workspace        (deterministic: SAS %include graph + closure, lazy)
  → parse_sas                 (deterministic: full SAS parser → semantic AST/IR)
  → resolve_macros            (deterministic: static macro-resolution evidence)
  → create_work_unit          (deterministic: bounded repair context from IR + closure)
  → assemble_model_context    (deterministic: bounded prompt context)
  → propose_translation       (AGENTIC: LLM node — guarded)
  → guard_proposal            (deterministic: static analysis, secret scan, patch identity)
  → [interrupt: approve candidate]   (HITL — see §9)
  → execute_candidate         (subgraph: execution)
  → reconcile                 (subgraph: reconciliation)
  → route_after_reconcile     (conditional edge)
       certifiable  → certify → END
       mismatch     → repair (subgraph) → propose_translation (loop)
       failed       → HUMAN_REQUIRED → [interrupt: operator decision] → END
```

**State** (`MigrationGraphState`, a Pydantic model / `TypedDict` with typed channels):
- `session_id`, `migration_id`, `scope_id`
- `state: MigrationState` (the 10-state enum)
- `sas_source_artifact`, `candidate_artifact`, `reference_input_path`, `reference_output_path`
- `ir_snapshot`, `macro_resolution`, `work_unit`, `model_context`
- `last_reconciliation_result`, `repair_history: list[RepairAttempt]`, `repair_iteration: int`
- `open_execution_ids: set[str]`, `execution_outcomes: dict[str, str]`

### 4.2 Execution subgraph

```
START → policy_check → approval_check → submit → poll_status → collect_output → END
```
- `submit` calls the Databricks provider (or dummy); appends `execution/started` (durable, flushed).
- `poll_status` maps unknown states → `UNKNOWN` → `HUMAN_REQUIRED`.
- `collect_output` verifies output artifact digests.
- Idempotency key = `sha256(session_id:executionId)[:64]`.

### 4.3 Reconciliation subgraph

```
START → load_reference → load_candidate → profile_both → compare → write_report → END
```
- **Heavy lifting in Spark**: a small Databricks Spark job reads both Excel files (reference output + candidate output), computes schema/aggregate/row/statistical profiles, returns JSON. The backend reconciles the JSON profiles (keeps the backend light; handles GB-scale).
- `compare` produces the four verdicts + `certifiable`.
- `write_report` creates a content-addressed report artifact and appends `reconciliation/completed` or `reconciliation/failed`.

### 4.4 Repair subgraph (checkpointed loop)

```
START → diagnose → propose_patch (AGENTIC, guarded) → guard_patch → execute_candidate → reconcile
  → route:  improved & certified → END
             improved & not certified → diagnose (loop, conditional edge)
             regression / oscillation → HUMAN_REQUIRED → END
             budget exceeded (repair_iteration >= max) → HUMAN_REQUIRED → END
```
- LangGraph checkpointing stores each iteration (`SqliteSaver`).
- **Guard node** rejects: same byte patch, same semantic patch, same hypothesis after falsified result, same reconciliation signature, `A→B→A` transition, non-improving candidate, candidate that fixes one metric but creates a prohibited regression.
- Budget = `max_repair_iterations` (configurable); exceeding → `HUMAN_REQUIRED`.

### 4.5 LangGraph-specific gotchas (from official docs)

These are non-obvious and will bite if ignored:

- **`interrupt()` re-runs the entire node from the beginning on resume.** Any code before `interrupt()` executes again. → Side effects before `interrupt()` **must be idempotent** (e.g. upsert, not insert). Prefer placing side effects *after* `interrupt()` or in a separate downstream node.
- **Do not wrap `interrupt()` in `try/except`** — it works by throwing a special exception; a bare `except` swallows it.
- **Do not reorder or conditionally skip `interrupt()` calls** within a node — resume matching is strictly index-based.
- **Validation loops: use a conditional edge, NOT `while True` + `interrupt()`** — the latter causes exponential re-execution on each resume. Pattern: call `interrupt()` exactly once per node invocation; store the re-prompt question in state; route back via `add_conditional_edges` until valid.
- **Subgraphs manage their own checkpoint namespace.** For data that must cross graph boundaries, use a LangGraph `Store` or write to the parent checkpoint explicitly.
- **Checkpoints grow unbounded** over long sessions — prune old checkpoints periodically (retention policy).
- **`thread_id` is the persistent cursor.** Reuse it to resume; new value starts fresh. Keep under 255 chars.
- **`Command(resume=...)` is the only `Command` pattern for graph input.** `Command(update=...)` / `Command(goto=...)` are for *returning from* node functions, not for `invoke()` input.
- Drive interrupt-capable graphs with `graph.stream_events(..., version="v3")`; read `stream.interrupts` / `stream.interrupted`; resume with `Command(resume=...)`.

---

## 5. Service port map (14 services)

Each TS service → a Python class registered in the typed registry. Dependencies injected via constructor. All sync.

| TS service | Python module | Key behavior to preserve |
|---|---|---|
| `EvidenceService` | `evidence.py` | `hash()` (canonical JSON → SHA-256), `verify()` (chain + sequence + recompute), `root()` |
| `MigrationSessionService` | `session/service.py` | `create/open/replay/fork/appendEvent`; projection fold over events; torn-tail recovery (SQLite makes torn-tail unlikely but keep the `session/recovered` + `execution/unknown` projection) |
| `JsonlSessionPersistence` → | `session/persistence.py` | **SQLite WAL** append-only `events` table + `sessions` table; `append` is an atomic insert; `read` loads + verifies; no fsync/torn-tail logic needed |
| `FileSystemArtifactService` | `artifacts.py` | SHA-256 content addressing, dedup, overwrite reject, scope-bound paths, metadata verify on read |
| `ScopeService` | `scopes.py` | tenant→migration→attempt→candidate→execution; `effectivePolicy` (narrow-only); per-scope provider dict; `mount`/`isolate` |
| `PolicyService` | `policy.py` | `resolve`, `validatePolicy`, `assertPolicyNoWider`, `effectivePolicy`, `policyFingerprint` |
| `ApprovalService` | `approvals.py` | `grant`/`assertApproved` (binding fingerprint match + stale detection) / `revoke` |
| `GuardService` | `guards.py` | `register`/`evaluate` (deny-only, denial log) |
| `ExecutionService` | `execution.py` | `runSpark` (policy → approval → guard → submit → status → output → durable events); `cancel`; `markUnknown` |
| `ReconciliationService` | `reconciliation.py` | `reconcile` (load → profile → compare → report → durable event); Spark-side profiling for Excel (see §7) |
| `WorkUnitService` | `work_units.py` | bounded repair context; create/update/close; IR/macro snapshots can't widen |
| `WorkspaceDiscoveryService` | `workspace_discovery.py` | %include graph + closure; **unbounded + lazy** (remove MAX_* limits); cycle/unresolved detection |
| `SemanticIrService` | `semantic_ir.py` | bounded structural IR + source spans; now fed by the full SAS parser |
| `MacroResolutionService` | `macro_resolution.py` | static macro-definition/call/unresolved evidence |
| `ModelContextService` | `model_context.py` | bounded prompt context (nodes/excerpts/evidence bytes) |

**Clock** (`clock.py`): `Clock` protocol with `now() -> str` (ISO-8601 UTC) and `uuid() -> str`. `SystemClock` uses `datetime.now(timezone.utc)` + `uuid.uuid4()`. `FixedClock` replicates the TS tick sequence for tests.

**Registry composition** (`workbench.py` → `create_workbench()`): mirrors TS `createWorkbench()` — instantiate services in dependency order, register in the container, return a `WorkbenchRuntime`. Consumers reference dependencies by key (no Cordis fibers; plain constructor injection).

---

## 6. Data models (Pydantic v2)

All event payloads, policies, artifacts, and graph state are **Pydantic v2 frozen models**.

```python
from pydantic import BaseModel, ConfigDict, Field
from typing import Literal

class WorkbenchModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", validate_assignment=True)

class ExecutionPolicy(WorkbenchModel):
    version: str
    network: Literal["none", "allowlist", "any"]
    allowed_hosts: tuple[str, ...]        # tuple, not list — hashable & immutable
    allowed_volume_prefixes: tuple[str, ...]
    maximum_runtime_ms: int
    maximum_cost_units: int
    maximum_data_classification: Literal["public","internal","confidential","restricted"]
    approval_required: bool
    allowed_actions: tuple[str, ...]
```

**Event discriminated union** (26 types, mirroring `MigrationEventDataMap`):

```python
from typing import Annotated, Union
from pydantic import TypeAdapter

class MigrationCreatedEvent(WorkbenchModel):
    type: Literal["migration/created"] = "migration/created"
    migration_id: str
    tenant_id: str
    title: str

# ... one model per event type ...

MigrationEvent = Annotated[
    Union[MigrationCreatedEvent, ExecutionStartedEvent, ReconciliationCompletedEvent, ...],
    Field(discriminator="type"),
]
```

**Canonical JSON / hashing:**
```python
def canonical_json(model: WorkbenchModel) -> str:
    return model.model_dump_json(by_alias=True)  # sort_keys via model_config

def sha256_json(model: WorkbenchModel) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(model).encode()).hexdigest()
```
Validators reject `NaN`/`Infinity` and normalize `-0.0 → 0.0` (Pydantic field validators), matching the TS `snapshotJson` guarantees.

---

## 7. Reconciliation design

### 7.1 Data flow (confirmed in interview)

```
Operator provides:
  (1) SAS source
  (2) reference input Excel path  (in Databricks UC Volume / DBFS)
  (3) reference output Excel path (pre-computed by SAS elsewhere)

Workbench:
  parse SAS → translate (LLM) → PySpark candidate
  → submit PySpark to Databricks with the reference INPUT Excel
  → Databricks runs it, writes candidate OUTPUT
  → Spark job reads reference OUTPUT Excel + candidate OUTPUT Excel
      → computes schema/aggregate/row/statistical profiles → returns JSON
  → backend reconciles JSON profiles → verdicts → report artifact → durable event
```

The workbench **does not execute the original SAS** — the reference output is pre-computed and provided.

### 7.2 Spark-side profiling (handles GB-scale)

A small PySpark job (submitted via the Databricks provider) reads both Excel files as Spark DataFrames and computes:
- **Schema profile**: column names, types, nullability, Spark type mapping.
- **Aggregate profile**: per-column null count, non-null count, type counts, numeric sum (using Spark `decimal` aggregation to avoid float drift).
- **Row profile**: row count; row hashes (canonical per-row JSON, sorted keys); ordered vs unordered comparison per policy.
- **Statistical profile**: deterministic profile hash.

The job returns a JSON document; the backend's `ReconciliationService` compares the two profiles and produces the four verdicts. This keeps multi-GB Excel data in Spark and the backend light.

### 7.3 Decimal arithmetic

`decimal.Decimal` with a module-level context:
```python
import decimal
decimal.getcontext().prec = 28
decimal.getcontext().rounding = decimal.ROUND_HALF_EVEN
```
Numeric sums are computed as `Decimal` strings. **Note:** this may diverge from the TS BigInt string sums on rounding — acceptable per the semantic-parity decision, but add a parity test against TS fixtures to catch drift on known cases.

### 7.4 Reconciliation policy (ported)

```python
class ReconciliationPolicy(WorkbenchModel):
    version: str
    schema: Literal["exact", "relaxed"]
    row_order: Literal["ordered", "unordered"]
    null_equality: Literal["strict", "sql-like"]
    decimal_normalization: Literal["none", "trim-zeros"]
    max_differences: int   # 1..1000
    statistical: Literal["deterministic-profile", "disabled"]
```
Defaults: `exact / ordered / sql-like / trim-zeros / 25 / deterministic-profile`.

---

## 8. Execution provider — Databricks

### 8.1 Databricks provider

Uses `databricks-sdk-python`. Implements the `SparkExecutor` protocol (`submit` / `status` / `output` / `cancel`).

- `submit`: submits a Databricks job (or one-time run) with the PySpark candidate + input Excel path. Returns `runId`. Uses the deterministic idempotency key.
- `status`: polls `runs.get(runId)`; maps Databricks states to the closed `ExternalRunStatus` enum. **Unknown states → `UNKNOWN`** (never retry).
- `output`: reads the candidate output artifact (Excel/CSV/JSON) from the Databricks workspace/UC volume, materializes it as a content-addressed artifact in the workbench.
- `cancel`: `runs.cancel(runId)`.

`SUCCESS_WITH_FAILURES` → `FAILED` (partial success is failure).

### 8.2 Secret resolver

```python
class SecretResolver(Protocol):
    def resolve(self, reference: str) -> str: ...   # credential_ref → token/value

class EnvSecretResolver:   # dev
    def resolve(self, reference: str) -> str:
        return os.environ[reference]   # e.g. DATABRICKS_TOKEN

# Prod: KmsSecretResolver / VaultSecretResolver added later
```
The Databricks `WorkspaceClient` is constructed with a token from the resolver. **No credential values in code, events, or model context** — only `credential_ref` names (e.g. `DATABRICKS_TOKEN`). Source scanning must catch connection strings, passwords, tokens, SAS `LIBNAME`/ODBC DSNs, embedded URLs.

### 8.3 Dummies (ported)

`DummySparkExecutor` and `ReferenceCsvSasExecutor` are ported for tests and local proofs (deterministic, in-process).

---

## 9. Human-in-the-loop (approval)

Uses LangGraph `interrupt()` + resume (the LangGraph-native HITL pattern from the docs).

```python
from langgraph.types import interrupt, Command

def approve_candidate_node(state: MigrationGraphState):
    decision = interrupt({
        "question": "Approve this PySpark candidate for execution?",
        "candidate_artifact": state.candidate_artifact,
        "scope_id": state.scope_id,
    })
    return Command(goto="execute_candidate" if decision else "human_required")
```

- FastAPI exposes `POST /api/sessions/{id}/approve` → resumes the graph with `Command(resume=True/False)`.
- The React UI shows the pending interrupt (via SSE); the operator approves/rejects inline.
- **ApprovalService still validates binding freshness on resume** (belt-and-suspenders): semantic hash + source hash + policy + environment + snapshot + action + scope must all match. Stale → `STALE_APPROVAL`.

**Critical interrupt rules (from LangGraph docs — see §4.5):**
- Side effects before `interrupt()` must be idempotent (node re-runs from the top on resume).
- Don't wrap `interrupt()` in `try/except`.
- For re-prompting on invalid input, use a conditional edge back to the node — not a `while` loop.

---

## 10. LLM translation (propose node)

```python
from langchain.chat_models import init_chat_model
from langchain.tools import tool

model = init_chat_model(model_provider_config)  # swappable: openai/anthropic/etc.

@tool
def inspect_ir_nodes(ir_id: str, node_ids: list[str]) -> dict:
    """Inspect specific semantic-IR nodes for the current SAS program."""
    ...

@tool
def inspect_macro_definition(macro_name: str) -> dict:
    """Look up a static macro definition and its resolution evidence."""
    ...

@tool
def inspect_dependency(file_path: str) -> str:
    """Read a %include dependency file."""
    ...
```

The propose node uses a `ChatPromptTemplate` with:
- **System prompt**: migration rules, PySpark idioms, the five-step rule, "you propose; you do not certify."
- **Bounded model context**: the assembled `ModelContext` (IR summary, macro evidence, dependency closure) as structured JSON.
- **The target SAS source**.
- **Tools** so the LLM can pull additional IR/macro/dependency detail on demand (agentic, bounded by the model-context limits).

The LLM returns a PySpark candidate (string). The `guard_proposal` node then runs static analysis, secret scan, patch-identity check, and monotonic guards before the candidate proceeds.

---

## 11. SAS parser

A **full SAS parser** (PROC/DATA/macro expansion) is in scope. There is no mature off-the-shelf Python SAS parser. **Recommended approach** (to validate against current open-source projects before committing):

1. **Start with a purpose-built recursive-descent / tokenizer** for the SAS dialects actually being migrated (DATA step, PROC SQL, PROC SORT, PROC FREQ/MEANS/summary, MERGE, %macro/%mend, %include, %let, &var, formats/informats, LIBNAME, KEEP/DROP/RENAME, BY, WHERE, IF/THEN, RETAIN, first./last.). Most control. No grammar dependency.
2. **Consider tree-sitter-sas** if error-tolerant incremental parsing matters for large/legacy files (community grammars are incomplete — you'd extend them).
3. **Consider ANTLR4** if you want a generated parser with good error reporting and a shareable grammar.

The parser feeds `SemanticIrService` (structural IR + source spans) and `MacroResolutionService` (static macro evidence). For 1000-line interdependent SAS, the parser works per-file; `WorkspaceDiscoveryService` resolves the %include graph and feeds files in dependency order.

**Research action:** before implementation, survey current open-source SAS→PySpark migration repos and blogs (e.g. SAS official "Migrate from SAS to Python" content, community GitHub SAS parsers, `saspy`, `sas7bdat`/`pyreadstat` for file reading) to borrow techniques and avoid known pitfalls. The pitfalls in §14 are the starting checklist.

---

## 12. Operator surface — FastAPI + React

### 12.1 FastAPI backend (`backend/api/`)

- `POST /api/migrations` — create a migration (SAS source + reference I/O Excel paths + scope/policy).
- `POST /api/migrations/{id}/run` — invoke the LangGraph migration graph (returns `thread_id`).
- `GET /api/sessions/{id}` — session header + projection (state, open executions, verdicts).
- `GET /api/sessions/{id}/events` — event log (paginated).
- `GET /api/sessions/{id}/stream` — **SSE** stream of event-log appends as they happen.
- `POST /api/sessions/{id}/approve` — resume an interrupted graph with `Command(resume=...)`.
- `GET /api/sessions/{id}/reconciliation` — latest reconciliation result + report artifact.
- `GET /api/scopes` / `POST /api/scopes` — scope hierarchy.
- OpenAPI docs at `/docs` (auto-generated).

Request/response models reuse the Pydantic frozen models (validation + serialization for free).

### 12.2 React frontend (`frontend/`)

- **Vite + React + TypeScript**.
- **TanStack Query** for server state (sessions, events, verdicts) with SSE integration (EventSource → query invalidation/streaming cache).
- **Zustand** for UI state (selected session, filters, approval dialog open).
- **Views**:
  - Migration list / create form (upload SAS, specify Databricks Excel paths).
  - Session detail: event timeline (SSE live), state badge, reconciliation verdicts, report download.
  - Approval queue: pending interrupts with approve/reject.
  - Scope/policy inspector.
- Built bundle served by FastAPI from `backend/static/` (single deploy artifact, no separate web server).

---

## 13. Project layout

```
sas-pyspark/
├── backend/                    # Python package (pyproject.toml)
│   ├── pyproject.toml
│   ├── src/sas2pyspark/
│   │   ├── core/               # hash.py, json.py, errors.py
│   │   ├── services/           # the 14 services + clock.py
│   │   ├── session/            # service.py, persistence.py (SQLite WAL)
│   │   ├── graph/              # migration graph + subgraphs + nodes
│   │   ├── sas/                # full SAS parser + IR
│   │   ├── providers/          # databricks.py, dummy.py, secret_resolver.py
│   │   ├── llm/                # propose node, tools, prompts
│   │   ├── api/                # FastAPI app + routes + SSE
│   │   ├── static/             # built React bundle (gitignored)
│   │   └── workbench.py        # create_workbench() composition
│   ├── tests/                  # pytest (1:1 port of TS suites)
│   │   └── conftest.py         # with_workbench, fixed_clock, make_scopes, make_session
│   └── .venv/
├── frontend/                   # React + Vite
│   ├── package.json
│   ├── src/
│   └── vite.config.ts
├── src/                        # TS reference (kept until parity proven)
├── test/                       # TS tests (kept until parity proven)
├── reference/                  # src/test/ archived here once Python is authoritative
├── docs/
│   └── langgraph-migration-spec.md   # this file
└── AGENTS.md
```

**Python setup (Windows):**
```bash
python -m venv .venv && .venv\Scripts\activate && pip install -e .
```
**Unix:**
```bash
python -m venv .venv && .venv/bin/activate && pip install -e .
```
**Local verify gate:**
```bash
ruff check . && mypy src/sas2pyspark && pytest
```

---

## 14. SAS→PySpark migration pitfalls & best practices

Synthesized from industry experience with production SAS migrations. **Validate against current open-source projects before implementation.** These are the issues that cause reconciliation failures in practice.

| SAS construct | PySpark equivalent | Pitfall |
|---|---|---|
| DATA step row-by-row | DataFrame ops | SAS processes row-by-row with implicit iteration; Spark is columnar/batch. `RETAIN`/`first.`/`last.` need **window functions** with explicit partition/order. |
| `PROC SORT` | `orderBy` | SAS has **stable** sort with deterministic tie-breaking; Spark `orderBy` is non-deterministic for ties unless all keys are specified. Add a tiebreaker column. |
| `MERGE` | `join` | SAS MERGE matches on **pre-sorted** keys and preserves order; Spark joins don't. `IN=` variables need explicit flags. One-to-many vs many-to-many semantics differ. |
| `PROC SQL` | Spark SQL | Mostly direct, but **non-equijoins** and Cartesian products differ. Subquery/HAVING semantics. `count(*)` vs `count(distinct)`. |
| `%macro` / `&var` | Python functions / f-strings | SAS macros are **text substitution** (pre-compile); expansion order matters. Macro loops generate code. Need a macro-expansion phase before translation. |
| `%include` | module import / file read | Dependency graph + ordering (handled by `WorkspaceDiscoveryService`). Circular includes must be rejected. |
| Formats (`DATE9`, `DOLLAR`, `z.`) | Spark `cast` / `date_format` / UDF | Custom formats need UDFs. Numeric formats lose precision if cast to double — use `Decimal`. |
| `LIBNAME` | Spark catalog / table path | SAS librefs → Spark databases/tables or UC volumes. |
| `KEEP`/`DROP`/`RENAME` | `select`/`withColumnRenamed` | Straightforward but must preserve column order for ordered reconciliation. |
| `WHERE` vs `IF` | `filter` | SAS `IF` (DATA-step, row-level, post-iteration) vs `WHERE` (optimization, pre-iteration) differ in timing. |
| `BY` group processing | window functions | `first.var`/`last.var`/`RETAIN` by group → Spark `Window.partitionBy().orderBy()` with row-range. |
| `PROC FREQ`/`MEANS`/`SUMMARY` | `groupBy().agg()` | Watch `N` vs `NMISS`, `MIN`/`MAX` on nulls, `STD` formula (sample vs population). |
| Numeric type | `Decimal` vs `Double` | SAS numeric is **float8**; Spark `double` matches but accumulates drift. Use `Decimal` for monetary/ID columns. Reconciliation `decimal_normalization=trim-zeros` handles trailing-zero drift. |
| Missing values | `NULL` | SAS missing `.` vs special missings `.A`–`.Z` vs empty string. Spark has only `NULL`. Map special missings to sentinel strings or a separate column. `null_equality=sql-like` handles `"NULL"` strings. |
| Date/time | Spark `DateType`/`TimestampType` | SAS dates = days from **1960-01-01**; datetimes = seconds from 1960. Spark uses Unix epoch (1970). **Off-by-3653-days** if not converted. |
| **Row order** | — | SAS DATA steps **preserve input order**; Spark does **not** guarantee row order. This is the #1 reconciliation failure. Use `row_order=unordered` policy or add a deterministic sort key. |
| Floating point | — | SAS and Spark both use IEEE-754 double, but **aggregation order** changes the sum. Decimal sums (§7.3) mitigate. |
| 1000-line interdependent SAS | — | %include chains + macro libraries + format catalogs. Resolve dependency graph first; translate bottom-up; macro-expand before parsing. Lazy-load to avoid loading everything at once. |

**Process best practices:**
- **Macro-expand before parsing** — expand `%macro`/`%let`/`&var` to a canonical expanded source, then parse. Keeps the parser simpler.
- **Translate bottom-up by dependency** — leaf %include files first; their translations become imports for dependents.
- **Preserve source spans** — every translated PySpark line should map back to a SAS source span for difference localization.
- **Reconcile early and often** — don't wait for the full translation; reconcile per-PROC/per-DATA-step to localize drift.
- **Mutation-test the translator** — `>↔>=`, `AND↔OR`, join kind, nulls, partition, order, aggregation, date/missing semantics, duplicates. A translator that looks correct but fails reconciliation is not a success.
- **Never let the LLM certify** — the LLM proposes; the reconciler verifies; the operator decides. The guard node enforces this.

---

## 15. Testing strategy

- **Port the 11 TS suites 1:1** as pytest modules: `artifacts`, `execution`, `macro_resolution`, `model_context`, `reconciliation`, `runtime`, `semantic_ir`, `session`, `work_units`, `workspace_discovery`, plus a new `graph` suite.
- `conftest.py` provides `with_workbench` (tmp_path lifecycle), `fixed_clock` (replicates TS `FixedClock` tick sequence), `make_scopes`, `make_session`.
- Same assertions, same coverage targets: hash-chain tamper detection, contiguous sequence, scope ancestry, artifact dedup/overwrite-reject, policy monotonicity, stale approval, deny-only guards, reconciliation verdicts (schema/aggregate/row/statistical), decimal-sum drift, policy fingerprints.
- **Add a graph suite**: invoke the LangGraph migration graph end-to-end (dummy provider), assert final state + durable events.
- **Add a Databricks integration test** (gated, needs workspace access): submit → status → output → reconcile against a known reference Excel.
- Local verify gate: `ruff check . && mypy src/sas2pyspark && pytest`.

---

## 16. Milestones & TS retirement

| Milestone | Proves |
|---|---|
| M0 — Core port | 14 services + SQLite persistence + Pydantic models; ported pytest suite green; deterministic invariants hold |
| M1 — Graph wired | LangGraph migration graph + subgraphs run end-to-end with dummy provider; FastAPI + React skeleton; SSE event timeline |
| M2 — SAS parser + LLM translation | Full SAS parser → IR; LLM propose node produces PySpark candidate; guard node validates |
| M3 — Databricks E2E (**first shippable**) | Real Databricks submit → status → output → reconcile against reference Excel; operator approval via interrupt/UI |
| M4 — Repair loop | Checkpointed repair subgraph; oscillation guards; budget; human escalation |
| M5 — Production hardening | Postgres checkpointer, multi-worker, CI/CD, OTel, Docker — future |

### M5 future scope (not implemented in the initial port)

- Replace the local checkpointer/session persistence with PostgreSQL-backed durable storage and define checkpoint retention/pruning.
- Add multi-worker high availability, migration leases, and cross-worker execution idempotency.
- Add CI/CD, OpenTelemetry tracing/metrics, and Docker packaging.
- Keep these deployment changes out of M0–M4 acceptance claims; they require separate design, operational controls, and verification.

**TS retirement:** once M3 is green AND the ported pytest suite passes, move `src/` + `test/` → `reference/` (archived, out of the build, kept readable as the reference spec). Never delete outright.

---

## 17. Non-goals & risks

- **Not a kernel-grade sandbox** — the workbench does not confine the LLM or Spark; that's a deployment prerequisite.
- **No multi-worker / no Postgres initially** — single process, SQLite. Horizontal scaling is M5.
- **No CI/CD initially** — local verify only.
- **`decimal.Decimal` may diverge from TS BigInt sums** — acceptable per semantic-parity decision; add parity tests against TS fixtures.
- **Full SAS parser is high-effort and high-risk** — no mature off-the-shelf parser; a hand-written parser for the target dialects is recommended but must be validated against real 1000-line interdependent SAS early.
- **LLM translation is non-deterministic** — the reconciler is the safety net; the guard node rejects regressions; the operator decides on uncertainty. Translation quality depends on the model + prompt + bounded context.
- **Unbounded dependency graph** — removing the TS hard limits loses the bounded-context safety property; lazy loading must still reject cycles and unresolved dependencies.
- **Databricks workspace access required from M3** — no creds in repo; operator configures env / secret resolver.

---

## 18. Open research items (resolved 2026-09-29)

1. **SAS parser implementation** — **Decision: build a custom recursive-descent SAS parser in Python** (no off-the-shelf parser handles 1000-line interdependent SAS with macros). Survey results: `tree-sitter-sas` (404'd, no active grammar); `pyreadstat` (404'd, reads SAS datasets not SAS *code*); `saspy` (requires live SAS installation, not a parser); `spark-excel` (Excel reader, irrelevant). Open-source SAS→PySpark repos (12 found on GitHub) use either (a) rule-based pattern matching + LLM conversion (`mhdara/sas-to-pyspark-migration`), (b) pattern libraries with unit tests (`KunalSpaz/sas-to-pyspark-migration`, `jkanie/SAS-to-PySpark-migration`), or (c) agentic AI (`ganesh2312/sas_to_pyspark`, `ashh50/SAS-to-Pyspark-migration-using-agentic-AI`). **Our approach**: custom parser → structural IR (already in `semanticIr` service) → LLM translation with rule-based fallback for known patterns. See §21 for techniques borrowed from these repos.
2. **LangGraph subgraph state sharing** — **Resolved** (fetched `docs.langchain.com/oss/python/langgraph/use-subgraphs.md`). Two patterns: (a) **call inside a node** — different state schemas, wrapper function maps parent↔subgraph state; (b) **add as node** — shared state keys, pass compiled subgraph to `add_node`. Three persistence modes: per-invocation (default, `checkpointer=None`, supports interrupts + parallel), per-thread (`checkpointer=True`, multi-turn memory, no parallel), stateless (`checkpointer=False`). **Decision**: use pattern (b) for the execute subgraph (shares `MigrationState`); per-invocation persistence (each migration session is independent, but interrupts work). Use `get_state(config, subgraphs=True)` for debugging. Stream subgraph outputs via `stream.subgraphs` projection.
3. **Databricks Excel reading in Spark** — **Resolved** (fetched Databricks docs). Databricks Runtime 17.1+ has native `spark.read.excel(path)` with `.option("headerRows", 1)`, `.option("dataAddress", "Sheet1!A1:E10")`, `listSheets` operation, schema inference, Auto Loader streaming. Limitations: no password-protected files, one header row, merged cells populate top-left only. **Decision**: use Databricks native Excel support (not `crealytics/spark-excel`). Schema/aggregate profiles computed server-side via `df.printSchema()` + `df.summary()`. Fallback to `com.crealytics.spark.excel` for older runtimes.
4. **LangGraph + FastAPI integration** — **Resolved** (fetched `docs.langchain.com/oss/python/langgraph/event-streaming.md` + `deploy.md`). Use `graph.stream_events(input, version="v3")` which returns a run stream with typed projections: `stream.messages` (token deltas), `stream.values` (state snapshots), `stream.interrupts` (HITL payloads), `stream.interrupted` (bool). FastAPI endpoint wraps this in an async generator → SSE. `thread_id` = migration session ID (one thread per session). Resume after interrupt with `Command(resume=...)`. Custom `StreamTransformer` for forwarding reconciliation verdicts as SSE events. Graceful shutdown via `RunControl.request_drain()` on SIGTERM.

---

## 19. Borrowed patterns from DeepSeek Harness & OpenCode

Researched from the repo's DSH verification docs (`docs/deepseek-harness-verification.md`, `docs/dsh-target-audit.md`), OpenCode's official docs (tools, agents), DeepSeek's API docs (context caching), and LangGraph's memory docs. Mapped to the four capabilities you asked about.

### 19.1 Migration creation

| Source | Pattern | Adopt in our port |
|---|---|---|
| **OpenCode** | `/init` analyzes the project → writes `AGENTS.md`; Plan mode (read-only, suggests how) → Build mode (makes changes after review) | The Plan→Build two-phase loop *is* our five-step rule. The propose node is "Plan" (no side effects); execute/reconcile is "Build" (after guard + approval). We already did `/init`→`AGENTS.md`. |
| **OpenCode** | Subagents: `explore` (read-only codebase search), `scout` (external docs/dependency research), `general` (multi-step). Invoked via `@` or automatically by the primary agent. | Model the migration graph's read-only phases (workspace discovery, IR, macro resolution) as LangGraph subgraphs with restricted tool permissions — the "explore" pattern. A `scout`-style subagent fetches external SAS/PySpark docs during translation. |
| **DSH** | Boot compatibility preflight (`app-boot/compatibility-preflight.ts`) audits plugin/runtime compatibility before composition | Add a startup attestation that pins the LangGraph + Pydantic + databricks-sdk versions and refuses to compose if a required service is missing (fail-closed, like DSH). |
| **DSH** | Safe-prefix session forking with `childSessionMeta` lineage; independent replayable candidate branches | Already in our spec (session fork). Keep DSH's lineage fields (parent session, boundary sequence, delegation depth) in the fork header for audit. |
| **OpenCode** | `opencode agent create` interactively generates an agent (description, prompt, permissions, markdown file) | The FastAPI "create migration" endpoint mirrors this: operator provides SAS + reference I/O → system generates the graph run config + scope + policy. |

### 19.2 Editing files (LLM proposes PySpark candidates / repair patches)

| Source | Pattern | Adopt in our port |
|---|---|---|
| **OpenCode** | `edit` = exact string replacement (precise, reviewable, fails if oldString not found); `write` = create/overwrite; `apply_patch` = patch with `*** Add File / *** Update File / *** Move to / *** Delete File` markers | Use OpenCode's `apply_patch` marker format for the repair subgraph's `propose_patch` node — the LLM emits a patch, the guard node validates it (patch identity, monotonic check), then it's applied to the candidate artifact. Exact-string `edit` semantics for targeted fixes. These are proven, diff-able, reviewable patterns. |
| **OpenCode** | Permission model: `allow` / `ask` / `deny` per tool, glob patterns, last-match-wins; `edit` permission gates `write`+`edit`+`apply_patch` | Map directly to our `PolicyService` + `GuardService`: the propose node's file-write tools are `ask` (require approval binding) by default; the guard node is deny-only. Per-scope policy narrowing = OpenCode's per-agent permission override. |
| **DSH** | `ToolRuntime` seam: policy → approval → pre-execute → guard → body → post-execute → result. Tool args/results are frozen, schema-validated, lossless snapshots. | Our execution service already has this seam (policy→approval→guard→submit). Port the frozen-args/frozen-result guarantee: the LLM's proposed patch is a frozen Pydantic model; the applied result is a content-addressed artifact (DSH's "lossless snapshot"). |
| **DSH** | PTC runtimes execute generated code in bounded processes (Node: fresh process, empty env, V8/output/time budgets; Python: process-group/RLIMIT controls) | The translated PySpark **never runs in the backend process** — it's submitted to Databricks (a real isolated substrate). This is DSH's PTC principle applied to Spark: the backend orchestrates; Databricks executes. |
| **LangGraph** | `interrupt()` inside a tool pauses before the side effect for approval | Place `interrupt()` in the apply-patch tool so a human reviews each repair patch before it mutates the candidate. (Watch the idempotency-before-interrupt rule — §4.5.) |

### 19.3 Search (dependency discovery, code lookup, translation examples)

| Source | Pattern | Adopt in our port |
|---|---|---|
| **OpenCode** | `grep` (regex content search via ripgrep, respects `.gitignore`), `glob` (file patterns, sorted by mtime), `.ignore` to override gitignore | Use ripgrep (or Python `rg`/`grep`-equivalent) as the engine for `WorkspaceDiscoveryService`'s unbounded lazy %include graph — find `%include`, `LIBNAME`, macro definitions across 1000-line interdependent SAS. Fast, respects ignore patterns, scales. |
| **OpenCode** | `lsp` tool: `goToDefinition`, `findReferences`, `hover`, `documentSymbol`, `workspaceSymbol`, `callHierarchy` | Build a SAS language-server-like interface over our SAS parser: go-to-definition for macros/%include, find-references for variables, call hierarchy for PROC/DATA dependencies. Feed these as LangChain tools to the propose node so the LLM can navigate the SAS structure on demand (the "tools" prompt choice from the interview). |
| **OpenCode** | `@` fuzzy file search in the TUI | The React operator UI gets a fuzzy file picker for selecting SAS source / reference Excel paths in Databricks. |
| **LangGraph** | `Store` with **semantic search** (vector embeddings) + content filtering for long-term memory; episodic memory as few-shot examples | Use a LangGraph `Store` (SQLite-backed) to save successful SAS→PySpark translation pairs as episodic memory. The propose node retrieves similar past translations (semantic search) as few-shot examples — directly enables the RAG-over-examples pattern for 1000-line varied SAS. Namespace by SAS construct type (DATA step, PROC SQL, MERGE…). |
| **DSH** | Compaction: "model-visible context is a projection; compaction/summaries/prompts never replace durable evidence" | Our `ModelContextService` bounds the context as a projection of the event log + IR. Search results that feed the prompt are projections — the durable events/artifacts remain the authority. |

### 19.4 Prompt caching (reduce cost/latency on repeated translation + repair calls)

| Source | Pattern | Adopt in our port |
|---|---|---|
| **DeepSeek API** | Context caching on disk, **enabled by default**, no code change. Cache prefix units at (a) request boundaries, (b) common-prefix detection across requests, (c) fixed token intervals. `prompt_cache_hit_tokens` / `prompt_cache_miss_tokens` in usage. Best-effort, seconds to build, auto-clears in hours–days. | **Structure the propose prompt as a stable prefix + variable suffix.** Stable prefix = system prompt + migration rules + bounded model context (IR summary, macro evidence, dependency closure) — this is large and reused across repair iterations, so it caches. Variable suffix = the specific SAS snippet being translated + the latest reconciliation diff — small, changes each iteration. This maximizes cache hits on the DeepSeek/Anthropic provider. |
| **DeepSeek API** | Common-prefix detection: requests sharing a prefix `A` get `A` persisted as a cache unit | All repair iterations for one migration share the same system prompt + model context prefix → automatic cache hits after the first iteration. Log `prompt_cache_hit_tokens`/`prompt_cache_miss_tokens` via structlog for cost monitoring. |
| **DSH** | `token-meter` (provider usage vs heuristic pressure), `llm-retry` (retry policy on prepared routes) | Add a token meter that tracks cache hit/miss + cost per migration; retry policy on the LLM adapter (bounded retries, exponential backoff). |
| **OpenCode** | `compaction` hidden agent auto-summarizes long context when needed | Add a LangGraph node that summarizes repair history when the prompt exceeds a token budget — keeps the durable events intact, only the model-facing projection is compacted (DSH's principle). |
| **LangGraph** | Short-term memory = thread-scoped checkpoints; long-term memory = `Store` across threads; procedural memory = prompt re-writing via reflection | Checkpoints hold the per-migration graph state (short-term). The `Store` holds cross-migration translation patterns (long-term, semantic search). Procedural memory: the propose node's system prompt can be refined over time as successful patterns accumulate (reflection) — store the prompt in the `Store`, update it in a background task (not the hot path, to avoid latency). |

### 19.5 Concrete prompt-cache structure for the propose node

```python
# Stable prefix (cached across repair iterations for one migration)
SYSTEM_PROMPT = """You are a SAS→PySpark migration engine. Rules:
- You propose; you do not certify. The reconciler verifies.
- Preserve source spans for difference localization.
- Use decimal.Decimal for monetary/ID columns, never float.
- Map SAS date (days from 1960-01-01) to Spark epoch (1970).
- SAS DATA-step row order is NOT guaranteed in Spark; add deterministic sort keys.
..."""

# Bounded model context (cached — same for all iterations of this migration)
model_context = assemble_model_context(work_unit, ir_snapshot, macro_resolution, dependency_closure)
# → structured JSON: IR node summary, macro definitions, %include graph

# Variable suffix (changes each repair iteration)
sas_snippet = read_sas_span(source_artifact, target_span)
reconciliation_diff = last_reconciliation_result.differences  # empty on first pass

prompt = ChatPromptTemplate.from_messages([
    ("system", SYSTEM_PROMPT),
    ("system", "Model context:\n{model_context}"),   # cached prefix ends here
    ("user",  "Translate this SAS:\n{sas_snippet}\n\n"
              "Previous reconciliation failures:\n{reconciliation_diff}"),  # variable tail
])
```

The system + model-context messages form the cacheable prefix; the user message is the variable tail. DeepSeek/Anthropic prompt caching hits the prefix on every repair iteration after the first.

---

## Appendix A — Migration state machine (ported from state-machine.md)

```
CREATED → TRANSLATING → CANDIDATE_READY → EXECUTING → RECONCILING
  ├→ CERTIFIED
  └→ DIAGNOSING → REPAIRING → CANDIDATE_READY (loop)

Terminals: HUMAN_REQUIRED, FAILED
```

Only the coordinator (graph) transitions state. A provider/model message can request a transition but cannot assert one.

## Appendix B — Failure taxonomy (closed, typed)

```
TRANSLATION_FAILED, UNSUPPORTED_CONSTRUCT, STATIC_VALIDATION_FAILED,
POLICY_DENIED, APPROVAL_REQUIRED, EXECUTION_FAILED, EXECUTION_UNKNOWN,
SCHEMA_MISMATCH, AGGREGATE_MISMATCH, ROW_MISMATCH, DIAGNOSIS_UNCERTAIN,
REPAIR_REJECTED, REPAIR_OSCILLATION, REPAIR_BUDGET_EXCEEDED, CERTIFICATION_BLOCKED,
STALE_APPROVAL, SESSION_CORRUPTION, EVIDENCE_TAMPERED, DUPLICATE_ARTIFACT,
ARTIFACT_SCOPE_VIOLATION, PROVIDER_UNAVAILABLE, SESSION_NOT_FOUND, SESSION_ALREADY_EXISTS, INVALID_INPUT
```

Each maps to a `WorkbenchError` subclass. `ERROR` is never the only machine state.

---

## §20 — Agentic tool-building best practices (from LangChain/LangGraph docs)

Sources fetched from `docs.langchain.com` (2026-09-29): `thinking-in-langgraph`, `workflows-agents`, `interrupts`, `fault-tolerance`, `concepts/memory`, `langchain/guardrails`, `deepagents/overview`.

### 20.1 Graph design methodology (Thinking in LangGraph)

LangGraph's 5-step methodology maps directly onto our state machine:

1. **Map the workflow as discrete steps** — our 14 services are already discrete nodes (§4 service port map).
2. **Identify what each step needs** — LLM (propose), data (execute/reconcile), action (artifact store), user-input (approve). Already captured in the node responsibility table.
3. **Design state — store raw data, not formatted text** — **critical rule**: keep the `MigrationState` raw (SAS source, IR, PySpark code, row data, verdicts). Format prompts on-demand inside the `propose` node. Never store a pre-formatted prompt string in state. This keeps the checkpoint compact and avoids stale-format bugs.
4. **Build nodes with appropriate error handling** — LangGraph's taxonomy:
   - **Transient** (network, rate-limit) → `RetryPolicy` (§20.4).
   - **LLM-recoverable** (bad translation) → store error in state, loop back to `propose` with the error as context.
   - **User-fixable** (policy denial, approval needed) → `interrupt()` (§7).
   - **Unexpected** → bubble up as `WorkbenchError`, transition to `ERROR`.
   - **Saga/compensation** (partial side-effects) → `error_handler` (§20.4).
5. **Wire together with minimal edges + `Command` for routing** — our graph already uses `Command(goto=...)` for the repair loop and approval routing.

**Node granularity**: LangGraph recommends smaller nodes = more checkpoints = less re-execution on failure/resume, at the cost of boilerplate. Our split of `execute` into `execute_spark` + `execute_sas` + `reconcile` follows this — a Spark failure doesn't force re-translation.

### 20.2 Workflow patterns (Workflows and Agents)

Five canonical patterns; we use three:

| Pattern | Where in our graph | Notes |
|---|---|---|
| **Prompt chaining** | `translate` → `execute` → `reconcile` | Sequential, each consumes previous output. |
| **Parallelization** | `execute_spark` ∥ `execute_sas` | Fan-out to two providers, fan-in at `reconcile`. Use `Send` API if provider count becomes dynamic. |
| **Evaluator-optimizer** | `reconcile` → (verdict) → `propose` | Reconcile evaluates; if mismatch, feeds structured feedback back to propose. This is our repair loop. |
| ~~Routing~~ | — | Not needed; our path is deterministic given the verdict. |
| ~~Orchestrator-worker~~ | — | Not needed; single migration session. Could apply if we fan out to per-procedure subgraphs. |

**`ToolNode` + `ToolRuntime`**: LangGraph provides a prebuilt `ToolNode` that handles parallel tool execution, error handling, and state injection. `ToolRuntime[Context, State]` lets tools access graph state and run-scoped context. **Decision**: use `ToolNode` for the execution provider tools (Spark submit, SAS submit, artifact store) so they get parallel execution + error handling for free. The LLM-bound tools (translate, diagnose) stay as explicit nodes because they need prompt formatting and output validation.

### 20.3 Interrupt patterns (Interrupts)

Rules from the docs, applied to our `approve` node:

1. **Don't wrap `interrupt()` in try/except** — it catches the interrupt exception itself. Our `approve` node calls `interrupt(approval_request)` bare.
2. **Don't reorder/skip interrupt calls** — LangGraph matches resume values by index. We have exactly one interrupt point (approval); if we add a second (e.g., policy override), it must be in a stable order.
3. **Side effects before interrupt must be idempotent** — we write the approval request to the event log *before* interrupting. If the operator resumes with the same session, the event is already written. Use `event_id` as the idempotency key.
4. **Use conditional edges, not `while True` + interrupt, for validation loops** — our repair loop uses a conditional edge on `verdict.kind`, not a while-loop. This avoids exponential re-execution on resume.
5. **Streaming with HITL**: use `graph.stream(..., stream_events=True, version="v3")`. When paused, `stream.interrupted` is `True` and `stream.interrupts` has the payload. The FastAPI SSE endpoint (§10) must forward both `events` and `interrupts` channels to the React UI.

**Multiple interrupts**: if we later add a policy-override interrupt alongside approval, map interrupt IDs to resume values: `Command(resume={"approval": value, "override": value})`.

### 20.4 Fault tolerance (RetryPolicy, TimeoutPolicy, error_handler)

```python
from langgraph.errors import RetryPolicy, TimeoutPolicy

retry = RetryPolicy(
    max_attempts=3,
    initial_interval=0.5,
    backoff_factor=2.0,
    retry_on=lambda e: isinstance(e, (ProviderUnavailable, RateLimitError)),
)
timeout = TimeoutPolicy(run_timeout=300, idle_timeout=60)
```

**Application**:
- `execute_spark` / `execute_sas`: `RetryPolicy` on `ProviderUnavailable` + `TimeoutPolicy(run_timeout=300)` (5 min for long SAS jobs) with `runtime.heartbeat()` inside the poll loop.
- `propose` (LLM): `RetryPolicy` on `RateLimitError` only — never retry on `PolicyDenied` or `TranslationFailed` (those are deterministic failures, not transient).
- `reconcile`: no retry — it's pure and deterministic. A failure here is a bug, not a transient error.
- **`error_handler`** for saga/compensation: if `execute_spark` succeeds but `execute_sas` fails, we've created a Spark temp view that should be cleaned up. Register an `error_handler` on the execute subgraph that drops the temp view on failure.
- **`set_node_defaults`**: apply `RetryPolicy` + `TimeoutPolicy` graph-wide, then override per-node for special cases (e.g., `reconcile` gets no retry).
- **Graceful shutdown**: `RunControl` + `request_drain()` on SIGTERM — finish in-flight node, checkpoint, then exit. The FastAPI server must wire SIGTERM to `runtime.request_drain()`.

### 20.5 Memory (short-term + long-term)

LangGraph distinguishes:

- **Short-term** = thread-scoped checkpoint (our SQLite checkpointer, §8). Already covered.
- **Long-term** = `Store` with semantic search + content filtering, scoped to namespaces.

**Long-term memory decision**: use `Store` for **procedural memory** — successful repair patterns. When the reconciler finds a mismatch and the LLM proposes a fix that passes, store the (SAS pattern → fix → verdict) triple as a few-shot example. On future translations of similar SAS patterns, retrieve the top-k examples and inject them into the propose prompt.

```python
from langgraph.store.memory import InMemoryStore

store = InMemoryStore(index={"embed": embed_fn, "dims": 384})
# After a successful repair:
store.put(
    namespace=("repairs", session.scope_id),
    key=work_unit_id,
    value={"sas_pattern": pattern, "fix": fix, "verdict": "MATCH"},
)
# On next propose:
examples = store.search(
    namespace=("repairs", session.scope_id),
    query=current_sas_pattern,
    limit=5,
)
```

**Three memory types mapped**:
- **Semantic** (facts): policy rules, scope configs — already in SQLite, no need for Store.
- **Episodic** (experiences): successful repair patterns → Store with embeddings.
- **Procedural** (instructions): prompt re-writing via reflection — future: if the LLM consistently mistranslates a SAS construct, auto-rewrite the propose prompt. Out of scope for M0–M2.

**Hot path vs background**: write repair examples in the **background** (after the session completes) to avoid adding latency to the migration hot path.

### 20.6 Guardrails (middleware)

LangGraph/LangChain uses middleware-based guardrails:

- `before_agent` — validate request at entry.
- `after_agent` — validate output before returning.

**Built-in**: `PIIMiddleware` (redact/mask/hash/block), `HumanInTheLoopMiddleware` (interrupt on specific tools).

**Our mapping**:
- `before_agent` → our **policy check** + **guard check** (§4 services `policy` + `guards`). Implement as LangGraph middleware that runs before the `propose` node. Deny-only: if no rule allows, block.
- `after_agent` → our **reconciliation** is the output validator. Implement as middleware that runs after `execute` and before the result is committed. If verdict is not `MATCH`, block the commit.
- **PII**: not applicable (SAS source is internal). Skip `PIIMiddleware`.
- **HITL middleware**: our `approve` interrupt is equivalent. Use the native `interrupt()` rather than `HumanInTheLoopMiddleware` for finer control over the approval payload.

**Stacking**: middleware executes in array order. Our order: `[policy_middleware, guard_middleware, reconciliation_middleware]`.

### 20.7 Deep Agents patterns (borrowed, not adopted wholesale)

`create_deep_agent` provides: file systems, subagent spawning, long-term memory, task planning, skills, sandboxes, permissions, context engineering.

**Borrowed**:
- **Context engineering**: summarize/offload large SAS files rather than stuffing the entire source into the prompt. Our `modelContext` service (§4) already bounds the context. Add a summarization step for files > 10k tokens: include the IR + spans + a summary of the raw source.
- **Subagent spawning**: for multi-procedure SAS files, spawn one subgraph per procedure, each with its own `propose → execute → reconcile` loop. Use `Send` API to fan out. **Decision**: defer to M4 (scale milestone); M0–M3 handle single-procedure sessions.

**Not adopted**:
- **Skills/sandboxes/permissions**: our security model is the policy + guard + approval services, not LangGraph's built-in permission system. Keep our deterministic guard.
- **File systems**: our artifact store is content-addressed, not a generic file system. Keep our design.

### 20.8 Summary: what to adopt in the Python port

| Practice | Adopt? | Where | Milestone |
|---|---|---|---|
| Raw state, format on-demand | **Yes** | `MigrationState` | M0 |
| Small nodes for granular checkpoints | **Yes** | execute split | M0 |
| `ToolNode` for execution tools | **Yes** | Spark/SAS/artifact tools | M1 |
| `RetryPolicy` on transient errors | **Yes** | execute nodes | M1 |
| `TimeoutPolicy` + heartbeat | **Yes** | execute nodes | M1 |
| `error_handler` for temp-view cleanup | **Yes** | execute subgraph | M1 |
| `set_node_defaults` | **Yes** | graph construction | M0 |
| Graceful shutdown (`request_drain`) | **Yes** | FastAPI server | M3 |
| Conditional edges for repair loop | **Yes** (already designed) | repair loop | M0 |
| `interrupt()` bare, no try/except | **Yes** (already designed) | approve node | M0 |
| Idempotent side-effects before interrupt | **Yes** | event log write | M0 |
| `stream_events(version="v3")` for SSE | **Yes** | FastAPI + React | M3 |
| `Store` for episodic repair memory | **Yes** | after reconcile | M2 |
| Background memory writing | **Yes** | post-session hook | M2 |
| `before_agent`/`after_agent` middleware | **Yes** | policy/guard/reconcile | M0 |
| Context summarization for large SAS | **Yes** | modelContext | M4 |
| Subagent spawning per procedure | **Deferred** | — | M4 |
| `PIIMiddleware` | **No** | — | — |
| `HumanInTheLoopMiddleware` | **No** (use native interrupt) | — | — |
| Deep Agents skills/sandbox/permissions | **No** (keep our guard) | — | — |

---

## §21 — Open-source harness & coding-agent comparative analysis + SAS migration techniques

Researched 2026-09-29 from: DeepSeek Harness (in-repo docs), OpenCode (`github.com/anomalyco/opencode`, 210.7k stars, `opencode.ai/docs`), Open Interpreter (`github.com/openinterpreter/openinterpreter`, 68.5k stars, harness docs), Kimi Code (`MoonshotAI/kimi-code`), Z.AI/ZCode (Open Interpreter zcode harness), Qwen Code (`QwenLM/qwen-code`), SWE-agent, and 12 open-source SAS→PySpark migration repos on GitHub.

### 21.1 Harness emulation pattern (Open Interpreter — key innovation)

Open Interpreter (Rust fork of OpenAI Codex) introduces **harness emulation**: a single runtime that switches the model-facing prompt, tool schema, message conversion, and response handling per provider, while keeping the native Rust runtime. Harness IDs:

| Harness | Wire API | What it shapes | Reference |
|---|---|---|---|
| `native` | responses/chat | Codex-compatible surface | OpenAI Codex |
| `claude-code` | responses/chat/messages | Claude Code full agent: Bash, Read, Write, Edit, TodoWrite, Glob, Grep, web search/fetch, LSP, subagents | Anthropic Claude Code |
| `claude-code-bare` | responses/chat/messages | Smaller Claude Code profile (DeepSeek default) | Anthropic |
| `zcode` | messages | ZCode-shaped GLM coding-agent: system prompt, headers, tools, todo/plan, skills, session context, subagents | Z.AI Coding Plan |
| `kimi-code` | chat | Kimi Code: prompt, tool defs, prompt-cache key, thinking config, message format | `MoonshotAI/kimi-code` |
| `kimi-cli` | chat | Legacy Kimi CLI: Shell, ReadFile, WriteFile, StrReplaceFile, Glob, Grep, ReadMediaFile, SearchWeb, FetchUrl, SetTodoList, plan-mode, background tasks, AskUserQuestion, Agent | `MoonshotAI/kimi-cli` |
| `qwen-code` | chat | Qwen Code: read, write, edit, shell, glob, grep, todo, ask user, plan exit, agent | `QwenLM/qwen-code` |
| `deepseek-tui` | chat | DeepSeek TUI/CodeWhale: shell, apply patch, edit/write/read, list dir, grep, git status/diff, diagnostics, checklist, plan, tool search | `DeepSeek-TUI/DeepSeek-TUI` |
| `swe-agent` | chat | SWE-agent discussion/command loop (parse assistant response for shell command, inject action, return observation) | `SWE-agent/SWE-agent` |
| `minimal` | chat | Compact software-agent prompt, plain Chat Completions tool list | — |

**Automatic harness defaults**: Open Interpreter infers the harness from the provider/model (Anthropic→`claude-code`, Kimi→`kimi-code`, Qwen→`qwen-code`, DeepSeek→`claude-code-bare`). Explicit `harness="..."` always wins.

**Key insight for our port**: The harness pattern separates *transport* (wire API, message format) from *tool surface* (what tools the model sees) from *runtime* (how tool calls execute). We should adopt this separation in our LangGraph port: the `propose` node's prompt + tool schema should be configurable per LLM provider (DeepSeek, Anthropic, GLM) without changing the deterministic execution/reconciliation runtime.

### 21.2 OpenCode patterns (210.7k stars, `anomalyco/opencode`)

OpenCode is a TypeScript/Bun-based open-source AI coding agent. Key patterns:

| Pattern | Description | Adopt in our port |
|---|---|---|
| **Plan→Build two-phase** | `plan` agent (read-only, denies edits, asks permission for bash) → `build` agent (full access). Tab key to switch. | Already our five-step rule. The `propose` node is Plan (no side effects); execute/reconcile is Build (after guard + approval). |
| **`/init` → AGENTS.md** | Analyzes project → writes `AGENTS.md` at root. Commit to Git. | Already done. |
| **`/undo` + `/redo`** | Revert changes, show original message, tweak prompt, retry. Multiple undos supported. | The operator UI should have undo/redo for migration sessions — revert to a previous checkpoint, re-run with modified parameters. LangGraph's time-travel (`get_state_history`) enables this. |
| **`/share`** | Creates a shareable link to the conversation. | The FastAPI server can expose a read-only session view endpoint for sharing migration evidence with reviewers. |
| **Permission model** | `allow` / `ask` / `deny` per tool, glob patterns, last-match-wins. `edit` permission gates `write`+`edit`+`apply_patch`. | Maps to our `PolicyService` + `GuardService`. Per-scope policy narrowing = per-agent permission override. |
| **Subagents** | `explore` (read-only codebase search), `general` (multi-step). Invoked via `@` or automatically. | Model read-only phases (workspace discovery, IR, macro resolution) as LangGraph subgraphs with restricted tool permissions. |
| **Config surface** | Tools, Rules, Agents, Models, Themes, Keybinds, Commands, Formatters, Permissions, Policies, LSP Servers, MCP servers, ACP, Skills, References, Custom Tools, SDK, Server, Plugins, Ecosystem | Our config is simpler (pyproject.toml + env), but the *concept* of separable configuration surfaces is sound. Keep policy, guard, and provider config as separate files. |
| **OpenCode Zen** | Curated list of models tested and verified by the OpenCode team. | Maintain a curated model list for the migration LLM (DeepSeek, Claude, GLM) with tested prompt configurations. |

### 21.3 DeepSeek Harness patterns (from in-repo docs)

Already documented in §19 and `docs/deepseek-harness-verification.md` + `docs/dsh-target-audit.md`. Key takeaways preserved:

- **Cordis is the worker-local composition kernel** — `Context` is a service repository/proxy; child contexts, `Service`, `Fiber` provide dependency lifecycle. We model this as LangGraph nodes with explicit dependencies.
- **Tool results are canonical and frozen** — policy/approval/guard/body/result seam separated. Tool args/results are schema-validated, deep-frozen, lossless snapshots. Our Pydantic frozen models + content-addressed artifacts implement this.
- **Session authority** — canonical in-memory log + write-behind persistence + explicit flush/checkpoint barriers. Our hash-chained event log + SQLite WAL implements this with stronger integrity (SHA-256 chain, DSH has none).
- **PTC containment is provider-specific** — Node PTC = fresh managed process with budgets; Python PTC = bounded process controls, no filesystem confinement. Our approach: translated PySpark never runs in the backend — submitted to Databricks (real isolated substrate).
- **Boot compatibility preflight** — audits plugin/runtime compatibility before composition. We add startup attestation that pins versions and refuses to compose if a service is missing.

### 21.4 Kimi Code / Z.AI ZCode / Qwen Code — harness-specific techniques

| Harness | Technique | Adopt in our port |
|---|---|---|
| **Kimi Code** | Prompt-cache key in the system prompt for cross-request cache hits. Thinking configuration (reasoning effort mapping). | Structure the propose prompt with a stable prefix (system + rules + model context) that caches, and a variable suffix (specific SAS snippet + reconciliation diff). Map reasoning effort to the migration complexity. |
| **Kimi CLI** | Tool surface: Shell, ReadFile, WriteFile, StrReplaceFile, Glob, Grep, ReadMediaFile, SearchWeb, FetchUrl, SetTodoList, plan-mode, background tasks, AskUserQuestion, Agent. | Our LangGraph tools mirror this: `read_sas`, `write_pyspark`, `str_replace_patch`, `glob_sas`, `grep_sas`, `search_translation_examples`, `fetch_sas_docs`, `set_todo`, `ask_operator`, `spawn_subgraph`. |
| **ZCode (Z.AI)** | Anthropic Messages-compatible with ZCode-shaped system prompt, headers, todo/plan behavior, skills, session context, subagent surface. | If using GLM as the LLM provider, configure the LangGraph `propose` node with ZCode-shaped prompt formatting. The deterministic runtime stays the same. |
| **Qwen Code** | Synthetic setup exchange (date, OS, current directory, folder listing) before the user conversation. | Inject a synthetic setup message into the propose prompt: migration session ID, SAS file path, scope, policy version, reference I/O summary. |
| **DeepSeek TUI** | Turn metadata, repository context, generated project instructions when none found. Tool surface: shell, apply patch, edit/write/read, list dir, grep, git status/diff, diagnostics, checklist, plan, tool search. | The `apply_patch` marker format and `diagnostics` tool are directly useful. Add a `diagnostics` tool that runs `pylint`/`mypy` on the generated PySpark before reconciliation. |
| **SWE-agent** | Discussion/command loop: parse assistant response for shell command, inject action, return observation. 30s timeout. | Not directly applicable (we use structured tool calls, not free-text command parsing), but the *0timeout pattern is relevant for SAS execution. |

### 21.5 SAS→PySpark migration techniques from open-source repos

GitHub search for "sas to pyspark migration" returned 12 repos. Key techniques extracted:

| Repo | Approach | Techniques borrowed |
|---|---|---|
| `mhdara/sas-to-pyspark-migration` | Code analysis + rule-based + LLM conversion + automated equivalence checks against real SAS output | **Most aligned with our approach.** (1) Rule-based conversion for known SAS patterns (DATA step, PROC SQL, MERGE, PROC SORT). (2) LLM conversion for complex/ambiguous constructs. (3) Equivalence checking against real SAS output (our reconciliation). (4) Code analysis phase before conversion (our IR). |
| `KunalSpaz/sas-to-pyspark-migration` | 43 modules, 508 tests, full security audit | **Pattern library approach.** Maintain a library of SAS→PySpark pattern mappings with unit tests. Each pattern: SAS input → expected PySpark output → test. Our `semanticIr` service should encode these patterns. 508 tests = comprehensive coverage target. |
| `jkanie/SAS-to-PySpark-migration` | SAS patterns, equivalent Python code, unit-tests, example usage (Jupyter Notebook) | **Educational pattern library.** Jupyter notebooks documenting each SAS→PySpark conversion with examples. Useful as few-shot examples for the LLM propose node. |
| `beyondleeelaine/SasBridgePy` | Toolkit for migration, validation, edge-case handling | **Edge-case handling.** SAS edge cases (missing values, format conversions, date/time handling, macro expansion, PROC TRANSPOSE, PROC FREQ with WEIGHT). Document these as a pitfall table (already in our spec §13). |
| `ganesh2312/sas_to_pyspark` + `ashh50/SAS-to-Pyspark-migration-using-agentic-AI` | Agentic AI approaches | **Agentic patterns.** Use an LLM agent with tools (read SAS, write PySpark, run tests, check equivalence) in a loop until convergence. This is our propose→execute→reconcile→repair loop. |
| `naviak007/SAS2PY_V1` | Migration acceleration tool building | **Acceleration tooling.** Build a CLI/UI that accelerates migration by automating the repetitive parts (file discovery, pattern matching, test generation). |

### 21.6 Synthesized best practices for agentic tool building

Combining all sources (DSH, OpenCode, Open Interpreter, Kimi Code, Z.AI, Qwen, SWE-agent, LangGraph docs, SAS migration repos):

#### Architecture
1. **Separate transport from tool surface from runtime** (Open Interpreter harness pattern). The LLM provider's wire API, the tool schema the model sees, and the deterministic runtime that executes tool calls are three independent layers.
2. **Plan→Build two-phase loop** (OpenCode). Read-only analysis → propose changes → review → execute. Our five-step rule.
3. **Raw state, format on-demand** (LangGraph). Store raw data in state, format prompts inside nodes. Never store pre-formatted prompt strings.
4. **Small nodes for granular checkpoints** (LangGraph). Split execute into `execute_spark` + `execute_sas` + `reconcile` so a Spark failure doesn't force re-translation.
5. **Boot compatibility preflight** (DSH). Pin versions, refuse to compose if a service is missing, fail-closed.

#### Safety
6. **Permission model: allow/ask/deny per tool** (OpenCode). Last-match-wins with glob patterns. Our `PolicyService` + `GuardService`.
7. **Frozen tool args/results** (DSH). Schema-validated, deep-frozen, lossless snapshots. Pydantic frozen models + content-addressed artifacts.
8. **PTC containment** (DSH). Generated code never runs in the backend process — submit to Databricks (real isolated substrate).
9. **Deny-only guards** (DSH). If no rule allows, block. The guard is not an extension point.
10. **Idempotent side effects before interrupt** (LangGraph). Write the approval request to the event log before `interrupt()`. Use `event_id` as idempotency key.

#### Translation
11. **Rule-based + LLM hybrid** (`mhdara/sas-to-pyspark-migration`). Known patterns → rule-based conversion. Complex/ambiguous → LLM. Both → equivalence checking.
12. **Pattern library with unit tests** (`KunalSpaz/sas-to-pyspark-migration`). Each SAS→PySpark mapping has a test. 508 tests = coverage target.
13. **Few-shot examples from past translations** (LangGraph `Store` + `jkanie` notebooks). Semantic search over successful translation pairs.
14. **Edge-case catalog** (`beyondleeelaine/SasBridgePy`). Missing values, format conversions, date/time, macro expansion, PROC TRANSPOSE, PROC FREQ with WEIGHT.
15. **Prompt caching with stable prefix** (DeepSeek API + Kimi Code). System + rules + model context = stable prefix (caches). Specific SAS snippet + reconciliation diff = variable suffix.

#### Verification
16. **Equivalence checking against real SAS output** (`mhdara/sas-to-pyspark-migration`). Not just unit tests — compare PySpark output against actual SAS execution results. Our reconciliation service.
17. **Diagnostics tool** (DeepSeek TUI). Run `pylint`/`mypy` on generated PySpark before reconciliation. Catch syntax/type errors early.
18. **`/undo` + `/redo`** (OpenCode). Revert to previous checkpoint, re-run with modified parameters. LangGraph time-travel enables this.
19. **Hash-chained event log** (our invariant). DSH has contiguous sequence but no hash chain. We add SHA-256 per-event chain for tamper detection.
20. **Content-addressed artifacts** (our invariant). Same digest deduplicates; overwrite rejected. DSH's "lossless snapshot" principle.

#### UX
21. **`/share` for conversation sharing** (OpenCode). Read-only session view endpoint for sharing migration evidence with reviewers.
22. **Fuzzy file picker** (OpenCode `@` key). React operator UI for selecting SAS source / reference Excel paths.
23. **SSE streaming with typed projections** (LangGraph event-streaming). `stream.messages` for token deltas, `stream.values` for state snapshots, `stream.interrupts` for HITL. Forward each as SSE events to React.
24. **Todo list synced from agent state** (Deep Agents frontend pattern). The operator UI shows the migration progress (parse→translate→execute→reconcile→certify) as a real-time todo list.
25. **Auto-review** (Open Interpreter). Automated review of generated code before presenting to operator. Our guard node + diagnostics tool.

### 21.7 What we adopt vs. reject

| Practice | Source | Adopt? | Where | Milestone |
|---|---|---|---|---|
| Harness emulation (per-provider prompt/tool shaping) | Open Interpreter | **Yes** | propose node config | M1 |
| Plan→Build two-phase | OpenCode | **Yes** (already designed) | propose→execute | M0 |
| `/undo` + `/redo` via time-travel | OpenCode + LangGraph | **Yes** | operator UI | M3 |
| `/share` session view | OpenCode | **Yes** | FastAPI endpoint | M3 |
| Permission allow/ask/deny per tool | OpenCode | **Yes** (already designed) | PolicyService | M0 |
| Rule-based + LLM hybrid conversion | `mhdara` repo | **Yes** | translate node | M1 |
| Pattern library with unit tests | `KunalSpaz` repo | **Yes** | semanticIr + tests | M1 |
| Few-shot examples from past translations | LangGraph Store + `jkanie` | **Yes** | propose node | M2 |
| Edge-case catalog | `SasBridgePy` | **Yes** | pitfall table §13 | M1 |
| Prompt caching (stable prefix + variable suffix) | DeepSeek + Kimi | **Yes** | propose prompt | M1 |
| Diagnostics tool (pylint/mypy before reconcile) | DeepSeek TUI | **Yes** | post-translate | M1 |
| Auto-review before operator | Open Interpreter | **Yes** | guard node | M0 |
| Todo list synced from state | Deep Agents | **Yes** | React UI | M3 |
| Synthetic setup message | Qwen Code | **Yes** | propose prompt | M0 |
| `apply_patch` marker format | OpenCode + DeepSeek TUI | **Yes** | repair subgraph | M1 |
| Boot compatibility preflight | DSH | **Yes** | startup | M0 |
| Frozen tool args/results | DSH | **Yes** (already designed) | Pydantic + artifacts | M0 |
| PTC containment (Databricks as substrate) | DSH | **Yes** (already designed) | execution service | M1 |
| Hash-chained event log | Our invariant | **Yes** (already designed) | evidence service | M0 |
| SWE-agent discussion/command loop | SWE-agent | **No** (use structured tools) | — | — |
| Open Interpreter multi-harness runtime | Open Interpreter | **No** (single LangGraph runtime) | — | — |
| OpenCode plugins/ecosystem | OpenCode | **No** (keep simple config) | — | — |
| Deep Agents skills/sandbox/permissions | Deep Agents | **No** (keep our guard) | — | — |
