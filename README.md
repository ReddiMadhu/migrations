# Cordis-Native SAS → PySpark Migration & Verification Workbench

> **The model proposes.**  
> **The harness executes.**  
> **The reconciler verifies.**  
> **The event log proves.**  
> **The operator decides when uncertainty remains.**

This repository is an independent workbench built directly on upstream [Cordis](https://github.com/cordiverse/cordis). It does **not** fork DeepSeek Harness and has no `@deepseek-ai/dsh-*` business dependency. DeepSeek Harness is used only as an architectural research reference.

## Current milestone

Milestone 1 implements and tests the smallest proof that matters. Milestone 2 adds independent deterministic reconciliation:

```text
Cordis Context
  → service providers and dependent plugins
  → typed events
  → scope hierarchy
  → immutable content-addressed artifacts
  → durable hash-chained session events
  → bounded MigrationWorkUnit context
  → deterministic SAS include discovery
  → bounded SAS structural IR
  → dummy execution
  → deterministic projection and replay
  → schema/aggregate/row reconciliation with report artifacts
```

Run it:

```bash
pnpm install
pnpm run verify
pnpm run demo          # Milestone 1 execution proof and Milestone 2 reconciliation proof
```

The current `verify` gate is 49 tests across eleven suites, followed by a strict TypeScript build.

## Run the frontend and Python backend

Install the backend from the repository root:

```powershell
cd backend
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
```

Start the backend from the `backend` directory:

```powershell
.\.venv\Scripts\python.exe -m uvicorn --app-dir src main:app --reload --port 8000
```

The `--app-dir src` option is required because the Python package uses a `src/` layout. The ASGI app is exported as `app` by `backend/main.py`, so Uvicorn's `main:app` syntax works when launched from the backend directory. The API docs are available at <http://127.0.0.1:8000/docs>. In another terminal, from the repository root, run `pnpm run frontend:dev` and open <http://localhost:5173>. The frontend development server proxies API requests to the backend.

## Repository map

- [`docs/architecture.md`](docs/architecture.md) — target architecture, invariants, and DeepSeek-derived decisions
- [`docs/deepseek-harness-verification.md`](docs/deepseek-harness-verification.md) — pinned source research and divergences
- [`docs/dsh-target-audit.md`](docs/dsh-target-audit.md) — target `46a7f68…` correction ledger and source classifications
- [`docs/databricks-execution.md`](docs/databricks-execution.md) — current Databricks API/deployment design
- [`docs/state-machine.md`](docs/state-machine.md) — deterministic migration/repair states
- [`docs/work-units.md`](docs/work-units.md) — bounded semantic repair context
- [`docs/workspace-discovery.md`](docs/workspace-discovery.md) — deterministic SAS include graph and closure discovery
- [`docs/semantic-ir.md`](docs/semantic-ir.md) — bounded SAS structural IR and source spans
- [`docs/model-context.md`](docs/model-context.md) — deterministic bounded model-context assembly
- [`docs/macro-resolution.md`](docs/macro-resolution.md) — bounded static macro and autocall evidence
- [`docs/security.md`](docs/security.md) — threat boundaries and controls
- [`docs/milestones.md`](docs/milestones.md) — phased delivery and explicit current limitations
- [`docs/adr/`](docs/adr) — architecture decision records
- [`src/`](src) — Cordis-native domain implementation
- [`test/`](test) — lifecycle, scope, evidence, replay, execution, and reconciliation tests

## Scope of this repository

The current code is an executable foundation, not a claim of full SAS semantic equivalence. Deterministic dataset reconciliation, static SAS `%include` discovery, bounded structural SAS IR, static macro-resolution evidence, and bounded model-context assembly are implemented for small control-plane evidence; macro expansion, complete data-flow semantics, translation, Databricks execution, repair, and certification remain provider contracts and subsequent milestones. The foundation deliberately prevents a premature LLM from entering the authoritative loop.
