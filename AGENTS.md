# AGENTS.md

## Project

Cordis-native SAS → PySpark migration & deterministic verification workbench. The harness executes, the reconciler verifies, the hash-chained event log proves, the operator decides. The LLM is advisory only and never enters the authoritative loop unguarded.

## Active implementation — read first

The backend is a **LangGraph Python** implementation. The TypeScript/Cordis reference has been deleted; Python is the single authoritative implementation.

**Full design spec:** [`docs/langgraph-migration-spec.md`](docs/langgraph-migration-spec.md) — read this before any work. It captures every architectural decision (graph topology, DI, persistence, reconciliation, Databricks provider, LLM/HITL, UI, milestones) and the LangGraph interrupt gotchas.

- Python package lives in `backend/` and is the single implementation.
- LangGraph replaces the Cordis layer in full: DI/service composition, the migration state machine, and the guarded agent/tool runtime.
- Python tooling: `venv` + `pip` with `pyproject.toml`. Do not introduce poetry, uv, or conda.
- Preserve every deterministic invariant below.

## Deterministic invariants (must survive the port)

- **Hash-chained event log**: sessions append ordered events; each event hashes the previous; `GENESIS_HASH` is the chain root. Tamper detection depends on this.
- **Content-addressed artifacts**: SHA-256 over exact bytes; same digest deduplicates; overwrite rejected.
- **Canonical JSON** before hashing/freezing (`canonicalJson`/`snapshotJson`). Python equivalent: `json.dumps(..., sort_keys=True, separators=(",", ":"))`.
- **Frozen inputs/policies**: arguments and policies are immutable before execution. No in-place mutation of event payloads.
- **Deterministic clock**: tests inject a fixed clock; production uses ISO-8601 UTC. Never call `Date.now()` / `time.time()` directly in domain code.
- **Five-step rule**: model proposes → harness executes → reconciler verifies → event log proves → operator decides.

## Historical TypeScript stack (reference deleted — for context only)

The TypeScript/Cordis reference implementation has been deleted from the working tree. The following stack details are retained for historical context only. The active stack is Python (venv + pip + LangGraph + FastAPI) per the Python backend conventions section below.

- Node ≥ 22.16, **pnpm 11.7** pinned. Use `pnpm`, never npm/yarn.
- Cordis `4.0.0-rc.10` (RC, pinned — do not bump without upgrade testing).
- ESM only. Relative imports use `.js` extensions even for `.ts` files (`verbatimModuleSyntax`).
- Entry: `src/index.ts` re-exports the public surface; `src/workbench.ts` `createWorkbench()` composes the runtime.
- `src/sas2pyspark_harness/{evidence,executors,reconciliation,sas,translation}/` are empty reserved dirs for future provider contracts.

### Commands

```bash
pnpm install
pnpm run verify          # check && test && build — the gate; run before claiming done
pnpm run check           # tsc --noEmit (strict typecheck)
pnpm run test            # vitest run
pnpm run test -- -t Name # single test by name
pnpm run build           # tsc -p tsconfig.json
pnpm run demo            # milestone-1 + milestone-2 execution/reconciliation proof
```

Validation order is fixed: `check → test → build` (i.e. `pnpm run verify`).

### tsconfig gotchas

Beyond `strict`: `noUncheckedIndexedAccess` (index access → `T | undefined`), `exactOptionalPropertyTypes` (cannot set optional prop to `undefined`), `useUnknownInCatchVariables`, `noImplicitOverride`, `noFallthroughCasesInSwitch`, `noPropertyAccessFromIndexSignature`, `verbatimModuleSyntax`.

## Tests

- pytest. Tests live in `backend/tests/` with `conftest.py` fixtures (`test_dir`, `fixed_clock`, `workbench`, `make_scopes`, `make_session`).
- Tests run in OS temp dirs and clean up in `finally`/fixtures. Use `tmp_path` pattern.
- Reconciliation tests assert deterministic JSON certification, ordered/unordered row parity, decimal-sum drift, and policy fingerprints.
- Run: `cd backend && PYTHONPATH=src .venv/Scripts/python.exe -m pytest tests/ -q`

## Architecture map

```
Context (Cordis) → services as plugins
  sessions        authoritative hash-chained sessions (JSONL persistence)
  workUnits       bounded repair context
  workspace       deterministic SAS %include discovery
  semanticIr      bounded SAS structural IR + source spans
  macroResolution static macro-resolution evidence
  modelContext    bounded model-context assembly
  artifacts       immutable content-addressed objects (FS provider)
  evidence        hash chain, roots, certificates
  execution       guarded Spark/SAS execution
  reconciliation  deterministic dataset parity
  policy/approval/guard  safety services
```

In `createWorkbench()` consumers are intentionally started before their providers; Cordis fibers stay pending until the provider activates. When porting to LangGraph, model this as graph nodes with explicit dependencies, not eager construction.

## Python backend conventions (target)

- Layout: `backend/` with `pyproject.toml`. Setup (Windows): `python -m venv .venv && .venv\Scripts\activate && pip install -e .`; Unix: `.venv/bin/activate`.
- LangGraph `StateGraph` for the migration state machine; nodes = workbench services, edges = deterministic transitions.
- Keep deterministic helpers pure (hashing, canonical JSON, reconciliation). No I/O inside pure nodes.
- Tests: pytest with `tmp_path`; port `test/helpers.ts` to a `conftest.py` fixture.
- Standardize on venv + pip only.

## Do not

- Do not bump Cordis, ajv, or zod without upgrade testing.
- Do not add npm/yarn lockfiles; this repo is pnpm-only.
- Do not commit real Databricks credentials or live workspace config.
- Do not let the LLM write directly to the event log or certify results unguarded.

## Docs

Design docs in `docs/` (`architecture.md`, `state-machine.md`, `milestones.md`, `security.md`). On conflict between docs and executable code, code wins.
