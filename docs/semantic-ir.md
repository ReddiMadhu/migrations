# Bounded SAS semantic IR

`SemanticIrService` produces a deterministic structural IR snapshot from a discovered SAS dependency graph. It is the first bounded source-context layer that can be attached to a `MigrationWorkUnit`.

The IR is intentionally structural. It is not a claim that the parser understands all SAS semantics, and it is not a translation engine.

## API

```ts
const ir = await runtime.ctx.semanticIr.create({
  session,
  scope: candidateScope,
  graph,
  rootPath: 'lib/claims.sas',
  irId: 'ir-claims-01',
})

const workUnit = await runtime.ctx.semanticIr.createWorkUnit({
  session,
  scope: candidateScope,
  ir,
  rootPath: 'lib/claims.sas',
  workUnitId: 'wu-claims-01',
  irNodeIds: [
    'ir-claims-01-n00007',
    'ir-claims-01-n00008',
  ],
})
```

The service reads only the small SAS source artifacts represented by the graph. It does not read the Databricks reference dataset.

## Snapshot shape

A durable IR snapshot contains:

- `irId`
- source `graphId`
- migration and scope identity
- selected `rootPath`
- parser version
- artifact-backed source file inventory
- structural nodes and source spans
- bounded diagnostics

Node kinds currently include:

- `program`
- `data-step`
- `procedure`
- `macro`
- `include`
- `operation`
- `statement`

Each node has a stable ID within the snapshot, a name, a source span, bounded attributes, and child node IDs.

Spans use one-based line and column numbers. Offsets are zero-based, half-open character ranges:

```json
{
  "path": "lib/claims.sas",
  "startLine": 4,
  "endLine": 4,
  "startColumn": 1,
  "endColumn": 18,
  "startOffset": 82,
  "endOffset": 99
}
```

The current parser recognizes structural statements such as:

- `data ... run;`
- `proc ... run;`
- `%macro ... %mend;`
- `%include ...;`
- `set`, `merge`, `join`, `by`, `where`, `keep`, `drop`, `sort`, `group`, `summary`, and related operations

Comments and quoted text are masked before directive recognition.

## Durability and bounds

The service appends:

```text
semantic-ir/created
```

The snapshot is replayed into:

```ts
session.projection.semanticIrs.get(irId)
```

The initial limits are:

- 512 IR files
- 2,048 IR nodes
- 256 diagnostics
- 32 attributes per node
- 2 MiB per SAS source artifact
- 32 MiB total source bytes

A work unit can reference at most 512 IR node IDs. `ctx.semanticIr.createWorkUnit()` attaches both `irSnapshotId` and the selected node IDs, and verifies that the snapshot belongs to the current migration/scope and dependency graph.

## Current limitations

This is a bounded structural parser, not a full SAS front end. It does not yet:

- expand macros
- resolve autocall libraries or generated code
- infer data-flow or type semantics
- build a complete query-plan representation
- translate SAS to PySpark
- decide semantic equivalence
- access Databricks tables

Those capabilities should be added as separate deterministic providers. The work-unit context should continue to reference bounded node IDs and source spans rather than copying the full source workspace or any large dataset into model context.
