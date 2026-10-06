# Workspace discovery

`WorkspaceDiscoveryService` is the first domain component that turns a bounded set of SAS source artifacts into a dependency graph and a `MigrationWorkUnit` closure.

It is deliberately not a general filesystem crawler. The caller supplies logical workspace paths and already-registered artifacts. The service verifies every artifact against the requested scope, reads only the small SAS source artifacts, and never treats logical paths as local filesystem paths.

## API

```ts
const graph = await runtime.ctx.workspace.discover({
  session,
  scope: candidateScope,
  discoveryId: 'discovery-claims',
  files: [
    { path: 'lib/claims.sas', artifact: claimsArtifact },
    { path: 'lib/common.sas', artifact: commonArtifact },
  ],
  rootPaths: ['lib/claims.sas'],
})

const workUnit = await runtime.ctx.workspace.createWorkUnit({
  session,
  scope: candidateScope,
  graph,
  rootPath: 'lib/claims.sas',
  workUnitId: 'wu-claims-01',
  requiredSkills: ['sas-by-group'],
})
```

`discover()` validates the input inventory and emits:

```text
workspace/discovered
dependency/graph-created
```

`createWorkUnit()` fails closed if the selected root's reachable graph has unresolved includes or a cycle. The resulting work unit contains a sorted dependency closure excluding the root module itself. The same closure calculation is available as `dependencyClosure(graph, rootPath)` for diagnostics.

## Static include parsing

The first implementation extracts case-insensitive `%include` directives from SAS source text:

```sas
%include "lib/common.sas";
%include dates.sas;
```

It resolves quoted and unquoted targets relative to the including file and optionally adds the `.sas` extension. It ignores include-looking text inside block comments, `//` comments, `%*` comments, and quoted strings.

Each edge records:

- source logical path
- target logical path
- `include` kind
- source line
- normalized source expression

Unresolved includes are retained in the graph as durable evidence with a reason:

- `NOT_FOUND`
- `INVALID_PATH`
- `AMBIGUOUS`

Cycles are also retained as normalized path sequences.

## Bounds

The current discovery limits are:

- 512 workspace files
- 2 MiB per SAS source artifact
- 32 MiB total SAS source bytes
- 4,096 dependency edges
- 1,024 unresolved dependency records

The resulting work unit still applies the stricter `MigrationWorkUnit` limits of 256 dependencies, 512 IR node IDs, and 64 required skills.

## Replay

The migration projection exposes:

```ts
session.projection.dependencyGraphs.get(graphId)
```

The graph is a complete snapshot in the `dependency/graph-created` event. Replay validates the event's migration and scope ancestry before accepting the graph. Forked sessions inherit the graph as read-only historical evidence; a child repair must create a new graph/work unit in its own scope if the source inventory changes.

## Current limitations

This slice discovers static `%include` dependencies only. It does not yet:

- execute SAS macro expansion
- resolve autocall libraries or environment search paths
- infer dependencies from `CALL EXECUTE` or generated code
- perform complete semantic/data-flow analysis
- connect to Databricks manifests
- read the large reference dataset

Those capabilities should be added as separate deterministic providers while preserving this bounded discovery contract.
