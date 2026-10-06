# Static SAS macro resolution

`MacroResolutionService` provides a bounded, deterministic first pass over static SAS macro definitions and calls. It is evidence for diagnosis and prompt construction; it is not a full SAS preprocessor or macro expander.

## API

```ts
const resolution = await runtime.ctx.macroResolution.create({
  session,
  scope: candidateScope,
  graph,
  rootPath: 'lib/claims.sas',
  resolutionId: 'macro-claims-01',
})

const workUnit = await runtime.ctx.macroResolution.createWorkUnit({
  session,
  scope: candidateScope,
  resolution,
  rootPath: 'lib/claims.sas',
  ir,
  irNodeIds: ['ir-claims-01-n00004'],
  workUnitId: 'wu-claims-01',
})
```

The service reads only the small SAS source artifacts in the verified include closure. It does not expand macros into executable code and does not read Databricks data.

## Snapshot contents

A durable snapshot contains:

- `resolutionId`
- graph/migration/scope/root identity
- resolver version
- source file inventory
- macro definitions and parameter lists
- static macro call spans
- resolved, unresolved, and ambiguous statuses
- unresolved evidence with source spans
- bounded diagnostics

Calls are case-insensitive. A call resolves only when exactly one definition exists in the graph closure:

```text
zero definitions  → UNRESOLVED
one definition    → RESOLVED
multiple           → AMIGUOUS
```

Unknown calls are retained as evidence rather than silently treated as built-in macros. The current resolver recognizes a bounded set of common SAS directives/functions and skips them as control directives; it does not claim to know every built-in macro.

Dynamic names such as:

```sas
%&generated_name;
```

produce a diagnostic and require a future resolver.

## Work-unit and model-context linkage

A work unit can carry an immutable:

```ts
macroResolutionId?: string
```

The resolution does not widen the dependency closure. `ctx.modelContext.assemble()` exposes only bounded macro counts and unresolved call names/spans, not macro bodies or generated code.

## Current bounds

- 256 macro definitions
- 2,048 macro calls
- 512 unresolved evidence entries
- 256 diagnostics
- 64 parameters per definition
- 1,024 characters per bounded argument/diagnostic string
- 2 MiB per SAS source artifact
- 32 MiB total source bytes

## Current limitations

The resolver does not yet:

- expand macro parameters or substitute `&variables`
- resolve autocall libraries
- execute generated code
- perform semantic/data-flow analysis
- infer SAS built-in macro semantics
- query Databricks

Autocall and generated-code behavior must remain visible unresolved evidence until a separate deterministic provider can prove it.
