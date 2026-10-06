# ADR-001: Use upstream Cordis as runtime foundation

**Status:** Accepted

## Problem
The workbench needs dependency injection, plugin lifecycle, effects, child contexts, and typed events without a second homemade framework.

## Options
1. DeepSeek Harness packages.
2. Fork/vendor DeepSeek’s modified Cordis.
3. Depend directly on upstream Cordis.
4. Build a custom runtime.

## Decision
Use exact upstream `cordis@4.0.0-rc.10` directly. Domain services extend Cordis `Service`; plugins use `inject` and `ctx.effect`. DeepSeek source is an architectural reference only.

## Why
It satisfies the product requirement, keeps dependency provenance clear, and avoids coupling business capabilities to a developer-agent distribution.

## Tradeoffs
Cordis 4 remains an RC. We pin the version and regression-test composition upgrades. A custom scope/evidence layer is still needed because those are migration-domain semantics, not Cordis primitives.

## Failure modes
RC API drift; accidental direct provider imports; overusing process-local registry as distributed authority.

## Reversal cost
Moderate. The runtime API is small, but every service/plugin would need porting.
