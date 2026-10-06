# ADR-002: The durable event log is authoritative

**Status:** Accepted

## Problem
A mutable migration state object can diverge from the model/operator/audit view and cannot prove what happened.

## Options
Mutable state; notification bus; snapshot-only state; append-only event sourcing with derived projections.

## Decision
Store canonical migration events in an append-only log. Derive migration, attempt, candidate, execution, repair, approval, cost, and certificate projections by deterministic fold.

## Why
It makes trajectory reconstruction, replay, crash repair, and audit independent of live objects.

## Tradeoffs
Projection code, schema migrations, and event-upgrade policy are required. Reads may need projection caches. The log is the canonical session state, but an append is not automatically a distributed durability barrier: providers must flush/checkpoint before side effects, and ambiguous external outcomes remain unknown until reconciled.

## Failure modes
Projection bug, event schema drift, ordering/concurrency mistakes, unbounded growth.

## Reversal cost
Very high; the certificate and audit model depend on this invariant.
