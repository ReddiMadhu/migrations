# ADR-010: Identity-bound approval

**Status:** Accepted

## Problem
An action-level “allow once” is stale if code, policy, input, or environment changes before execution.

## Options
Session-wide approval; UI-only warning; runtime binding to full execution identity.

## Decision
Bind approval to semantic/source hashes, policy, environment, input snapshot, action, tenant, migration, and candidate. Validate lazily at execution. This is intentionally stricter than DSH's ephemeral approval grant, which does not inherently bind all of these identities.

## Why
Runtime validation is the safety boundary; UI can only warn early.

## Tradeoffs
Approvals become less reusable and require clear invalidation UX.

## Failure modes
TOCTOU mutation, incorrect environment fingerprint, policy substitution, missing exact action.

## Reversal cost
Medium; stored approval records can be migrated, but weakening the binding is a security regression.
