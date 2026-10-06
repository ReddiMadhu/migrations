# ADR-013: Replaceable advisory model providers

**Status:** Accepted

## Problem
Migration semantics, reconciliation, approval, and certificates must not change when the model vendor changes.

## Options
Vendor SDK in domain services; one LLM abstraction; provider-neutral LLM service plus replay adapter.

## Decision
Define a provider-neutral advisory LLM service and record provider/model/request/response metadata as evidence. A deterministic replay provider can reproduce tests and incidents. Milestone 1 deliberately mounts no model provider; DeepSeek's `llm-deepseek` and `llm-pi-ai` are examples of replaceable adapters, not authority.

## Why
Model replacement becomes composition rather than a redesign, and replay never needs another model request.

## Tradeoffs
The lowest-common-denominator request schema can lose provider-specific features; unsupported features require explicit capability negotiation.

## Failure modes
Non-deterministic replay, provider metadata omission, prompt drift, hidden model state.

## Reversal cost Moderate; the migration loop and verifier do not change, but adapters must evolve.
