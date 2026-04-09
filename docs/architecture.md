# Architecture Overview

This public repository keeps the system structure visible while omitting sensitive production details.

## Main Components

- `core/`
  Rust runtime for signal handling, filtering, execution orchestration, exits, alerting, and state transitions.
- `control/`
  TypeScript control plane for config loading, runtime control, admin endpoints, and supporting data aggregation.
- `integrations/telegram_tracker/`
  Python integration for Telegram-based signal intake and event forwarding.
- `infra/`
  Local Docker and database bootstrap assets.

## Event Flow

1. An integration worker observes an external signal.
2. The worker normalizes that signal into a trade intent or alert event.
3. Redis is used for queueing, control flags, and shared runtime state.
4. The Rust runtime consumes intents, applies filters and routing logic, and manages position lifecycle.
5. The control plane exposes operational visibility and control surfaces around the runtime.

## Public-Safe Scope

The production system includes additional configuration, integrations, and decision logic that are intentionally not documented in detail here. This version is meant to demonstrate:

- service decomposition
- cross-language boundaries
- queue-driven architecture
- operational controls
- provider normalization patterns
- maintainable code organization

It is not intended to expose live credentials, tracked entities, or copy-paste trading logic.
