## SOLANA BOT

This Bot is a Solana trading bot built across Rust, TypeScript, and Python, designed around an event-driven architecture for signal ingestion, execution orchestration, state management, and operator control.

This repository presents the system in a public-safe format while preserving the core architecture, implementation quality, and engineering structure. **For more information or access to the complete version, reach out directly.**

### Summary

- Event-driven runtime for ingesting external signals and managing trade lifecycle
- Rust hot path for filtering, state transitions, execution orchestration, and exits
- TypeScript control plane for configuration, operator controls, and supporting data services
- Python integration layer for Telegram-based signal intake
- Redis-backed coordination between services

### Repository Layout

```text
core/                         Rust runtime and execution pipeline
control/                      TypeScript control plane and operator endpoints
integrations/telegram_tracker/ Telegram signal ingestion utilities
infra/                        Local development services and schema bootstrap
config/                       Public-safe example configuration
docs/                         Architecture notes
scripts/                      Utility scripts for local development
```

### Architecture

At a high level, the system separates ingestion, control, and execution responsibilities:

1. Integration workers observe external signals.
2. Signals are normalized into intents and alerts.
3. Redis is used for queueing, shared state, and runtime control flags.
4. The Rust runtime consumes intents and manages lifecycle decisions.
5. The control plane exposes operational visibility and admin controls.

See [docs/architecture.md](/Users/claudio/Documents/Blockchain Coding/solana_bot copy/docs/architecture.md) for the public-safe architecture overview.

### Public Scope

This repository intentionally withholds or simplifies:

- credentials and operational infrastructure details
- tracked wallets, channels, and private datasets
- proprietary trading rules, thresholds, and sizing logic
- private analysis and decision-service contracts

### Local Setup

The repository includes safe templates and stripped-down example configs for code exploration.

1. Copy:
   - `core/env.example` to `core/.env`
   - `control/env.example` to `control/.env`
2. Review the public-safe config files under `config/`
3. Optionally start local dependencies:

```bash
docker compose -f infra/docker-compose.yml up -d
```

4. Run the prereq check:

```bash
bash scripts/check-prereqs.sh
```

5. Start services for local exploration:

```bash
cd control && npm install && npm run dev
```

```bash
cd core && cargo run -p core-app
```

### Notes

- Runtime examples default to non-production-oriented settings.
- Some implementation areas remain intentionally simplified in the public version.
- Local integration config is excluded from version control.
