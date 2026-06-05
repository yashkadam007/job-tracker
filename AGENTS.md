# AGENTS.md

## Scope

Single Go module (`go.mod`, Go 1.25.0) for an event-sourced job tracker. Producers publish Kafka events; consumers update Postgres, schedule reminders, and notify via stdout/Telegram. Read the relevant ADR in `docs/adr/` before changing event contracts, schema/migrations, consumer error handling, config, or TUI behavior.

## Layout

- `cmd/cli`, `cmd/jobtracker`, `cmd/bot`: producer entrypoints. Use `internal/jobclient` for publishing.
- `cmd/store`, `cmd/scheduler`, `cmd/notifier`: consumer/service entrypoints.
- `internal/events`: Kafka topics, event structs, allowed values, status transitions.
- `internal/jobclient`: shared publisher/reader and producer-side validation.
- `internal/db`: pgx connection helpers, `LISTEN/NOTIFY`, idempotency helpers, migrations.
- `internal/store`, `internal/scheduler`: Postgres write/reminder domain logic.
- `internal/tui`, `internal/bot`, `internal/telegram`, `internal/config`, `internal/consumeradmin`: feature/shared support packages.

## Commands

Run from the repository root.

- Dependencies: `go mod download`
- Build: `make build` (`go build ./...`)
- Test all packages: `make test` (`go test ./...`)
- Test one package: `go test ./internal/events`
- Test one case: `go test ./internal/events -run TestCanTransition`
- Format: `make fmt` (`go fmt ./...`)
- Vet: `make vet` (`go vet ./...`)
- Tidy modules: `make tidy` (`go mod tidy`)
- Start/stop local stack: `make up` / `make down`
- Compose status/logs: `make ps` / `make logs`
- Apply migrations: `make migrate-up`
- Create a migration pair: `make migrate-new NAME=<slug>`
- Create Kafka topics after the stack is up: `go run ./cmd/cli ensure-topics`

No `.github` workflows, separate lint command, typecheck command, integration-test runner, or e2e-test runner exists in this checkout.

## Architecture Rules

- Keep producers on `internal/jobclient.Publisher`; do not write job state directly from producers.
- Keep shared Kafka wire contracts in `internal/events`. `job_id` is the stable job identity; URL is metadata.
- Keep producer validation in `internal/jobclient`; allowed enum-like values live in `internal/events`.
- Store writes must claim `(consumer, event_id)` in `processed_events` in the same transaction as the business write.
- Services do not run migrations on startup. Schema changes go through `internal/db/migrations` and `make migrate-up`.
- `cmd/jobtracker` reads Postgres directly and refreshes through Postgres `LISTEN/NOTIFY`; there is no API tier.
- Preserve `internal/config` env handling: services use unprefixed env vars, while `cmd/jobtracker` uses `JOB_TRACKER_`.
- Preserve ADR 0006 consumer behavior in `store` and `scheduler`: retry infra errors, skip/count permanent bad messages, crash on unclassified errors.

## Change Rules

- For status or allowed-value changes, update the Go constants/slices, validation, database constraints/migrations, CLI/TUI/bot inputs, and tests together.
- Create migrations only with `make migrate-new NAME=<slug>`. Do not edit already-applied migrations; add a new migration.
- Do not run destructive migration targets (`migrate-down-all`, `migrate-force`) or destructive Git commands unless explicitly requested.
- Do not commit `.env`, Telegram tokens/chat IDs, real database credentials, or personal job/application data. `.env` is gitignored; `.env.example` is the template.
- `go.sum` is maintained by Go module commands; do not hand-edit it.
- Protect unrelated uncommitted work. Check `git status --short` before editing and keep changes scoped to the request.

## Validation

- Go behavior change: run `go test ./...` unless clearly impractical.
- Dependency change: run `go mod tidy` and `go test ./...`.
- Schema change: include up/down migrations and run `make migrate-up` against a local/disposable DB when possible.
- Kafka/consumer workflow change: run relevant unit tests and document any manual smoke test with `make up`, `make migrate-up`, `go run ./cmd/cli ensure-topics`, and affected services.
- Final response must list the checks actually run. Do not claim skipped or unavailable checks passed.
