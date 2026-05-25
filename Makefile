# Root Makefile — operator entry point for migrations, build/test, and
# compose lifecycle (ADR 0009).
#
# Override env vars on the command line (`make migrate-up DATABASE_URL=…`)
# or via a gitignored `.env` at the repo root.

# Load .env if present so DATABASE_URL / KAFKA_BOOTSTRAP / Telegram
# creds resolve the same way they do for `docker compose`.
ifneq (,$(wildcard .env))
  include .env
  export
endif

# Defaults mirror internal/config (ADR 0008). When .env doesn't override
# these, `make migrate-up` hits the same localhost DSN that the services
# resolve to when run on the host.
DATABASE_URL    ?= postgres://jobtracker:jobtracker@localhost:5432/jobtracker?sslmode=disable
KAFKA_BOOTSTRAP ?= localhost:9092

MIGRATIONS_DIR  := internal/db/migrations
COMPOSE         ?= podman-compose

# Prefer a locally-installed `migrate` binary; fall back to the
# golang-migrate container image via podman so a contributor without
# the CLI on PATH still gets a working `make migrate-*`. --network=host
# lets the container reach a host-bound Postgres on Linux; on macOS,
# install the binary (`brew install golang-migrate`).
MIGRATE_LOCAL := $(shell command -v migrate 2>/dev/null)
ifdef MIGRATE_LOCAL
  MIGRATE_DB  := $(MIGRATE_LOCAL) -path $(MIGRATIONS_DIR) -database "$(DATABASE_URL)"
  MIGRATE_FS  := $(MIGRATE_LOCAL)
else
  MIGRATE_DB  := podman run --rm --network=host \
                   -v $(CURDIR)/$(MIGRATIONS_DIR):/migrations \
                   docker.io/migrate/migrate:latest \
                   -path /migrations -database "$(DATABASE_URL)"
  MIGRATE_FS  := podman run --rm \
                   -v $(CURDIR)/$(MIGRATIONS_DIR):/migrations \
                   docker.io/migrate/migrate:latest
endif

# Timestamp format matching ADR 0009: YYYYMMDDHHMMSS_<slug>.up.sql.
MIGRATE_TS_FORMAT := 20060102150405

.PHONY: help \
  migrate-up migrate-up-one migrate-down-one migrate-down-all \
  migrate-version migrate-force migrate-new \
  build test fmt vet tidy \
  up down logs restart ps

help:
	@echo "Migrations:"
	@echo "  migrate-up         apply all pending migrations"
	@echo "  migrate-up-one     apply the next pending migration"
	@echo "  migrate-down-one   revert the most recently applied migration"
	@echo "  migrate-down-all   revert every applied migration"
	@echo "  migrate-version    print the current schema_migrations version"
	@echo "  migrate-force      mark VERSION as applied without running it (e.g. VERSION=20260525110139)"
	@echo "  migrate-new        create a NAME=<slug> migration pair"
	@echo ""
	@echo "Build & test:    build  test  fmt  vet  tidy"
	@echo "Compose:         up  down  logs  restart  ps"

# ----- Migrations -----------------------------------------------------

migrate-up:
	$(MIGRATE_DB) up

migrate-up-one:
	$(MIGRATE_DB) up 1

migrate-down-one:
	$(MIGRATE_DB) down 1

migrate-down-all:
	$(MIGRATE_DB) down -all

migrate-version:
	$(MIGRATE_DB) version

migrate-force:
ifndef VERSION
	$(error VERSION is required, e.g. `make migrate-force VERSION=20260525110139`)
endif
	$(MIGRATE_DB) force $(VERSION)

migrate-new:
ifndef NAME
	$(error NAME is required, e.g. `make migrate-new NAME=add_jobs_owner`)
endif
ifdef MIGRATE_LOCAL
	$(MIGRATE_FS) create -ext sql -dir $(MIGRATIONS_DIR) -format $(MIGRATE_TS_FORMAT) $(NAME)
else
	$(MIGRATE_FS) create -ext sql -dir /migrations -format $(MIGRATE_TS_FORMAT) $(NAME)
endif

# ----- Build & test ---------------------------------------------------

build:
	go build ./...

test:
	go test ./...

fmt:
	go fmt ./...

vet:
	go vet ./...

tidy:
	go mod tidy

# ----- Compose lifecycle ----------------------------------------------

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

restart:
	$(COMPOSE) restart

ps:
	$(COMPOSE) ps
