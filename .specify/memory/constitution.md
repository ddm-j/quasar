<!--
SYNC IMPACT REPORT
==================
Version change: 0.0.0 → 1.0.0 (MAJOR: Initial constitution ratification)

Modified principles: N/A (initial creation)

Added sections:
  - Core Principles (6 principles)
  - Technology Constraints (stack requirements)
  - Development Workflow (commit, review, CI)
  - Governance (amendment process)

Removed sections: N/A (initial creation)

Templates requiring updates:
  - .specify/templates/plan-template.md: ✅ Updated (Constitution Check now references all 6 principles)
  - .specify/templates/spec-template.md: ✅ Compatible (requirements structure aligns)
  - .specify/templates/tasks-template.md: ✅ Compatible (phase structure supports principles)

Follow-up TODOs: None
-->

# Quasar Constitution

## Core Principles

### I. Provider Abstraction

All external data sources and broker integrations MUST be implemented as providers
inheriting from the established ABC hierarchy (`DataProvider`, `HistoricalDataProvider`,
`LiveDataProvider`). Providers MUST:

- Register via the `@register_provider` decorator
- Declare class-level `RATE_LIMIT` and `CONCURRENCY` constraints
- Use async context managers for session and resource lifecycle
- Implement streaming via `AsyncIterator` for memory-efficient data handling
- Be independently testable with mocked dependencies

**Rationale**: Uniform provider interface enables hot-swappable data sources, consistent
rate limiting, and predictable resource cleanup across all integrations.

### II. Async-First Architecture

All I/O-bound operations MUST use async/await patterns. This includes:

- Database operations via asyncpg with connection pooling
- HTTP requests via aiohttp sessions
- WebSocket connections with proper lifecycle management
- Scheduled jobs via APScheduler's async executor

Sync blocking calls in async contexts are prohibited except where explicitly justified
(e.g., CPU-bound cryptographic operations).

**Rationale**: Async-first ensures the platform can handle concurrent data streams,
multiple provider connections, and high-frequency scheduling without thread exhaustion.

### III. Constructor-Based Dependency Injection

Services and handlers MUST receive dependencies through their constructors rather than
importing global singletons. Key patterns:

- `DatabaseHandler` accepts `dsn` or existing `pool`
- `DataProvider` receives `DerivedContext` for secrets
- Services accept injected pools, stores, and clients

This enables complete mock injection in tests without monkey-patching.

**Rationale**: Explicit dependency injection produces testable, loosely-coupled code
where behavior can be verified in isolation.

### IV. Type-Safe Boundaries

Internal data transfer MUST use lightweight type-safe structures (`TypedDict`,
`NamedTuple`, `dataclass`). Pydantic models are reserved for API request/response
validation at service boundaries.

- `Bar`, `SymbolInfo`, `Req` use TypedDict/NamedTuple internally
- FastAPI endpoints use Pydantic schemas in `services/*/schemas.py`
- Generated enums from YAML are the single source of truth for categorical values

**Rationale**: Lightweight DTOs minimize serialization overhead in hot paths while
Pydantic provides validation where untrusted input crosses service boundaries.

### V. Test Infrastructure First

Test infrastructure MUST be centralized and consistent:

- All fixtures reside in `tests/conftest.py`
- Mock pools, stores, and contexts are reusable across test modules
- `TestClient` fixtures (`datahub_client`, `registry_client`) enable API testing
- Coverage excludes example providers (not production code)

New features MUST have corresponding test coverage. PRs reducing coverage require
explicit justification.

**Rationale**: Centralized fixtures reduce test boilerplate and ensure consistent
mocking strategies. High coverage catches regressions before production.

### VI. Simplicity Over Abstraction

Prefer concrete implementations over premature abstraction:

- YAGNI: Do not build features until needed
- Three similar blocks of code is acceptable before extracting a utility
- Avoid indirection (wrappers, facades) unless solving a concrete problem
- Configuration over code: use YAML/env for values that vary by deployment

When complexity is introduced, document the specific problem it solves in the PR.

**Rationale**: Over-engineered code is harder to understand, debug, and modify.
Complexity must pay for itself with measurable benefit.

## Technology Constraints

The following stack decisions are non-negotiable for core platform development:

| Layer | Technology | Version/Notes |
|-------|------------|---------------|
| Backend Language | Python | 3.12+ required |
| API Framework | FastAPI | Async endpoints |
| Database | PostgreSQL + TimescaleDB | 17+ with hypertables |
| DB Driver | asyncpg | Connection pooling |
| Scheduling | APScheduler | Async job store |
| Frontend | React + Vite | CoreUI Pro components |
| State | Redux | Frontend state management |
| Container | Docker Compose | Local and production |

Introducing alternative technologies (e.g., different ORM, sync framework, NoSQL)
requires a constitution amendment with migration plan.

## Development Workflow

### Commits

- One commit = one logical change
- Codebase MUST be in working state after each commit
- Message format: `<type>(<scope>): <description>`
- Types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`

### Code Review

- All changes to `main` go through pull requests
- Self-review is acceptable for solo work but PR still required
- CI checks (tests, enum drift, linting) MUST pass before merge

### Continuous Integration

- `pytest` runs full test suite
- `make enums` regenerates enums; `git diff --exit-code` catches drift
- Coverage uploaded to Codecov; significant drops require justification

### Documentation

- Google-style docstrings for public APIs (see `docs/contributing/docstrings.md`)
- Architecture diagrams in `docs/architecture.md`
- Pattern reference in `.claude/docs/architectural_patterns.md`

## Governance

This constitution supersedes informal practices. All contributions MUST comply.

### Amendment Process

1. Propose change via GitHub issue with rationale
2. Draft amendment with version bump justification
3. Update dependent templates if principles change
4. Merge requires explicit approval

### Version Policy

- **MAJOR**: Principle removed, redefined, or backward-incompatible governance change
- **MINOR**: New principle added, section materially expanded
- **PATCH**: Clarifications, wording, typo fixes

### Compliance

- PRs SHOULD reference relevant principles when applicable
- Plan-template includes Constitution Check gate
- Violations require documented justification in Complexity Tracking

**Version**: 1.0.0 | **Ratified**: 2026-01-17 | **Last Amended**: 2026-01-17
