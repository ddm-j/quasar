<!--
SYNC IMPACT REPORT
==================
Version change: 0.0.0 → 1.0.0 (Initial ratification)

Modified principles: N/A (initial creation)

Added sections:
- Core Principles (5 principles)
- Development Standards
- Quality Gates
- Governance

Removed sections: None

Templates validation:
- .specify/templates/plan-template.md: ✅ No constitution-specific references requiring update
- .specify/templates/spec-template.md: ✅ No constitution-specific references requiring update
- .specify/templates/tasks-template.md: ✅ No constitution-specific references requiring update
- .specify/templates/checklist-template.md: ✅ No constitution-specific references requiring update
- .specify/templates/agent-file-template.md: ✅ No constitution-specific references requiring update

Follow-up TODOs: None
-->

# Quasar Constitution

## Core Principles

### I. Simplicity First

Every change MUST solve the immediate problem with minimal complexity. Code MUST NOT anticipate hypothetical future requirements.

- No abstractions for single-use operations
- No feature flags or backwards-compatibility shims when direct changes suffice
- Three similar lines of code are preferable to a premature abstraction
- Delete unused code completely; avoid `_unused` renames or `// removed` comments
- YAGNI (You Aren't Gonna Need It) governs all design decisions

**Rationale**: Complexity compounds over time. Each unnecessary abstraction increases cognitive load and maintenance burden. The simplest solution that works is the correct solution.

### II. Provider Abstraction

All external data sources MUST be accessed through the provider interface pattern. Providers MUST be self-contained and independently testable.

- Providers inherit from `HistoricalDataProvider` or `LiveDataProvider`
- Registration via `@register_provider` decorator
- Each provider MUST define: `name`, rate limits, concurrency limits
- Providers receive dependencies through `DerivedContext` constructor injection
- No direct external API calls outside the provider layer

**Rationale**: The platform's value lies in aggregating multiple data sources. A consistent provider contract enables new sources to be added without modifying core logic.

### III. Async Resource Safety

All I/O-bound operations MUST use async patterns with explicit lifecycle management. Resources MUST be cleaned up even when exceptions occur.

- Use `__aenter__`/`__aexit__` for connection lifecycle (database pools, HTTP sessions, WebSockets)
- Use `AsyncIterator` for streaming data to enable memory-efficient processing
- All database operations go through `asyncpg` pool with proper connection release
- Rate limiting via class-level `AsyncLimiter` and `Semaphore`
- Timeouts MUST be explicit; use the custom timeout decorator for async functions

**Rationale**: Resource leaks in long-running trading services cause degraded performance and eventual failure. Explicit lifecycle management prevents connection exhaustion and memory growth.

### IV. Test with Mocks

Tests MUST isolate the unit under test by mocking external dependencies. Test fixtures MUST be centralized and reusable.

- Fixtures live in `tests/conftest.py`
- Mock database connections with `mock_asyncpg_pool` and `mock_asyncpg_conn`
- Mock secrets with `mock_secret_store` and `mock_system_context`
- Use `*_with_mocks` fixtures for fully-mocked service instances
- Use `*_client` fixtures with FastAPI `TestClient` for API testing
- Tests MUST pass without network access or database connections

**Rationale**: Fast, deterministic tests enable confident refactoring. Flaky tests that depend on external services erode trust in the test suite.

### V. Atomic Changes

Each commit MUST represent one logical change that leaves the codebase in a working state. Commits MUST NOT mix unrelated modifications.

- One commit = one logical change (describable without using "and")
- Tests MUST pass before committing
- Use conventional commit format: `<type>(<scope>): <description>`
- Types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`
- Feature branches for all work; `main` is always deployable
- Pull requests required even for solo work (they document decisions)

**Rationale**: Clean commit history enables effective code review, bisection for debugging, and clear understanding of why changes were made.

## Development Standards

### Code Style

- Python: Google-style docstrings, type hints on all public functions
- Docstrings: imperative summary, `Args`, `Returns`, `Raises` sections
- Pydantic models at API boundaries only; use `TypedDict`/`NamedTuple` internally
- Enums generated from YAML via `make enums`; CI checks for drift

### Boundaries

- Services inherit from both `DatabaseHandler` and `APIHandler`
- Secrets accessed only through `SystemContext`/`DerivedContext`
- Configuration via environment variables with explicit defaults
- No hardcoded credentials, URLs, or environment-specific values

## Quality Gates

All contributions MUST satisfy these gates before merge:

1. **Tests pass**: `pytest` completes without failures
2. **Enum sync**: `make enums` produces no diff
3. **Type safety**: No new type errors introduced
4. **Commit hygiene**: Conventional format, atomic changes
5. **Code review**: Self-review minimum; document non-obvious decisions

## Governance

This constitution supersedes all other development practices and guidelines. Compliance is mandatory for all contributions.

### Amendment Procedure

1. Propose change via pull request modifying this file
2. Document rationale for addition, modification, or removal
3. Update version according to semantic versioning:
   - MAJOR: Principle removal or incompatible redefinition
   - MINOR: New principle or material expansion
   - PATCH: Clarification, wording, or non-semantic refinement
4. Propagate changes to dependent templates if principle impacts their content
5. Merge requires explicit acknowledgment of governance change

### Compliance Review

- All pull requests MUST be evaluated against constitution principles
- Complexity MUST be justified when it appears to violate Principle I
- Reviewers SHOULD cite specific principles when requesting changes

**Version**: 1.0.0 | **Ratified**: 2026-01-16 | **Last Amended**: 2026-01-16
