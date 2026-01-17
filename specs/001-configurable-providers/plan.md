# Implementation Plan: Configurable Providers

**Branch**: `001-configurable-providers` | **Date**: 2026-01-17 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/001-configurable-providers/spec.md`

## Summary

Enable runtime configuration of provider behavior without code changes. This includes:
- **Scheduling**: Historical providers get configurable delay (0-24h), live providers get configurable pre/post close buffers
- **Data**: Historical providers get configurable lookback period (1-8000 days) for new subscriptions
- **Credentials**: Update API secrets without re-uploading provider code

The implementation leverages the existing `preferences` JSONB column in `code_registry`, extends provider base classes with `CONFIGURABLE` schema declarations, adds validation at the API layer, and integrates preferences into DataHub's scheduling and data collection flows.

## Technical Context

**Language/Version**: Python 3.12+, JavaScript (React 18)
**Primary Dependencies**: FastAPI, asyncpg, APScheduler, aiohttp, CoreUI Pro, Redux
**Storage**: PostgreSQL 17 with TimescaleDB; existing `preferences` JSONB column in `code_registry`
**Testing**: pytest with async fixtures from `tests/conftest.py`
**Target Platform**: Docker Compose (Linux containers), web browser
**Project Type**: Web application (Python backend + React frontend)
**Performance Goals**: Config UI interactions < 30 seconds; job scheduling within 60-second tolerance
**Constraints**: Single-user system; backward compatibility with existing providers
**Scale/Scope**: ~10 providers, ~1000 symbols; config changes are infrequent operations

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Review against `.specify/memory/constitution.md` principles:

- [x] **I. Provider Abstraction**: Feature adds `CONFIGURABLE` to ABC hierarchy; providers receive preferences via constructor; no changes to `@register_provider` mechanism
- [x] **II. Async-First**: All new DB operations use asyncpg async patterns; Registry↔DataHub communication via async HTTP
- [x] **III. Constructor DI**: Preferences passed to provider constructor as optional parameter; no global state
- [x] **IV. Type-Safe Boundaries**: Internal preferences use dict; Pydantic schemas at API edges for request/response validation
- [x] **V. Test Infrastructure**: Tests will use existing `registry_client`/`datahub_client` fixtures; new tests in `tests/`
- [x] **VI. Simplicity**: Schema declared in base classes (not dynamic); JSONB storage (no new tables); leverages existing `OffsetCronTrigger`

*No constitution violations. All principles satisfied.*

## Project Structure

### Documentation (this feature)

```text
specs/001-configurable-providers/
├── plan.md              # This file
├── research.md          # Phase 0 output - implementation decisions
├── data-model.md        # Phase 1 output - entity definitions
├── quickstart.md        # Phase 1 output - developer guide
├── contracts/           # Phase 1 output - API specifications
│   └── openapi.yaml     # Registry and DataHub endpoints
└── tasks.md             # Phase 2 output - implementation tasks
```

### Source Code (repository root)

```text
quasar/
├── lib/
│   └── providers/
│       └── core.py                    # Add CONFIGURABLE to base classes
├── services/
│   ├── registry/
│   │   ├── handlers/
│   │   │   └── config.py              # Add schema, secret-keys, secrets endpoints
│   │   └── schemas.py                 # Extend Pydantic models
│   └── datahub/
│       ├── handlers/
│       │   ├── providers.py           # Fetch preferences on load, add unload endpoint
│       │   └── collection.py          # Apply preferences to scheduling/lookback
│       └── utils/
│           └── constants.py           # Reference only (defaults stay here)
└── tests/
    ├── test_configurable_providers.py # New unit tests
    └── test_config_integration.py     # New integration tests

web/
└── src/
    ├── views/
    │   └── registry/
    │       └── ProviderConfigModal.js # Add Scheduling, Data tabs; implement API Secrets
    └── services/
        └── registry_api.js            # Add new endpoint calls
```

**Structure Decision**: Web application pattern. Backend changes span `lib/providers` (schema declaration), `services/registry` (API endpoints), and `services/datahub` (preference consumption). Frontend changes isolated to existing `ProviderConfigModal.js` component.

## Complexity Tracking

> **No constitution violations identified.**

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| N/A | N/A | N/A |

## Implementation Phases

Per the user's detailed specifications in `config-feature/`, the implementation follows five phases:

### Phase 1: Schema Infrastructure
1. Add `CONFIGURABLE` declarations to `DataProvider`, `HistoricalDataProvider`, `LiveDataProvider`, `IndexProvider`
2. Create schema lookup utility in Registry service (maps `class_subtype` → `CONFIGURABLE`)
3. Add `GET /api/registry/config/schema` endpoint
4. Enhance `PUT /api/registry/config` with provider-type-specific validation

### Phase 2: DataHub Preferences Integration
1. Fetch preferences in `load_provider_cls()` alongside provider loading
2. Pass preferences to provider constructor (optional `preferences: dict | None` parameter)
3. Store in `_provider_preferences` dict for runtime access
4. Add `POST /api/datahub/providers/{name}/unload` endpoint

### Phase 3: Scheduling Configuration
1. Historical: Read `delay_hours` from preferences, apply to `OffsetCronTrigger` in `refresh_subscriptions()`
2. Live: Read `pre_close_seconds`, apply as negative offset to `OffsetCronTrigger`
3. Live: Read `post_close_seconds`, use in timeout calculation in `get_data()`
4. Add Scheduling tab to UI with provider-type-specific content

### Phase 4: Data Configuration
1. Read `lookback_days` in `_build_reqs_historical()` instead of `DEFAULT_LOOKBACK`
2. Add Data tab to UI with preset radio buttons + custom input

### Phase 5: API Credentials
1. Add `GET /api/registry/config/secret-keys` endpoint (returns key names, not values)
2. Add `PATCH /api/registry/config/secrets` endpoint (re-encrypts credentials)
3. Call DataHub unload endpoint after credential update
4. Replace "Coming Soon" placeholder in API Secrets tab with dynamic password fields

## Key Technical Decisions

These decisions are documented in the user's `config-feature/` specifications:

1. **Schema in base classes only**: User-uploaded code cannot declare new preference fields (security)
2. **JSONB merge for updates**: Existing `PUT /api/registry/config` uses `jsonb_strip_nulls(COALESCE(...) || ...)` pattern
3. **OffsetCronTrigger for scheduling**: Existing infrastructure supports positive (delay) and negative (pre-close) offsets
4. **All-or-nothing credential updates**: Simplifies implementation; user provides complete secret set
5. **DataHub unload via API call**: Registry calls `POST /api/datahub/providers/{name}/unload` (service separation)

## Migration & Compatibility

- **No database migration required**: Existing `preferences` JSONB column accommodates new fields
- **Backward compatible**: New preferences have defaults matching current hardcoded values
- **Provider code unchanged**: Optional `preferences` parameter in constructor; existing providers work as-is
