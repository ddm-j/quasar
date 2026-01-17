# Tasks: Configurable Providers

**Input**: Design documents from `/specs/001-configurable-providers/`
**Prerequisites**: plan.md (required), spec.md (required), research.md, data-model.md, contracts/openapi.yaml, quickstart.md

**Tests**: Tests included as separate tasks per user story phase.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Backend**: `quasar/lib/`, `quasar/services/`
- **Frontend**: `web/src/`
- **Tests**: `tests/`
- **Database**: `db/schema/`

---

## Phase 1: Setup (Environment Preparation)

**Purpose**: Fresh deploy to ensure clean slate for feature implementation

- [x] T001 Fresh deploy: Run `docker compose down -v` to stop containers and remove volumes for clean database state
- [x] T002 Fresh deploy: Run `docker compose up -d --build` to rebuild and start all services with fresh database
- [x] T003 Verify services are running: Confirm registry (port 8080) and datahub (port 8081) respond to health checks

---

## Phase 2: Foundational (Schema Infrastructure)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**CRITICAL**: No user story work can begin until this phase is complete

### CONFIGURABLE Schema Declarations

- [x] T004 [P] Add CONFIGURABLE dict to `DataProvider` base class with crypto preferences in `quasar/lib/providers/core.py`
- [x] T005 [P] Add CONFIGURABLE dict to `HistoricalDataProvider` with scheduling.delay_hours and data.lookback_days in `quasar/lib/providers/core.py`
- [x] T006 [P] Add CONFIGURABLE dict to `LiveDataProvider` with scheduling.pre_close_seconds and scheduling.post_close_seconds in `quasar/lib/providers/core.py`
- [x] T007 [P] Add CONFIGURABLE dict to `IndexProvider` (inherits DataProvider only) in `quasar/lib/providers/core.py`
- [x] T008 Update `DataProvider.__init__` to accept optional `preferences: dict | None = None` parameter in `quasar/lib/providers/core.py`

### Registry Schema Lookup

- [x] T009 Create SCHEMA_MAP dict mapping class_subtype to CONFIGURABLE in `quasar/services/registry/handlers/config.py`
- [x] T010 Create `get_schema_for_subtype()` utility function in `quasar/services/registry/handlers/config.py`

### Pydantic Models

- [ ] T011 [P] Add `HistoricalSchedulingPreferences` model with delay_hours field (ge=0, le=24) in `quasar/services/registry/schemas.py`
- [ ] T012 [P] Add `LiveSchedulingPreferences` model with pre_close_seconds (ge=0, le=300) and post_close_seconds (ge=0, le=60) in `quasar/services/registry/schemas.py`
- [ ] T013 [P] Add `DataPreferences` model with lookback_days field (ge=1, le=8000) in `quasar/services/registry/schemas.py`
- [ ] T014 [P] Add `ConfigSchemaResponse` model with class_name, class_type, class_subtype, schema fields in `quasar/services/registry/schemas.py`
- [ ] T015 [P] Add `SecretKeysResponse` model with class_name, class_type, keys fields in `quasar/services/registry/schemas.py`
- [ ] T016 [P] Add `SecretsUpdateRequest` model with secrets: dict[str, str] field in `quasar/services/registry/schemas.py`
- [ ] T017 [P] Add `SecretsUpdateResponse` model with status and keys fields in `quasar/services/registry/schemas.py`

### Schema API Endpoint

- [ ] T018 Add `GET /api/registry/config/schema` endpoint handler in `quasar/services/registry/handlers/config.py`
- [ ] T019 Register schema endpoint route in registry service in `quasar/services/registry/app.py`

### Validation Enhancement

- [ ] T020 Enhance `handle_update_provider_config()` to validate updates against provider-type-specific schema in `quasar/services/registry/handlers/config.py`
- [ ] T021 Add logging for preference validation failures (FR-026) in `quasar/services/registry/handlers/config.py`

### DataHub Preferences Loading

- [ ] T022 Update SQL query in `load_provider_cls()` to SELECT preferences column in `quasar/services/datahub/handlers/providers.py`
- [ ] T023 Pass preferences to provider constructor in `load_provider_cls()` in `quasar/services/datahub/handlers/providers.py`
- [ ] T024 Add `_provider_preferences` dict to store loaded preferences for runtime access in `quasar/services/datahub/handlers/providers.py`
- [ ] T025 Add `POST /api/datahub/providers/{name}/unload` endpoint handler in `quasar/services/datahub/handlers/providers.py`
- [ ] T026 Register unload endpoint route in datahub service in `quasar/services/datahub/app.py`

### Foundation Tests

- [ ] T027 [P] Write unit tests for CONFIGURABLE schema inheritance in `tests/test_configurable_providers.py`
- [ ] T028 [P] Write unit tests for schema lookup utility in `tests/test_configurable_providers.py`
- [ ] T029 [P] Write contract test for GET /api/registry/config/schema endpoint in `tests/test_config_integration.py`

**Checkpoint**: Foundation ready - Restart docker containers (`docker compose down && docker compose up -d`) and verify schema endpoint returns correct data. Run `conda activate quasar && pytest tests/test_configurable_providers.py tests/test_config_integration.py -v`

---

## Phase 3: User Story 1 - Configure Historical Data Pull Timing (Priority: P1)

**Goal**: Users can configure delay_hours for historical providers to stagger data pulls and avoid API rate limits

**Independent Test**: Configure delay_hours=6 for EODHD provider and observe scheduled jobs fire at 06:00 UTC offset

### Backend Implementation for US1

- [ ] T030 [US1] Read delay_hours from preferences in `refresh_subscriptions()` for historical providers in `quasar/services/datahub/handlers/collection.py`
- [ ] T031 [US1] Apply positive offset (delay_hours * 3600) to `OffsetCronTrigger` for historical providers in `quasar/services/datahub/handlers/collection.py`
- [ ] T032 [US1] Add logging for preference changes (FR-025) when scheduling preferences are updated in `quasar/services/registry/handlers/config.py`

### Frontend Implementation for US1

- [ ] T033 [US1] Add Scheduling tab to ProviderConfigModal with conditional rendering based on class_subtype in `web/src/views/registry/ProviderConfigModal.js`
- [ ] T034 [US1] Implement delay_hours slider (0-24) with preview showing resulting pull time for historical providers in `web/src/views/registry/ProviderConfigModal.js`
- [ ] T035 [US1] Add `getConfigSchema()` API call to fetch schema for dynamic UI rendering in `web/src/services/registry_api.js`
- [ ] T036 [US1] Add `updateProviderConfig()` API call (if not already exists) for saving scheduling preferences in `web/src/services/registry_api.js`

### Tests for US1

- [ ] T037 [P] [US1] Write integration test: historical provider job fires at configured delay offset in `tests/test_config_integration.py`
- [ ] T038 [P] [US1] Write unit test: OffsetCronTrigger receives correct positive offset in `tests/test_configurable_providers.py`
- [ ] T039 [US1] Visual test with Playwright: Verify Scheduling tab displays delay slider for historical provider (use playwright-visual-tester agent)

**Checkpoint**: User Story 1 complete - Restart docker (`docker compose down && docker compose up -d`), run backend tests (`conda activate quasar && pytest tests/test_config*.py -v`), verify UI scheduling tab via Playwright testing

---

## Phase 4: User Story 2 - Configure Live Provider Timing Buffers (Priority: P1)

**Goal**: Users can configure pre_close_seconds and post_close_seconds for live providers to capture late-arriving data

**Independent Test**: Configure post_close_seconds=10 for Kraken and verify listening window extends accordingly

### Backend Implementation for US2

- [ ] T040 [US2] Read pre_close_seconds from preferences in `refresh_subscriptions()` for live providers in `quasar/services/datahub/handlers/collection.py`
- [ ] T041 [US2] Apply negative offset (-pre_close_seconds) to `OffsetCronTrigger` for live providers in `quasar/services/datahub/handlers/collection.py`
- [ ] T042 [US2] Read post_close_seconds from preferences and use in timeout calculation in `get_data()` in `quasar/services/datahub/handlers/collection.py`

### Frontend Implementation for US2

- [ ] T043 [US2] Implement pre_close_seconds input (0-300) for live providers in Scheduling tab in `web/src/views/registry/ProviderConfigModal.js`
- [ ] T044 [US2] Implement post_close_seconds input (0-60) for live providers in Scheduling tab in `web/src/views/registry/ProviderConfigModal.js`
- [ ] T045 [US2] Add visual timeline showing listening window in Scheduling tab for live providers in `web/src/views/registry/ProviderConfigModal.js`

### Tests for US2

- [ ] T046 [P] [US2] Write integration test: live provider job fires at configured pre_close offset in `tests/test_config_integration.py`
- [ ] T047 [P] [US2] Write unit test: OffsetCronTrigger receives correct negative offset for live providers in `tests/test_configurable_providers.py`
- [ ] T048 [US2] Visual test with Playwright: Verify Scheduling tab displays pre/post close inputs for live provider (use playwright-visual-tester agent)

**Checkpoint**: User Story 2 complete - Restart docker, run tests, verify UI via Playwright

---

## Phase 5: User Story 3 - Configure Historical Lookback Period (Priority: P2)

**Goal**: Users can configure lookback_days to reduce API costs for new subscriptions

**Independent Test**: Set lookback_days=365, add new symbol subscription, verify request starts from ~1 year ago

### Backend Implementation for US3

- [ ] T049 [US3] Read lookback_days from preferences in `_build_reqs_historical()` instead of DEFAULT_LOOKBACK in `quasar/services/datahub/handlers/collection.py`
- [ ] T050 [US3] Add logging when lookback preference is applied to new subscription in `quasar/services/datahub/handlers/collection.py`

### Frontend Implementation for US3

- [ ] T051 [US3] Add Data tab to ProviderConfigModal (visible for historical providers only) in `web/src/views/registry/ProviderConfigModal.js`
- [ ] T052 [US3] Implement preset radio buttons (1 month=30, 3 months=90, 1 year=365, 3 years=1095, 5 years=1825, max=8000) in `web/src/views/registry/ProviderConfigModal.js`
- [ ] T053 [US3] Implement custom lookback_days input field with validation (1-8000) in `web/src/views/registry/ProviderConfigModal.js`

### Tests for US3

- [ ] T054 [P] [US3] Write integration test: new subscription uses configured lookback_days for start date in `tests/test_config_integration.py`
- [ ] T055 [P] [US3] Write unit test: _build_reqs_historical() uses preference over DEFAULT_LOOKBACK in `tests/test_configurable_providers.py`
- [ ] T056 [US3] Visual test with Playwright: Verify Data tab displays preset options and custom input (use playwright-visual-tester agent)

**Checkpoint**: User Story 3 complete - Restart docker, run tests, verify UI via Playwright

---

## Phase 6: User Story 4 - Update API Credentials (Priority: P2)

**Goal**: Users can rotate API credentials without re-uploading provider code

**Independent Test**: Update credentials for EODHD, verify provider unloads and uses new credentials on next request

### Backend Implementation for US4

- [ ] T057 [US4] Add `handle_get_secret_keys()` endpoint handler that decrypts and returns key names (not values) in `quasar/services/registry/handlers/config.py`
- [ ] T058 [US4] Register GET /api/registry/config/secret-keys route in registry service in `quasar/services/registry/app.py`
- [ ] T059 [US4] Add `handle_update_secrets()` endpoint handler with re-encryption logic (new nonce per update) in `quasar/services/registry/handlers/config.py`
- [ ] T060 [US4] Register PATCH /api/registry/config/secrets route in registry service in `quasar/services/registry/app.py`
- [ ] T061 [US4] Call DataHub unload endpoint after successful credential update in `handle_update_secrets()` in `quasar/services/registry/handlers/config.py`
- [ ] T062 [US4] Add logging for credential updates (provider name, timestamp, unload triggered) in `quasar/services/registry/handlers/config.py`

### Frontend Implementation for US4

- [ ] T063 [US4] Add `getSecretKeys()` API call to fetch secret key names in `web/src/services/registry_api.js`
- [ ] T064 [US4] Add `updateSecrets()` API call to submit new credentials in `web/src/services/registry_api.js`
- [ ] T065 [US4] Replace "Coming Soon" placeholder in API Secrets tab with dynamic password fields in `web/src/views/registry/ProviderConfigModal.js`
- [ ] T066 [US4] Fetch secret key names on tab activation and render password inputs in `web/src/views/registry/ProviderConfigModal.js`
- [ ] T067 [US4] Implement submit handler that calls updateSecrets() with all credential values in `web/src/views/registry/ProviderConfigModal.js`
- [ ] T068 [US4] Add confirmation dialog before credential update with warning about all-or-nothing replacement in `web/src/views/registry/ProviderConfigModal.js`

### Tests for US4

- [ ] T069 [P] [US4] Write contract test for GET /api/registry/config/secret-keys endpoint in `tests/test_config_integration.py`
- [ ] T070 [P] [US4] Write contract test for PATCH /api/registry/config/secrets endpoint in `tests/test_config_integration.py`
- [ ] T071 [P] [US4] Write integration test: credential update triggers provider unload in `tests/test_config_integration.py`
- [ ] T072 [US4] Visual test with Playwright: Verify API Secrets tab renders password fields from key names (use playwright-visual-tester agent)

**Checkpoint**: User Story 4 complete - Restart docker, run tests, verify UI via Playwright

---

## Phase 7: User Story 5 - Retrieve Provider Configuration Schema (Priority: P3)

**Goal**: Developers can programmatically discover configurable options via schema endpoint

**Independent Test**: Call GET /api/registry/config/schema for different provider types and verify correct schemas returned

### Backend Implementation for US5

- [ ] T073 [US5] Enhance schema endpoint to return complete metadata (type as string, min, max, description) in `quasar/services/registry/handlers/config.py`
- [ ] T074 [US5] Add type conversion from Python types to JSON-friendly strings (int→"integer", str→"string") in `quasar/services/registry/handlers/config.py`

### Tests for US5

- [ ] T075 [P] [US5] Write contract test: schema endpoint returns scheduling.delay_hours for historical providers in `tests/test_config_integration.py`
- [ ] T076 [P] [US5] Write contract test: schema endpoint returns pre/post_close_seconds for live providers in `tests/test_config_integration.py`
- [ ] T077 [P] [US5] Write contract test: schema endpoint returns only crypto for index providers in `tests/test_config_integration.py`
- [ ] T078 [US5] Write integration test: schema response matches CONFIGURABLE definition in provider base class in `tests/test_config_integration.py`

**Checkpoint**: User Story 5 complete - Run tests and verify schema API matches OpenAPI contract

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Final validation and cleanup

- [ ] T079 [P] Run full test suite with coverage: `conda activate quasar && pytest --cov=quasar tests/test_config*.py -v`
- [ ] T080 [P] Verify backward compatibility: Providers with no preferences behave like current defaults
- [ ] T081 Final visual QA with Playwright: Full workflow - configure scheduling, data, credentials for a provider (use playwright-visual-tester agent)
- [ ] T082 Verify all success criteria from spec.md are met (SC-001 through SC-008)
- [ ] T083 Code cleanup: Remove any debug logging, ensure consistent error messages
- [ ] T084 Run quickstart.md validation: Execute API usage examples and verify responses match documentation

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phases 3-7)**: All depend on Foundational phase completion
  - US1 (Phase 3) and US2 (Phase 4): Can proceed in parallel (different scheduling types)
  - US3 (Phase 5): Can proceed independently (data configuration)
  - US4 (Phase 6): Can proceed independently (credentials)
  - US5 (Phase 7): Can proceed independently (schema endpoint enhancement)
- **Polish (Phase 8)**: Depends on all user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational - No dependencies on other stories
- **User Story 2 (P1)**: Can start after Foundational - No dependencies on other stories
- **User Story 3 (P2)**: Can start after Foundational - No dependencies on other stories
- **User Story 4 (P2)**: Can start after Foundational - Depends on unload endpoint from Phase 2
- **User Story 5 (P3)**: Can start after Foundational - Schema endpoint created in Phase 2

### Within Each User Story

- Backend implementation before frontend (API must exist for UI to call)
- Tests written alongside or immediately after implementation
- Docker restart recommended after backend changes before UI testing
- Playwright visual tests after frontend implementation complete

### Parallel Opportunities

- All Foundational Pydantic models (T011-T017) can run in parallel
- All Foundational CONFIGURABLE declarations (T004-T007) can run in parallel
- Tests marked [P] within each phase can run in parallel
- User Stories 1-5 can be worked on in parallel after Foundational phase (if team capacity allows)
- Frontend tasks within a story can be parallelized if they're in separate components

---

## Parallel Example: Foundational Phase

```bash
# Launch all Pydantic models together:
Task: "Add HistoricalSchedulingPreferences model in quasar/services/registry/schemas.py"
Task: "Add LiveSchedulingPreferences model in quasar/services/registry/schemas.py"
Task: "Add DataPreferences model in quasar/services/registry/schemas.py"
Task: "Add ConfigSchemaResponse model in quasar/services/registry/schemas.py"

# Launch all CONFIGURABLE declarations together:
Task: "Add CONFIGURABLE dict to DataProvider in quasar/lib/providers/core.py"
Task: "Add CONFIGURABLE dict to HistoricalDataProvider in quasar/lib/providers/core.py"
Task: "Add CONFIGURABLE dict to LiveDataProvider in quasar/lib/providers/core.py"
```

---

## Implementation Strategy

### MVP First (User Stories 1 + 2 Only)

1. Complete Phase 1: Setup (fresh deploy)
2. Complete Phase 2: Foundational (schema infrastructure)
3. Complete Phase 3: User Story 1 (historical scheduling)
4. Complete Phase 4: User Story 2 (live scheduling)
5. **STOP and VALIDATE**: Test both scheduling configurations independently
6. Deploy/demo if ready - scheduling configuration delivers immediate value

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 + 2 → Test independently → Deploy/Demo (MVP!)
3. Add User Story 3 → Test independently → Deploy/Demo (lookback config)
4. Add User Story 4 → Test independently → Deploy/Demo (credential rotation)
5. Add User Story 5 → Test independently → Deploy/Demo (schema discovery)
6. Each story adds value without breaking previous stories

### Docker Restart Guidelines

Restart containers (`docker compose down && docker compose up -d`) when:
- After modifying Python backend code (handlers, schemas, providers)
- Before running manual API endpoint tests
- Before Playwright UI testing
- After each user story phase completion for checkpoint validation

### Testing Environment

- All Python tests must run in `quasar` conda environment: `conda activate quasar`
- Playwright tests use the running dev server (port 3000)
- Integration tests require docker services to be running

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence
- Use playwright-visual-tester agent for all UI verification tasks
