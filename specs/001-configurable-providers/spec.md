# Feature Specification: Configurable Providers

**Feature Branch**: `001-configurable-providers`
**Created**: 2026-01-17
**Status**: Draft
**Input**: User description: "Configurable settings for user-uploaded providers including scheduling configuration, lookback period settings, and API credential management. This enables runtime customization of provider behavior without code changes."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Configure Historical Data Pull Timing (Priority: P1)

A user wants to adjust when their historical data provider fetches new data each day. Currently, all providers pull at midnight UTC, but the user wants to stagger their EODHD provider to run at 6 AM UTC to avoid API rate limit issues with multiple providers.

**Why this priority**: Scheduling configuration is the foundational capability that enables runtime provider customization. It demonstrates the preference system works end-to-end (UI → API → storage → DataHub consumption) and provides immediate value by reducing API conflicts.

**Independent Test**: Can be fully tested by configuring delay hours for a historical provider and observing that scheduled jobs fire at the configured offset time. Delivers value by allowing users to optimize their data collection timing.

**Acceptance Scenarios**:

1. **Given** a historical provider (EODHD) is registered, **When** the user opens the Provider Config Modal and navigates to the Scheduling tab, **Then** they see a delay slider with values from 0-24 hours and a preview showing the resulting pull time.

2. **Given** the user sets delay_hours to 6 for EODHD, **When** they save the configuration, **Then** the provider's next scheduled daily pull fires at 06:00 UTC instead of 00:00 UTC.

3. **Given** a historical provider has a configured delay, **When** a new interval subscription is added, **Then** the new job uses the configured delay offset.

---

### User Story 2 - Configure Live Provider Timing Buffers (Priority: P1)

A user running the Kraken live provider notices that bar data sometimes arrives a few seconds after the bar close time. They want to extend the post-close listening window from the default 5 seconds to 10 seconds to capture late-arriving data.

**Why this priority**: Live providers have critical timing requirements; incorrect buffers result in missed data. This is equally important as historical scheduling for data completeness.

**Independent Test**: Can be tested by configuring pre-close and post-close seconds for a live provider and verifying the listening window adjusts accordingly. Delivers value by ensuring complete data capture.

**Acceptance Scenarios**:

1. **Given** a live provider (Kraken) is registered, **When** the user opens the Scheduling tab in the Provider Config Modal, **Then** they see inputs for pre_close_seconds (default: 30) and post_close_seconds (default: 5) with a visual timeline showing the listening window.

2. **Given** the user configures post_close_seconds to 10, **When** a live data job runs, **Then** the provider continues listening for 10 seconds after the bar close time.

3. **Given** the user configures pre_close_seconds to 45, **When** the cron job is scheduled, **Then** it fires 45 seconds before the bar close time.

---

### User Story 3 - Configure Historical Lookback Period (Priority: P2)

A user adding new crypto symbols to their EODHD provider doesn't need 22 years of historical data (the current default). They want to configure the provider to only fetch 1 year of data for new subscriptions to reduce API costs and initial load time.

**Why this priority**: Lookback configuration provides significant value for optimizing API usage and load times, but is less critical than timing configuration since it only affects new subscriptions.

**Independent Test**: Can be tested by setting lookback_days to 365 and adding a new symbol subscription, then verifying the request starts from 1 year ago. Delivers value by reducing API costs and faster time-to-first-data.

**Acceptance Scenarios**:

1. **Given** a historical provider is selected, **When** the user navigates to the Data tab, **Then** they see preset options (1 month, 3 months, 1 year, 3 years, 5 years, maximum) plus a custom input field.

2. **Given** lookback_days is set to 365, **When** a new symbol is subscribed, **Then** the initial data request starts from approximately 1 year ago.

3. **Given** lookback_days is reduced after a symbol already has data, **When** incremental updates run, **Then** existing data is preserved and updates continue from the last fetched point (lookback only affects new subscriptions).

---

### User Story 4 - Update API Credentials (Priority: P2)

A user needs to rotate their EODHD API key after a security audit. Currently, this requires re-uploading the entire provider file. They want to update just the credentials through the existing Provider Config Modal.

**Why this priority**: Credential rotation is essential for security compliance but happens less frequently than configuration changes. The existing upload workflow is a functional (if inconvenient) alternative.

**Independent Test**: Can be tested by updating credentials for a provider and verifying the provider reloads with new credentials on the next data request. Delivers value by enabling credential rotation without code re-upload.

**Acceptance Scenarios**:

1. **Given** the user opens the API Secrets tab for a provider with stored credentials, **When** the tab loads, **Then** password input fields are rendered for each secret key name (e.g., "api_key", "api_secret") with masked placeholder values.

2. **Given** the user enters new credentials in all fields and clicks Update, **When** they confirm the action, **Then** the credentials are re-encrypted and stored, and the provider is unloaded from DataHub.

3. **Given** credentials have been updated, **When** the next data request occurs for that provider, **Then** the provider reloads and uses the new credentials.

---

### User Story 5 - Retrieve Provider Configuration Schema (Priority: P3)

A developer building a custom UI wants to programmatically discover which configuration options are available for a specific provider type. They need an API endpoint that returns the configurable fields, their types, constraints, and defaults.

**Why this priority**: Schema discovery enables dynamic UI generation and is important for extensibility, but the primary UI is being built alongside this feature.

**Independent Test**: Can be tested by calling the schema endpoint for different provider types and verifying the returned schema matches the provider's CONFIGURABLE definition. Delivers value by enabling dynamic UI and validation.

**Acceptance Scenarios**:

1. **Given** a historical provider class_name, **When** calling GET /api/registry/config/schema, **Then** the response includes scheduling.delay_hours and data.lookback_days with their types, defaults, min, and max values.

2. **Given** a live provider class_name, **When** calling GET /api/registry/config/schema, **Then** the response includes scheduling.pre_close_seconds and scheduling.post_close_seconds but not delay_hours or lookback_days.

3. **Given** an index provider class_name, **When** calling GET /api/registry/config/schema, **Then** the response includes only crypto preferences (no scheduling or data fields).

---

### Edge Cases

- What happens when a user configures lookback_days larger than the provider's actual data availability? The provider returns what it has; no error occurs.
- How does the system handle invalid credentials after an update? The provider fails on next load; error is visible in logs and UI status indicators.
- What happens if a user submits different secret keys than the original? All-or-nothing replacement; old keys are lost.
- What happens when delay_hours is set to 24? The job fires at midnight the next day (effectively a 24-hour delay from the default midnight time).
- How are preferences handled for providers with no configuration? Empty preferences object; all defaults apply.

## Requirements *(mandatory)*

### Functional Requirements

**Schema Infrastructure:**
- **FR-001**: Provider base classes MUST declare configurable preferences using a CONFIGURABLE dictionary that specifies field types, defaults, min/max constraints, and descriptions.
- **FR-002**: System MUST provide a schema lookup mechanism that returns the appropriate CONFIGURABLE definition based on provider class_subtype (Historical, Live, Index).
- **FR-003**: System MUST validate preference updates against the provider-type-specific schema before storing.

**API Endpoints:**
- **FR-004**: System MUST provide GET /api/registry/config/schema endpoint that returns the configurable preferences schema for a provider.
- **FR-005**: System MUST enhance PUT /api/registry/config to validate updates against provider-type-specific schemas.
- **FR-006**: System MUST provide GET /api/registry/config/secret-keys endpoint that returns the names (not values) of stored secrets for a provider.
- **FR-007**: System MUST provide PATCH /api/registry/config/secrets endpoint that re-encrypts and stores new credentials.
- **FR-008**: DataHub MUST provide POST /api/datahub/providers/{name}/unload endpoint to force provider reload.

**Scheduling Configuration:**
- **FR-009**: Historical providers MUST support delay_hours preference (0-24) that offsets the default cron schedule.
- **FR-010**: Live providers MUST support pre_close_seconds preference (0-300) that determines when to start listening before bar close.
- **FR-011**: Live providers MUST support post_close_seconds preference (0-60) that determines how long to continue listening after bar close.

**Data Configuration:**
- **FR-012**: Historical providers MUST support lookback_days preference (1-8000) that determines initial data depth for new subscriptions.
- **FR-013**: Lookback preference MUST only affect new subscriptions; existing subscriptions continue incremental updates.

**Credential Management:**
- **FR-014**: System MUST never return actual secret values to the frontend; only key names are exposed.
- **FR-015**: Credential updates MUST be all-or-nothing; partial updates are not allowed.
- **FR-016**: Credential updates MUST generate a new nonce for encryption.
- **FR-017**: Credential updates MUST trigger provider unload to ensure stale credentials are not used.

**DataHub Integration:**
- **FR-018**: DataHub MUST fetch provider preferences when loading providers.
- **FR-019**: DataHub MUST apply scheduling preferences when creating cron triggers.
- **FR-020**: DataHub MUST apply data preferences when building historical requests for new subscriptions.

**User Interface:**
- **FR-021**: Provider Config Modal MUST display tabs conditionally based on provider class_subtype.
- **FR-022**: Scheduling tab MUST show different content for historical vs. live providers.
- **FR-023**: Data tab MUST only be visible for historical providers.
- **FR-024**: API Secrets tab MUST dynamically render password fields from secret key names.

**Observability:**
- **FR-025**: System MUST log all preference changes with provider name, timestamp, and change type (scheduling, data, or crypto).
- **FR-026**: System MUST log all validation failures with provider name, timestamp, and reason for rejection.

### Key Entities

- **Provider Preferences**: Stored per-provider configuration containing scheduling, data, and crypto preference sections. Stored in the existing preferences JSONB column.
- **Provider Schema (CONFIGURABLE)**: Class-level definition in provider base classes declaring available preferences, their types, constraints, and defaults.
- **Encrypted Credentials**: API secrets stored as (nonce, ciphertext) pairs in code_registry, re-encryptable without changing the file hash.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Users can configure and save provider scheduling preferences through the UI in under 30 seconds.
- **SC-002**: Historical provider data pulls execute at the configured delay offset within a 60-second tolerance window.
- **SC-003**: Live provider listening windows match the configured pre/post close seconds within a 1-second tolerance.
- **SC-004**: New symbol subscriptions fetch historical data starting from the configured lookback period (verified by request start dates).
- **SC-005**: Credential updates complete successfully and the provider uses new credentials on the next data request.
- **SC-006**: 100% of preference updates that pass validation are persisted and applied on next provider operation.
- **SC-007**: Providers with no configured preferences behave identically to current hardcoded defaults (backward compatibility).
- **SC-008**: All configurable fields are discoverable via the schema endpoint with complete metadata (type, default, constraints, description).

## Clarifications

### Session 2026-01-17

- Q: Should the system log preference changes and validation failures for operational visibility? → A: Yes, log all preference changes and validation failures (provider name, timestamp, change type)

## Assumptions

- The existing `preferences` JSONB column in `code_registry` is sufficient for storing all new preference types (no schema migration required).
- The `class_subtype` column accurately identifies provider types for schema lookup.
- Users have a single browser session; no real-time sync of preference changes across multiple open modals is required.
- Preference updates take effect on the next scheduled operation (not retroactively applied to in-progress jobs).
- The DataHub unload mechanism will use a direct API call (Option A from the detailed spec) for service separation.
