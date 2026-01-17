# Research: Configurable Providers

**Feature**: 001-configurable-providers
**Date**: 2026-01-17

This document consolidates technical research and implementation decisions for the configurable providers feature. Most decisions were made during the detailed specification phase (see `config-feature/*.md`).

---

## 1. Schema Declaration Strategy

### Decision
Declare `CONFIGURABLE` dictionaries as class-level attributes in provider base classes (`DataProvider`, `HistoricalDataProvider`, `LiveDataProvider`, `IndexProvider`).

### Rationale
- **Security**: User-uploaded provider code cannot declare arbitrary configuration fields
- **Consistency**: All providers of the same type have the same configurable options
- **Simplicity**: No dynamic schema discovery or parsing; schema is static and type-safe

### Alternatives Considered

| Alternative | Why Rejected |
|-------------|--------------|
| Schema in provider code | Security risk; users could inject arbitrary config |
| Schema in database table | Over-engineering; adds migration burden |
| Schema in separate YAML | Unnecessary indirection; code is source of truth |

### Implementation Pattern
```python
class HistoricalDataProvider(DataProvider):
    CONFIGURABLE = {
        **DataProvider.CONFIGURABLE,
        "scheduling": {
            "delay_hours": {"type": int, "default": 0, "min": 0, "max": 24, ...}
        },
        "data": {
            "lookback_days": {"type": int, "default": 8000, "min": 1, "max": 8000, ...}
        }
    }
```

---

## 2. Schema Lookup Mechanism

### Decision
Map `class_subtype` (from `code_registry` table) to `CONFIGURABLE` dictionaries at runtime in the Registry service.

### Rationale
- `class_subtype` already reliably identifies provider type (`'Historical'`, `'Live'`, `'IndexProvider'`, `'UserIndex'`)
- Set during upload validation in DataHub (`handlers/providers.py:311-312`)
- No additional database queries needed beyond existing `code_registry` lookup

### Implementation Pattern
```python
# In registry service
SCHEMA_MAP = {
    "Historical": HistoricalDataProvider.CONFIGURABLE,
    "Live": LiveDataProvider.CONFIGURABLE,
    "IndexProvider": IndexProvider.CONFIGURABLE,
    "UserIndex": {},  # No configurable options
}

def get_schema_for_subtype(class_subtype: str) -> dict:
    return SCHEMA_MAP.get(class_subtype, DataProvider.CONFIGURABLE)
```

---

## 3. Preference Storage Format

### Decision
Store preferences as flat JSONB in existing `preferences` column with nested structure by category.

### Rationale
- No database migration required
- Existing `PUT /api/registry/config` already uses JSONB merge pattern
- Structure is self-documenting and easy to query

### Storage Structure
```json
{
  "crypto": {"preferred_quote_currency": "USDC"},
  "scheduling": {"delay_hours": 6},
  "data": {"lookback_days": 365}
}
```

### Backward Compatibility
- Existing `crypto.preferred_quote_currency` preserved
- Missing fields use defaults from `CONFIGURABLE`
- Empty `preferences` (`'{}'`) behaves identically to current hardcoded values

---

## 4. Scheduling Implementation

### Decision
Use existing `OffsetCronTrigger` for both historical delays and live pre-close offsets.

### Rationale
- `OffsetCronTrigger` already exists in `quasar/lib/common/offset_cron.py`
- Supports both positive offsets (delay) and negative offsets (pre-close)
- No new scheduling infrastructure needed

### Implementation Details

**Historical providers (delay_hours)**:
```python
# In refresh_subscriptions()
delay_hours = preferences.get("scheduling", {}).get("delay_hours", 0)
offset_seconds = delay_hours * 3600  # Convert to seconds
trigger = OffsetCronTrigger.from_crontab(cron, offset_seconds=offset_seconds)
```

**Live providers (pre_close_seconds)**:
```python
# In refresh_subscriptions()
pre_close = preferences.get("scheduling", {}).get("pre_close_seconds", DEFAULT_LIVE_OFFSET)
offset_seconds = -1 * pre_close  # Negative offset
trigger = OffsetCronTrigger.from_crontab(cron, offset_seconds=offset_seconds)
```

**Live providers (post_close_seconds)**:
- Used in timeout calculation for `get_data()`, not in cron trigger
- Total timeout = pre_close + post_close + buffer

---

## 5. DataHub Preferences Loading

### Decision
Fetch preferences from `code_registry` when loading providers; pass to constructor as optional parameter.

### Rationale
- Single database query already fetches provider metadata (`file_path`, `file_hash`, `nonce`, `ciphertext`)
- Adding `preferences` to same query is trivial
- Constructor injection follows Constitution Principle III (Constructor DI)

### Implementation Pattern
```python
# In load_provider_cls()
async with self.pool.acquire() as conn:
    row = await conn.fetchrow(
        "SELECT file_path, file_hash, nonce, ciphertext, preferences FROM code_registry WHERE ...",
        name
    )
    preferences = row['preferences'] or {}

# Pass to constructor
prov = ProviderCls(context=context, preferences=preferences)
self._provider_preferences[name] = preferences
```

### Provider Base Class Update
```python
class DataProvider(ABC):
    def __init__(self, context: DerivedContext, preferences: dict | None = None):
        self.context = context
        self.preferences = preferences or {}
        # ... existing init code ...
```

---

## 6. Provider Unload Mechanism

### Decision
Registry calls DataHub via HTTP API to unload providers after credential updates.

### Rationale
- Maintains service separation (Registry doesn't directly access DataHub internals)
- Simple implementation: one POST endpoint
- Consistent with existing inter-service communication patterns

### Alternatives Considered

| Alternative | Why Rejected |
|-------------|--------------|
| Shared database flag | Polling adds latency; complicates DataHub refresh cycle |
| Direct function call | Only works if services are co-located; violates service boundary |
| Message queue | Over-engineering for single-user system |

### Implementation
```
Registry: PATCH /api/registry/config/secrets
  → Success → POST /api/datahub/providers/{name}/unload
    → DataHub removes provider from _providers dict
    → Next data request triggers automatic reload with new credentials
```

---

## 7. Credential Update Security

### Decision
All-or-nothing credential updates with new nonce generation.

### Rationale
- **Simplicity**: No partial update logic; user provides complete secret set
- **Security**: New nonce per update is critical for AES-GCM; reusing nonces is catastrophic
- **Existing pattern**: Uses same HKDF + AES-256-GCM as initial upload

### Security Constraints
- Secret key names returned to UI, never actual values
- UI renders password fields; user types fresh values
- Backend generates new 12-byte random nonce for each update
- Encryption key derived from file hash (unchanged)

### Implementation Pattern
```python
# In handle_update_secrets()
file_hash = await get_file_hash(class_name, class_type)
key = self.system_context.get_derived_context(file_hash)
nonce = os.urandom(12)  # Fresh nonce
ciphertext = aesgcm.encrypt(nonce, json.dumps(secrets).encode(), None)
await update_code_registry(class_name, class_type, nonce, ciphertext)
await call_datahub_unload(class_name)
```

---

## 8. Frontend UI Strategy

### Decision
Extend existing `ProviderConfigModal.js` with new tabs; conditionally render based on `class_subtype`.

### Rationale
- Modal and tab infrastructure already exists
- "API Secrets" tab placeholder ready to be replaced
- Minimal new component creation; extend existing patterns

### Tab Visibility Rules

| Tab | Historical | Live | IndexProvider |
|-----|------------|------|---------------|
| Trading | Visible | Visible | Visible |
| Scheduling | Visible | Visible | Hidden |
| Data | Visible | Hidden | Hidden |
| API Secrets | Visible | Visible | Hidden |

### Dynamic Rendering
- Fetch `class_subtype` with existing config or via new schema endpoint
- Use schema endpoint to render appropriate controls with correct constraints
- Validate input against min/max before submission

---

## 9. Validation Strategy

### Decision
Validate at API layer using Pydantic models and schema constraints before JSONB merge.

### Rationale
- Follows Constitution Principle IV (Pydantic at API edges)
- Catches errors early with clear error messages
- Existing update pattern supports validation injection

### Validation Rules
- Type checking: `delay_hours` must be int, not string
- Range checking: `0 ≤ delay_hours ≤ 24`
- Field existence: Only allow fields declared in `CONFIGURABLE`
- Provider-type matching: Can't set `delay_hours` for live provider

### Error Response Pattern
```json
{
  "detail": "Validation error: scheduling.delay_hours must be between 0 and 24"
}
```

---

## 10. Logging and Observability

### Decision
Log preference changes and validation failures at INFO level.

### Rationale
- Per clarification session (spec.md), observability is required
- Standard Python logging to existing log infrastructure
- Minimal overhead; config changes are infrequent

### Log Events

| Event | Level | Example |
|-------|-------|---------|
| Preference update success | INFO | `"Provider EODHD preferences updated: scheduling.delay_hours=6"` |
| Validation failure | WARNING | `"Provider EODHD preference validation failed: delay_hours=25 exceeds max 24"` |
| Credential update | INFO | `"Provider EODHD credentials updated, unload triggered"` |
| Provider unload | INFO | `"Provider EODHD unloaded from DataHub"` |

---

## References

- `config-feature/configurable-providers-overview.md` - Overall architecture
- `config-feature/scheduling-configuration.md` - Scheduling details
- `config-feature/lookback-period.md` - Lookback details
- `config-feature/api-credentials.md` - Credential update flow
- `quasar/lib/common/offset_cron.py` - OffsetCronTrigger implementation
- `quasar/services/registry/handlers/config.py` - Existing config handlers
- `quasar/services/datahub/handlers/collection.py` - Scheduling and data collection
