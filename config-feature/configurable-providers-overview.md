# Configurable Providers: Overall Specification

## Executive Summary

This specification describes enhancements to the Quasar provider configuration system. The goal is to make provider behavior configurable at runtime without requiring code changes or re-uploads, while maintaining security and simplicity.

**Features:**
1. [Scheduling Configuration](./scheduling-configuration.md) - Timing for historical pulls and live data listening
2. [Lookback Period](./lookback-period.md) - Historical data depth for new subscriptions
3. [API Credentials](./api-credentials.md) - Update credentials without re-uploading code

---

## Current State

### What Exists (PR #45)

**Database:**
- `preferences` JSONB column in `code_registry` table
- Default value: `'{}'`

**Backend:**
- `GET /api/registry/config` - Retrieve provider preferences
- `PUT /api/registry/config` - Update preferences (partial merge via JSONB)
- `GET /api/registry/config/available-quote-currencies` - List quote currencies for crypto assets

**Schema (Pydantic):**
```python
class CryptoPreferences(BaseModel):
    preferred_quote_currency: Optional[str] = None

class ProviderPreferences(BaseModel):
    crypto: Optional[CryptoPreferences] = None
```

**Frontend:**
- Provider Config Modal with tabs
- "Trading Preferences" tab: Crypto quote currency dropdown
- "API Secrets" tab: Placeholder ("Coming Soon")

**Usage:**
- `AutomatedMapper` reads `preferred_quote_currency` to select crypto trading pairs during asset mapping

### What's Hardcoded

| Setting | Location | Current Value |
|---------|----------|---------------|
| Historical delay | `OffsetCronTrigger` offset | 0 seconds |
| Live pre-close buffer | `DEFAULT_LIVE_OFFSET` | 30 seconds |
| Live post-close buffer | `close_buffer_seconds` property | 5 seconds (Kraken) |
| Lookback period | `DEFAULT_LOOKBACK` | 8000 days |
| API credentials | `nonce`/`ciphertext` columns | Immutable after upload |

### Provider Type Detection

The `class_subtype` column in `code_registry` stores provider type:
- `'Historical'` - HistoricalDataProvider subclasses
- `'Live'` - LiveDataProvider subclasses
- `'IndexProvider'` - IndexProvider subclasses
- `'UserIndex'` - User-defined indices (UI-created, no code)

This is set during upload validation by DataHub (`handlers/providers.py:311-312`).

---

## Proposed Architecture

### Design Principles

1. **Schema in base classes** - Provider base classes declare what preferences they accept, with types, defaults, and constraints
2. **JSONB storage retained** - The `preferences` column remains flexible; validation happens at the application layer
3. **Provider-type-aware validation** - Use `class_subtype` to determine which schema applies
4. **Clean DataHub access** - Preferences loaded alongside provider, accessible at runtime

### Preference Schema Declaration

Preferences are declared in base classes, not in user-uploaded provider code. This ensures security (users can't declare arbitrary config) and consistency.

**Base class hierarchy:**

```python
# quasar/lib/providers/core.py

class DataProvider(ABC):
    """Base class for all data providers."""

    # Shared preferences (available to all provider types)
    CONFIGURABLE = {
        "crypto": {
            "preferred_quote_currency": {
                "type": str,
                "default": None,
                "description": "Preferred quote currency for crypto pairs (e.g., USDC, USDT, USD)"
            }
        }
    }


class HistoricalDataProvider(DataProvider):
    """Base class for historical data providers."""

    CONFIGURABLE = {
        **DataProvider.CONFIGURABLE,
        "scheduling": {
            "delay_hours": {
                "type": int,
                "default": 0,
                "min": 0,
                "max": 24,
                "description": "Hours after default cron time to run data pulls"
            }
        },
        "data": {
            "lookback_days": {
                "type": int,
                "default": 8000,
                "min": 1,
                "max": 8000,
                "description": "Days of historical data to fetch for new subscriptions"
            }
        }
    }


class LiveDataProvider(DataProvider):
    """Base class for live/realtime data providers."""

    CONFIGURABLE = {
        **DataProvider.CONFIGURABLE,
        "scheduling": {
            "pre_close_seconds": {
                "type": int,
                "default": 30,
                "min": 0,
                "max": 300,
                "description": "Seconds before bar close to start listening for data"
            },
            "post_close_seconds": {
                "type": int,
                "default": 5,
                "min": 0,
                "max": 60,
                "description": "Seconds after bar close to continue listening for late messages"
            }
        }
    }


class IndexProvider(DataProvider):
    """Base class for index providers."""
    # Inherits only shared preferences (crypto)
    # No scheduling or data preferences apply
    pass
```

### Validation Flow

```
1. User submits preference update via PUT /api/registry/config
   {
     "class_name": "EODHD",
     "class_type": "provider",
     "scheduling": {"delay_hours": 6}
   }

2. Registry service queries class_subtype from code_registry
   → Returns "Historical"

3. Registry loads schema for HistoricalDataProvider.CONFIGURABLE

4. Validates update against schema:
   - "scheduling.delay_hours" exists in schema? ✓
   - Value 6 is int? ✓
   - 0 ≤ 6 ≤ 24? ✓

5. Merges into existing preferences JSONB

6. Stores updated preferences
```

### DataHub Access to Preferences

When DataHub loads a provider, it also fetches preferences:

```python
# quasar/services/datahub/handlers/providers.py

async def load_provider_cls(self, name: str) -> bool:
    # ... existing code to load provider class and DerivedContext ...

    # Fetch preferences from code_registry
    preferences = await self._fetch_provider_preferences(name)

    # Pass to provider constructor
    prov = ProviderCls(context=context, preferences=preferences)

    # Store for runtime access
    self._providers[name] = prov
    self._provider_preferences[name] = preferences
```

**Provider base class update:**

```python
class DataProvider(ABC):
    def __init__(self, context: DerivedContext, preferences: dict | None = None):
        self.context = context
        self.preferences = preferences or {}
        # ... existing init code ...
```

**Runtime usage:**

```python
# In refresh_subscriptions() or get_data()
preferences = self._provider_preferences.get(provider_name, {})
delay_hours = preferences.get("scheduling", {}).get("delay_hours", 0)
```

### Database Schema

**No changes required to `code_registry` table.** The existing `preferences` JSONB column is sufficient.

**Stored structure for historical provider:**

```json
{
  "crypto": {
    "preferred_quote_currency": "USDC"
  },
  "scheduling": {
    "delay_hours": 6
  },
  "data": {
    "lookback_days": 365
  }
}
```

**Stored structure for live provider:**

```json
{
  "crypto": {
    "preferred_quote_currency": "USDT"
  },
  "scheduling": {
    "pre_close_seconds": 45,
    "post_close_seconds": 10
  }
}
```

---

## Feature Matrix by Provider Type

| Feature | Historical | Live | Index |
|---------|------------|------|-------|
| `crypto.preferred_quote_currency` | ✓ | ✓ | ✓ |
| `scheduling.delay_hours` | ✓ | — | — |
| `scheduling.pre_close_seconds` | — | ✓ | — |
| `scheduling.post_close_seconds` | — | ✓ | — |
| `data.lookback_days` | ✓ | — | — |
| API credential updates | ✓ | ✓ | — |

---

## API Design

### Existing Endpoints (Modified)

**`GET /api/registry/config`**

No signature changes. Returns full preferences object.

**`PUT /api/registry/config`**

Add validation against provider-type-specific schema before merging.

### New Endpoints

**`GET /api/registry/config/schema`**

Returns the configurable preferences schema for a provider based on its type.

```
GET /api/registry/config/schema?class_name=EODHD&class_type=provider

Response:
{
  "class_name": "EODHD",
  "class_type": "provider",
  "class_subtype": "Historical",
  "schema": {
    "crypto": {
      "preferred_quote_currency": {
        "type": "string",
        "default": null,
        "description": "Preferred quote currency for crypto pairs"
      }
    },
    "scheduling": {
      "delay_hours": {
        "type": "integer",
        "default": 0,
        "min": 0,
        "max": 24,
        "description": "Hours after default cron time to run data pulls"
      }
    },
    "data": {
      "lookback_days": {
        "type": "integer",
        "default": 8000,
        "min": 1,
        "max": 8000,
        "description": "Days of historical data to fetch for new subscriptions"
      }
    }
  }
}
```

**`GET /api/registry/config/secret-keys`**

Returns names of stored secrets (not values) for credential update UI.
See [API Credentials spec](./api-credentials.md).

**`PATCH /api/registry/config/secrets`**

Re-encrypts and stores new credentials.
See [API Credentials spec](./api-credentials.md).

### DataHub Endpoints

**`POST /api/datahub/providers/{name}/unload`**

Unloads a provider from memory, forcing reload on next use. Called by Registry after credential updates.

---

## UI Design

### Provider Config Modal Structure

```
┌─────────────────────────────────────────────────────────────┐
│  Provider Settings: {PROVIDER_NAME}                    [X]  │
├─────────────────────────────────────────────────────────────┤
│  [Trading]  [Scheduling]  [Data]  [API Secrets]             │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  (Tab content varies by provider type and selected tab)     │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Tab Visibility by Provider Type

| Tab | Historical | Live | Index |
|-----|------------|------|-------|
| Trading | ✓ | ✓ | ✓ |
| Scheduling | ✓ | ✓ | Hidden |
| Data | ✓ | Hidden | Hidden |
| API Secrets | ✓ | ✓ | Hidden |

### Tab Contents

**Trading Tab:**
- Crypto quote currency preference (existing, unchanged behavior)

**Scheduling Tab:**
- Historical: Delay slider (0-24 hours) with preview
- Live: Pre-close and post-close inputs with visual timeline
- See [Scheduling Configuration spec](./scheduling-configuration.md)

**Data Tab:**
- Lookback period radio buttons with presets + custom input
- See [Lookback Period spec](./lookback-period.md)

**API Secrets Tab:**
- Dynamic password fields generated from secret key names
- See [API Credentials spec](./api-credentials.md)

### Dynamic UI Generation

The frontend can use `GET /api/registry/config/schema` to:
1. Determine which tabs to show based on `class_subtype`
2. Render appropriate controls based on field types and constraints
3. Display descriptions as help text
4. Validate input before submission

---

## Implementation Phases

### Phase 1: Schema Infrastructure
1. Add `CONFIGURABLE` declarations to base provider classes
2. Create schema lookup by `class_subtype` in Registry service
3. Add `GET /api/registry/config/schema` endpoint
4. Add validation to `PUT /api/registry/config`

### Phase 2: DataHub Preferences Integration
1. Fetch preferences when loading providers
2. Pass preferences to provider constructors (optional parameter)
3. Store preferences in `_provider_preferences` dict for runtime access
4. Add provider unload endpoint

### Phase 3: Scheduling Configuration
1. Historical: Read `delay_hours`, apply to `OffsetCronTrigger`
2. Live: Read `pre_close_seconds`, apply as negative offset
3. Live: Read `post_close_seconds`, use in timeout calculation
4. Add Scheduling tab to UI (different content by provider type)

### Phase 4: Data Configuration
1. Read `lookback_days` in `_build_reqs_historical()`
2. Add Data tab to UI

### Phase 5: API Credentials
1. Add `GET /api/registry/config/secret-keys` endpoint
2. Add `PATCH /api/registry/config/secrets` endpoint
3. Integrate with DataHub unload mechanism
4. Replace placeholder in API Secrets tab

---

## Migration & Compatibility

### Existing Preferences

The current `crypto.preferred_quote_currency` structure is preserved. The new schema is a superset — no migration needed.

### Default Behavior

All new preferences have defaults matching current hardcoded values:

| Setting | Default | Matches |
|---------|---------|---------|
| `delay_hours` | 0 | Current behavior (no delay) |
| `pre_close_seconds` | 30 | `DEFAULT_LIVE_OFFSET` |
| `post_close_seconds` | 5 | Typical `close_buffer_seconds` |
| `lookback_days` | 8000 | `DEFAULT_LOOKBACK` |

**Providers with no configured preferences behave exactly as they do today.**

### Provider Code Compatibility

Existing provider implementations don't need changes. The `preferences` parameter is optional:

```python
def __init__(self, context: DerivedContext, preferences: dict | None = None):
    super().__init__(context, preferences)
    # Existing provider code unchanged
```

---

## Security Considerations

1. **Schema in base classes only** — User-uploaded code cannot declare new preference fields
2. **Validation before storage** — Invalid preference values rejected at API layer
3. **Credentials never exposed** — Secret key names returned to UI, never values
4. **New nonce per credential update** — AES-GCM security maintained
5. **Provider unload on credential change** — No stale credentials in memory

---

## Testing Strategy

### Unit Tests
- Schema lookup returns correct `CONFIGURABLE` for each `class_subtype`
- Validation rejects invalid types, out-of-range values
- Preferences merge correctly (partial updates)
- Offset calculations correct for all scheduling scenarios

### Integration Tests
- Historical jobs fire at configured delay
- Live jobs fire at configured pre-close offset
- Live listening stops at configured post-close time
- New subscriptions use configured lookback
- Credential update triggers provider unload
- Provider reload uses new credentials

### UI Tests
- Correct tabs visible for each provider type
- Schema endpoint drives field rendering
- Validation feedback before submission
- Toast notifications on save

---

## Related Documents

- [Scheduling Configuration](./scheduling-configuration.md) — Timing for historical and live providers
- [Lookback Period](./lookback-period.md) — Historical data depth configuration
- [API Credentials](./api-credentials.md) — Credential update flow

## Related Issues

- [#43 - Configurable API Secrets](https://github.com/ddm-j/quasar/issues/43)
- [#50 - Configurable cron scheduling](https://github.com/ddm-j/quasar/issues/50)
- [PR #45 - Asset Identification & Automapping](https://github.com/ddm-j/quasar/pull/45) — Initial preferences system
