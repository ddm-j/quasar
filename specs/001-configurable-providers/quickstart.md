# Quickstart: Configurable Providers

**Feature**: 001-configurable-providers
**Date**: 2026-01-17

This guide helps developers quickly understand and implement the configurable providers feature.

---

## Overview

This feature enables runtime configuration of provider behavior:

| Configuration | Provider Type | What It Controls |
|---------------|---------------|------------------|
| `scheduling.delay_hours` | Historical | Hours after midnight to run data pulls |
| `scheduling.pre_close_seconds` | Live | Seconds before bar close to start listening |
| `scheduling.post_close_seconds` | Live | Seconds after bar close to continue listening |
| `data.lookback_days` | Historical | Days of data to fetch for new subscriptions |
| API credentials | Historical, Live | Secret rotation without code re-upload |

---

## Quick Reference

### Key Files to Modify

| Component | File | Changes |
|-----------|------|---------|
| Schema declaration | `quasar/lib/providers/core.py` | Add `CONFIGURABLE` to base classes |
| Schema endpoint | `quasar/services/registry/handlers/config.py` | Add `handle_get_config_schema()` |
| Validation | `quasar/services/registry/handlers/config.py` | Enhance `handle_update_provider_config()` |
| Secrets endpoints | `quasar/services/registry/handlers/config.py` | Add `handle_get_secret_keys()`, `handle_update_secrets()` |
| Pydantic models | `quasar/services/registry/schemas.py` | Add scheduling/data preference models |
| Provider loading | `quasar/services/datahub/handlers/providers.py` | Fetch preferences, pass to constructor |
| Unload endpoint | `quasar/services/datahub/handlers/providers.py` | Add `handle_unload_provider()` |
| Job scheduling | `quasar/services/datahub/handlers/collection.py` | Apply scheduling preferences to triggers |
| Request building | `quasar/services/datahub/handlers/collection.py` | Apply lookback preference |
| UI modal | `web/src/views/registry/ProviderConfigModal.js` | Add Scheduling, Data, API Secrets tabs |
| API client | `web/src/services/registry_api.js` | Add new endpoint functions |

---

## Implementation Checklist

### Phase 1: Schema Infrastructure

- [ ] Add `CONFIGURABLE` dict to `DataProvider` with crypto preferences
- [ ] Add `CONFIGURABLE` dict to `HistoricalDataProvider` (inherits + scheduling + data)
- [ ] Add `CONFIGURABLE` dict to `LiveDataProvider` (inherits + scheduling)
- [ ] Add `CONFIGURABLE` dict to `IndexProvider` (inherits only)
- [ ] Create `SCHEMA_MAP` in registry service mapping `class_subtype` → `CONFIGURABLE`
- [ ] Add `GET /api/registry/config/schema` endpoint
- [ ] Add validation to `PUT /api/registry/config` using schema

### Phase 2: DataHub Integration

- [ ] Add `preferences` to provider query in `load_provider_cls()`
- [ ] Update `DataProvider.__init__` to accept optional `preferences` parameter
- [ ] Store preferences in `_provider_preferences` dict
- [ ] Add `POST /api/datahub/providers/{name}/unload` endpoint

### Phase 3: Scheduling Configuration

- [ ] Read `delay_hours` in `refresh_subscriptions()` for historical providers
- [ ] Apply positive offset to `OffsetCronTrigger`
- [ ] Read `pre_close_seconds` for live providers
- [ ] Apply negative offset to `OffsetCronTrigger`
- [ ] Read `post_close_seconds` for live providers
- [ ] Use in timeout calculation in `get_data()`
- [ ] Add Scheduling tab to UI with provider-type-specific content

### Phase 4: Data Configuration

- [ ] Read `lookback_days` in `_build_reqs_historical()` instead of `DEFAULT_LOOKBACK`
- [ ] Add Data tab to UI with preset radio buttons + custom input

### Phase 5: API Credentials

- [ ] Add `GET /api/registry/config/secret-keys` endpoint
- [ ] Add `PATCH /api/registry/config/secrets` endpoint
- [ ] Call DataHub unload after credential update
- [ ] Replace "Coming Soon" placeholder in API Secrets tab
- [ ] Add dynamic password fields from secret key names

---

## Code Snippets

### Adding CONFIGURABLE to Base Class

```python
# quasar/lib/providers/core.py

class DataProvider(ABC):
    """Base class for all data providers."""

    CONFIGURABLE = {
        "crypto": {
            "preferred_quote_currency": {
                "type": str,
                "default": None,
                "description": "Preferred quote currency for crypto pairs"
            }
        }
    }

    def __init__(self, context: DerivedContext, preferences: dict | None = None):
        self.context = context
        self.preferences = preferences or {}
        # ... existing init code ...
```

### Schema Lookup Utility

```python
# quasar/services/registry/handlers/config.py

from quasar.lib.providers.core import (
    DataProvider, HistoricalDataProvider, LiveDataProvider, IndexProvider
)

SCHEMA_MAP = {
    "Historical": HistoricalDataProvider.CONFIGURABLE,
    "Live": LiveDataProvider.CONFIGURABLE,
    "IndexProvider": IndexProvider.CONFIGURABLE,
    "UserIndex": {},
}

def get_schema_for_subtype(class_subtype: str) -> dict:
    return SCHEMA_MAP.get(class_subtype, DataProvider.CONFIGURABLE)
```

### Schema Endpoint

```python
async def handle_get_config_schema(
    self,
    class_name: str = Query(...),
    class_type: ClassType = Query(...)
) -> ConfigSchemaResponse:
    # Get class_subtype from database
    async with self.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT class_subtype FROM code_registry WHERE class_name = $1 AND class_type = $2",
            class_name, class_type.value
        )
    if not row:
        raise HTTPException(404, "Provider not found")

    schema = get_schema_for_subtype(row['class_subtype'])
    return ConfigSchemaResponse(
        class_name=class_name,
        class_type=class_type.value,
        class_subtype=row['class_subtype'],
        schema=schema
    )
```

### Fetching Preferences in DataHub

```python
# quasar/services/datahub/handlers/providers.py

async def load_provider_cls(self, name: str) -> bool:
    query = """
        SELECT file_path, file_hash, nonce, ciphertext, preferences
        FROM code_registry
        WHERE class_name = $1 AND class_type = 'provider'
    """
    async with self.pool.acquire() as conn:
        row = await conn.fetchrow(query, name)

    # ... existing loading code ...

    preferences = row['preferences'] or {}
    prov = ProviderCls(context=context, preferences=preferences)
    self._provider_preferences[name] = preferences
    self._providers[name] = prov
```

### Applying Scheduling Preferences

```python
# quasar/services/datahub/handlers/collection.py

async def refresh_subscriptions(self):
    # ... existing code ...

    for r in rows:
        prov_type = self._providers[r["provider"]].provider_type
        preferences = self._provider_preferences.get(r["provider"], {})

        if prov_type == ProviderType.HISTORICAL:
            delay_hours = preferences.get("scheduling", {}).get("delay_hours", 0)
            offset_seconds = delay_hours * 3600
        else:  # REALTIME
            pre_close = preferences.get("scheduling", {}).get(
                "pre_close_seconds", DEFAULT_LIVE_OFFSET
            )
            offset_seconds = -1 * pre_close

        trigger = OffsetCronTrigger.from_crontab(r["cron"], offset_seconds=offset_seconds)
        # ... schedule job ...
```

### Applying Lookback Preference

```python
# quasar/services/datahub/handlers/collection.py

async def _build_reqs_historical(self, provider, interval, symbols, exchanges):
    preferences = self._provider_preferences.get(provider, {})
    lookback_days = preferences.get("data", {}).get("lookback_days", DEFAULT_LOOKBACK)

    default_start = yday - timedelta(days=lookback_days)
    # ... rest of method ...
```

---

## Testing

### Unit Test Example

```python
# tests/test_configurable_providers.py

import pytest
from quasar.lib.providers.core import HistoricalDataProvider, LiveDataProvider

def test_historical_configurable_has_delay_hours():
    schema = HistoricalDataProvider.CONFIGURABLE
    assert "scheduling" in schema
    assert "delay_hours" in schema["scheduling"]
    assert schema["scheduling"]["delay_hours"]["default"] == 0

def test_live_configurable_has_pre_close():
    schema = LiveDataProvider.CONFIGURABLE
    assert "scheduling" in schema
    assert "pre_close_seconds" in schema["scheduling"]
    assert schema["scheduling"]["pre_close_seconds"]["default"] == 30
```

### Integration Test Example

```python
# tests/test_config_integration.py

@pytest.mark.asyncio
async def test_schema_endpoint_historical(registry_client):
    response = await registry_client.get(
        "/api/registry/config/schema",
        params={"class_name": "EODHD", "class_type": "provider"}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["class_subtype"] == "Historical"
    assert "delay_hours" in data["schema"]["scheduling"]
```

---

## API Usage Examples

### Get Configuration Schema

```bash
curl "http://localhost:8080/api/registry/config/schema?class_name=EODHD&class_type=provider"
```

Response:
```json
{
  "class_name": "EODHD",
  "class_type": "provider",
  "class_subtype": "historical",
  "schema": {
    "crypto": {
      "preferred_quote_currency": {
        "type": "str",
        "default": null,
        "description": "Preferred quote currency for crypto pairs"
      }
    },
    "scheduling": {
      "delay_hours": {
        "type": "int",
        "default": 0,
        "min": 0,
        "max": 24,
        "description": "Hours after default cron time to run data pulls"
      }
    },
    "data": {
      "lookback_days": {
        "type": "int",
        "default": 8000,
        "min": 1,
        "max": 8000,
        "description": "Days of historical data for new subscriptions"
      }
    }
  }
}
```

### Update Scheduling Preference

```bash
curl -X PUT "http://localhost:8080/api/registry/config?class_name=EODHD&class_type=provider" \
  -H "Content-Type: application/json" \
  -d '{"scheduling": {"delay_hours": 6}}'
```

### Update Credentials

```bash
# Get secret key names
curl "http://localhost:8080/api/registry/config/secret-keys?class_name=EODHD&class_type=provider"
```

Response:
```json
{
  "class_name": "EODHD",
  "class_type": "provider",
  "keys": ["api_token"]
}
```

```bash
# Update credentials
curl -X PATCH "http://localhost:8080/api/registry/config/secrets?class_name=EODHD&class_type=provider" \
  -H "Content-Type: application/json" \
  -d '{"secrets": {"api_token": "new_token_value"}}'
```

Response:
```json
{
  "status": "updated",
  "keys": ["api_token"]
}
```

---

## Common Pitfalls

1. **Don't forget to inherit CONFIGURABLE**: Use `**DataProvider.CONFIGURABLE` in subclasses
2. **Use correct offset sign**: Historical uses positive (delay), live uses negative (pre-close)
3. **Validate before merge**: Check against schema before JSONB merge
4. **New nonce per credential update**: Never reuse nonces for AES-GCM
5. **Call unload after credential update**: Ensures provider reloads with new credentials

---

## Related Documentation

- [Specification](./spec.md) - Feature requirements
- [Research](./research.md) - Technical decisions
- [Data Model](./data-model.md) - Entity definitions
- [OpenAPI Contract](./contracts/openapi.yaml) - API specification
- [config-feature/](../../config-feature/) - Original detailed specifications
