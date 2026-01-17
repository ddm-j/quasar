# Data Model: Configurable Providers

**Feature**: 001-configurable-providers
**Date**: 2026-01-17

## Overview

This feature does not introduce new database tables. It leverages the existing `preferences` JSONB column in `code_registry` and adds Python-level schema declarations in provider base classes.

---

## Entities

### 1. Provider Preferences (Database)

**Storage**: `code_registry.preferences` (existing JSONB column)

**Structure**:
```
preferences: {
    crypto?: {
        preferred_quote_currency?: string
    },
    scheduling?: {
        // Historical providers only
        delay_hours?: int (0-24)

        // Live providers only
        pre_close_seconds?: int (0-300)
        post_close_seconds?: int (0-60)
    },
    data?: {
        // Historical providers only
        lookback_days?: int (1-8000)
    }
}
```

**Defaults**: All fields are optional. Missing fields use defaults from `CONFIGURABLE` schema.

**Constraints**:
- Field presence validated against provider `class_subtype`
- Value ranges enforced at API layer
- JSONB merge preserves existing fields on partial update

---

### 2. CONFIGURABLE Schema (Python)

**Location**: `quasar/lib/providers/core.py`

**Purpose**: Declare available configuration options per provider type. Used for:
- API validation
- Schema endpoint response
- Frontend dynamic UI generation

**Structure**:
```python
CONFIGURABLE = {
    "<category>": {
        "<field>": {
            "type": <python_type>,        # int, str, float, bool
            "default": <value>,           # Default if not configured
            "min": <value>,               # Optional: minimum value
            "max": <value>,               # Optional: maximum value
            "description": <string>       # Human-readable description
        }
    }
}
```

**Inheritance**:
```
DataProvider.CONFIGURABLE
  └─ crypto.preferred_quote_currency

HistoricalDataProvider.CONFIGURABLE
  └─ (inherits DataProvider)
  └─ scheduling.delay_hours
  └─ data.lookback_days

LiveDataProvider.CONFIGURABLE
  └─ (inherits DataProvider)
  └─ scheduling.pre_close_seconds
  └─ scheduling.post_close_seconds

IndexProvider.CONFIGURABLE
  └─ (inherits DataProvider only)
```

---

### 3. Encrypted Credentials (Database)

**Storage**: `code_registry.nonce`, `code_registry.ciphertext` (existing columns)

**Structure**:
```
nonce: bytes (12 bytes, random per encryption)
ciphertext: bytes (AES-256-GCM encrypted JSON)
```

**Encrypted payload example**:
```json
{
    "api_key": "abc123",
    "api_secret": "xyz789"
}
```

**Key derivation**: HKDF from `file_hash` (unchanged by credential updates)

**Operations**:
- `GET /secret-keys`: Decrypt, return `list(payload.keys())` (names only)
- `PATCH /secrets`: Re-encrypt with new nonce, store updated `(nonce, ciphertext)`

---

## Entity Relationships

```
code_registry (1) ──────── (1) preferences
     │
     │  class_subtype
     │
     ▼
CONFIGURABLE Schema ────── Validates allowed fields
```

---

## Field Definitions

### preferences.crypto

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `preferred_quote_currency` | string | null | Optional | Preferred quote currency for crypto pairs |

**Applicable to**: All provider types (Historical, Live, IndexProvider)

---

### preferences.scheduling (Historical)

| Field | Type | Default | Min | Max | Description |
|-------|------|---------|-----|-----|-------------|
| `delay_hours` | int | 0 | 0 | 24 | Hours after default cron time to run data pulls |

**Applicable to**: Historical providers only

---

### preferences.scheduling (Live)

| Field | Type | Default | Min | Max | Description |
|-------|------|---------|-----|-----|-------------|
| `pre_close_seconds` | int | 30 | 0 | 300 | Seconds before bar close to start listening |
| `post_close_seconds` | int | 5 | 0 | 60 | Seconds after bar close to continue listening |

**Applicable to**: Live providers only

---

### preferences.data (Historical)

| Field | Type | Default | Min | Max | Description |
|-------|------|---------|-----|-----|-------------|
| `lookback_days` | int | 8000 | 1 | 8000 | Days of historical data for new subscriptions |

**Applicable to**: Historical providers only

---

## Validation Rules

### Type Validation
- `delay_hours`: Must be integer, not string
- `lookback_days`: Must be integer, not string
- `preferred_quote_currency`: Must be string or null

### Range Validation
- `delay_hours`: `0 ≤ value ≤ 24`
- `pre_close_seconds`: `0 ≤ value ≤ 300`
- `post_close_seconds`: `0 ≤ value ≤ 60`
- `lookback_days`: `1 ≤ value ≤ 8000`

### Provider Type Validation
- Historical providers: May set `scheduling.delay_hours`, `data.lookback_days`
- Live providers: May set `scheduling.pre_close_seconds`, `scheduling.post_close_seconds`
- Index providers: May only set `crypto.*` fields
- Setting invalid fields for provider type returns 400 error

---

## State Transitions

### Preference Updates

```
┌───────────────┐    PUT /config    ┌───────────────┐
│   No prefs    │ ───────────────▶ │ Has prefs     │
│ (defaults)    │                   │ (configured)  │
└───────────────┘                   └───────────────┘
        │                                  │
        │                                  │ PUT /config (partial)
        │                                  ▼
        │                           ┌───────────────┐
        │                           │ Updated prefs │
        │                           └───────────────┘
        │
        └──────────────────────────────────┘
              (defaults always apply for missing fields)
```

### Credential Updates

```
┌───────────────┐   PATCH /secrets   ┌───────────────┐
│ Credentials A │ ─────────────────▶ │ Credentials B │
│ (nonce1)      │                    │ (nonce2)      │
└───────────────┘                    └───────────────┘
                                            │
                                            ▼
                                     POST /unload
                                            │
                                            ▼
                                     ┌───────────────┐
                                     │ Provider      │
                                     │ unloaded      │
                                     └───────────────┘
                                            │
                                            │ Next data request
                                            ▼
                                     ┌───────────────┐
                                     │ Provider      │
                                     │ reloaded with │
                                     │ new creds     │
                                     └───────────────┘
```

---

## Database Operations

### Read Preferences
```sql
SELECT preferences FROM code_registry
WHERE class_name = $1 AND class_type = $2;
```

### Update Preferences (merge)
```sql
UPDATE code_registry
SET preferences = jsonb_strip_nulls(COALESCE(preferences, '{}'::jsonb) || $3::jsonb)
WHERE class_name = $1 AND class_type = $2;
```

### Get Class Subtype
```sql
SELECT class_subtype FROM code_registry
WHERE class_name = $1 AND class_type = $2;
```

### Update Credentials
```sql
UPDATE code_registry
SET nonce = $3, ciphertext = $4
WHERE class_name = $1 AND class_type = $2;
```

---

## Pydantic Schemas

### Request/Response Models

```python
# Scheduling preferences (provider-type-specific)
class HistoricalSchedulingPreferences(BaseModel):
    delay_hours: Optional[int] = Field(default=None, ge=0, le=24)

class LiveSchedulingPreferences(BaseModel):
    pre_close_seconds: Optional[int] = Field(default=None, ge=0, le=300)
    post_close_seconds: Optional[int] = Field(default=None, ge=0, le=60)

# Data preferences (historical only)
class DataPreferences(BaseModel):
    lookback_days: Optional[int] = Field(default=None, ge=1, le=8000)

# Full preferences model
class ProviderPreferences(BaseModel):
    crypto: Optional[CryptoPreferences] = None
    scheduling: Optional[dict] = None  # Validated separately by type
    data: Optional[DataPreferences] = None

# Schema response model
class ConfigSchemaResponse(BaseModel):
    class_name: str
    class_type: str
    class_subtype: str
    schema: dict

# Secret keys response
class SecretKeysResponse(BaseModel):
    class_name: str
    class_type: str
    keys: list[str]

# Secret update request
class SecretsUpdateRequest(BaseModel):
    secrets: dict[str, str]  # All keys required
```

---

## Migration Notes

**No database migration required.**

- `preferences` JSONB column already exists (PR #45)
- New preference keys are simply stored in existing JSON structure
- Missing keys default to values in `CONFIGURABLE` schema
- Existing `crypto.preferred_quote_currency` structure preserved
