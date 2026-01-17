# Configurable Lookback Period

## Overview

Allow users to configure how much historical data to request when a new symbol subscription is created, overriding the default 8000-day lookback.

## Problem Statement

When a new symbol is subscribed to a historical provider, the system requests a backfill starting from `DEFAULT_LOOKBACK` days ago (`constants.py:11`):

```python
DEFAULT_LOOKBACK = 8000  # ~22 years
default_start = yday - timedelta(days=DEFAULT_LOOKBACK)
```

This aggressive default is problematic:

- **API quota waste**: Requesting 22 years of data for a crypto token that's 2 years old burns API calls on empty responses
- **Slow initial load**: Large backfills delay time-to-first-data for new subscriptions
- **Unnecessary for many strategies**: A momentum strategy might only need 200 days; a mean-reversion strategy might need 60
- **Provider limitations**: Some providers don't have data going back that far, or charge more for extended history

## Solution

Add a `lookback_days` field to provider preferences. This value overrides `DEFAULT_LOOKBACK` when building requests for new subscriptions.

### How It Works

1. User configures `lookback_days: 365` for a provider

2. When a new symbol subscription is created (no prior data in `historical_symbol_state`):
   ```python
   # Current:
   default_start = yday - timedelta(days=DEFAULT_LOOKBACK)  # 8000 days

   # New:
   lookback = provider_preferences.get("data", {}).get("lookback_days", DEFAULT_LOOKBACK)
   default_start = yday - timedelta(days=lookback)
   ```

3. Subsequent incremental pulls are unaffected — they still use `last_updated + 1 day` as the start

### Configuration Scope

- **Per-provider**: Different providers have different data depths and API costs
- **Applies to new subscriptions only**: Existing subscriptions continue incremental updates
- **Does not delete existing data**: Reducing lookback doesn't truncate already-fetched history

### Data Model

Extends the existing `preferences` JSONB column in `code_registry`:

```python
class DataPreferences(BaseModel):
    lookback_days: int = 8000  # Default preserves current behavior

class ProviderPreferences(BaseModel):
    crypto: CryptoPreferences | None = None
    scheduling: SchedulingPreferences | None = None
    data: DataPreferences | None = None
```

### Backend Changes

In `DataHub._build_reqs_historical()` (`handlers/collection.py:166`):

```python
# Current:
default_start = yday - timedelta(days=DEFAULT_LOOKBACK)

# New:
lookback_days = self._get_provider_preference(provider, "data", "lookback_days", DEFAULT_LOOKBACK)
default_start = yday - timedelta(days=lookback_days)
```

---

## UI Design

### Location

Add a **"Data Collection"** section to the "Trading Preferences" tab, or create a dedicated **"Data"** tab in the Provider Config Modal.

### Component: Preset Selector with Custom Input

```
┌─────────────────────────────────────────────────────────────┐
│  Provider Settings: EODHD                              [X]  │
├─────────────────────────────────────────────────────────────┤
│  [Trading Preferences]  [Scheduling]  [API Secrets]         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  QUOTE CURRENCY PREFERENCE                                  │
│  (existing UI from PR #45)                                  │
│                                                             │
│  ─────────────────────────────────────────────────────────  │
│                                                             │
│  HISTORICAL LOOKBACK                                        │
│  ────────────────────                                       │
│  How much historical data to fetch when adding new symbols. │
│  This only affects new subscriptions, not existing data.    │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ ○  1 month (30 days)                                │   │
│  │ ○  3 months (90 days)                               │   │
│  │ ○  1 year (365 days)                                │   │
│  │ ●  3 years (1,095 days)                   ← default │   │
│  │ ○  5 years (1,825 days)                             │   │
│  │ ○  Maximum available (8,000 days)                   │   │
│  │ ○  Custom: [____] days                              │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  ℹ️  With 1,095 days lookback:                      │   │
│  │     • New symbols will fetch data back to Jan 2023  │   │
│  │     • Estimated API calls: ~3 per symbol            │   │
│  │     • Existing subscriptions are not affected       │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│                                        [Cancel] [Save]      │
└─────────────────────────────────────────────────────────────┘
```

### UI Elements

1. **Radio Button Group**
   - Curated presets covering common use cases
   - Clear day counts in parentheses
   - Visual indicator for recommended/default option

2. **Custom Input**
   - Numeric field for power users
   - Validation: 1 ≤ days ≤ 8000
   - Selecting "Custom" focuses the input field

3. **Info Panel**
   - Dynamic preview based on selection
   - Shows the actual start date for context
   - Estimates API impact (if calculable)
   - Reinforces that existing data is unaffected

### Preset Rationale

| Preset | Days | Use Case |
|--------|------|----------|
| 1 month | 30 | Quick testing, live-only strategies |
| 3 months | 90 | Short-term momentum, recent trends |
| 1 year | 365 | Seasonal analysis, standard backtests |
| 3 years | 1,095 | **Default** — balances depth vs. cost |
| 5 years | 1,825 | Longer-term strategies, cycle analysis |
| Maximum | 8,000 | Full history (current behavior) |

### Why This UI Works

- **Guided choices**: Presets help users who don't know what they need
- **Clear tradeoffs**: Day counts make the impact concrete
- **Escape hatch**: Custom input for specific requirements
- **Reassurance**: Info panel confirms existing data is safe
- **Sensible default**: 3 years is a reasonable starting point (vs. 22 years)

### Edge Cases

- **Lookback exceeds provider history**: Provider returns what it has; no error
- **Lookback = 0**: Invalid; minimum 1 day enforced
- **Crypto vs. Equities**: Same setting applies; crypto just has fewer calendar gaps
- **Existing subscriptions**: Unaffected; only new symbols use this setting

---

## Implementation Notes

### Existing Infrastructure

- `preferences` JSONB column exists in `code_registry` (PR #45)
- `DEFAULT_LOOKBACK` constant in `constants.py`
- Request building in `_build_reqs_historical()` already checks for new vs. existing

### New Work Required

1. **Backend**: Read `lookback_days` from preferences in `_build_reqs_historical()`
2. **API**: No new endpoints — existing PATCH `/api/registry/config` works
3. **Frontend**: Add radio group + custom input to Trading Preferences tab
4. **Validation**: Ensure 1 ≤ lookback_days ≤ 8000
5. **Consider**: Change system default from 8000 to 1095 (3 years)

### Testing

- Unit test: Lookback preference overrides default correctly
- Unit test: Existing subscriptions ignore lookback setting
- Integration test: New subscription uses configured lookback
- UI test: Presets and custom input persist correctly

---

## Design Decisions

1. **System default remains 8000 days** — preserves current behavior for users who don't configure; the UI presets guide users toward more reasonable values without forcing a change

2. **Re-backfill is out of scope** — if a user increases lookback after initial subscription, fetching the additional history is a separate feature (covered by existing issue)

3. **No per-asset-class suggestions** — providers return only what's available anyway; a 2-year-old crypto token won't return 8000 days regardless of the setting
