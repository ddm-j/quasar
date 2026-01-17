# Scheduling Configuration

## Overview

Allow users to configure timing behavior for data providers:
- **Historical providers**: When to run data pulls (offset from default cron)
- **Live providers**: When to start and stop listening for bar data (offsets from bar close)

Both configuration types live under the `scheduling` preference key, with different fields depending on provider type.

---

## Historical Providers

### Problem Statement

Historical data providers pull bars for the previous day. The default cron schedule runs at midnight UTC (for daily bars), which works for most providers since yesterday's data is typically available by then.

However, users may want to run pulls at different times:
- **API rate limits**: Spread pulls across the day to avoid quota exhaustion
- **System load**: Stagger multiple providers to reduce concurrent load
- **Provider quirks**: Some providers may have data available later than midnight
- **Operational convenience**: Run during business hours for easier monitoring

### Solution

Add a `delay_hours` field to scheduling preferences. This offset is applied to the interval's default cron schedule using the existing `OffsetCronTrigger` infrastructure.

### How It Works

1. Each interval has a default cron in `accepted_intervals`:
   - `1d` → `0 0 * * *` (midnight UTC)
   - `1h` → `0 * * * *` (top of hour)

2. User configures `scheduling.delay_hours: 6` for a provider

3. When scheduling jobs, the system applies a **positive offset**:
   ```python
   offset_seconds = delay_hours * 3600
   OffsetCronTrigger.from_crontab("0 0 * * *", offset_seconds=offset_seconds)
   # Results in job firing at 06:00 UTC instead of 00:00 UTC
   ```

4. The pull still requests `end=yesterday` — the offset only changes **when** the pull runs, not **what** it pulls

### Configuration

```json
{
  "scheduling": {
    "delay_hours": 6
  }
}
```

| Field | Type | Default | Min | Max | Description |
|-------|------|---------|-----|-----|-------------|
| `delay_hours` | int | 0 | 0 | 24 | Hours after default cron time to run |

---

## Live Providers

### Problem Statement

Live data providers connect to real-time feeds (e.g., WebSockets) to capture bar data as it forms. Two timing parameters control this behavior:

1. **Pre-close buffer**: When to start listening BEFORE the bar close time
2. **Post-close buffer**: How long to keep listening AFTER the bar close time

Currently these are hardcoded:
- `DEFAULT_LIVE_OFFSET = 30` seconds (pre-close, in constants.py)
- `close_buffer_seconds = 5` seconds (post-close, per-provider property)

Users may need to adjust these based on:
- **Provider latency**: Slow APIs may need more pre-close time to establish connection
- **Market characteristics**: Some markets (crypto, forex) accept trades seconds after close
- **Data completeness**: Laggy feeds may send final ticks several seconds after bar close

### Solution

Add `pre_close_seconds` and `post_close_seconds` fields to scheduling preferences for live providers.

### How It Works

**Pre-close buffer** (`pre_close_seconds`):

Controls when the cron job fires relative to bar close. Applied as a negative offset:

```python
offset_seconds = -1 * pre_close_seconds
OffsetCronTrigger.from_crontab("0 * * * *", offset_seconds=offset_seconds)
# For 1-hour bars with pre_close_seconds=45:
# Job fires at XX:59:15 instead of XX:00:00
```

**Post-close buffer** (`post_close_seconds`):

Controls how long the provider keeps listening after bar close:

```python
# In LiveDataProvider.get_live()
bar_end = get_next_interval_timestamp(interval)
cutoff = bar_end + timedelta(seconds=post_close_seconds)

# Listen until cutoff
async for message in conn:
    if datetime.now(timezone.utc) >= cutoff:
        break
```

**Combined timeout calculation**:

```python
# Total time the job may run
timeout = pre_close_seconds + post_close_seconds + buffer
# Example: 45 + 10 + 30 = 85 seconds
```

### Configuration

```json
{
  "scheduling": {
    "pre_close_seconds": 45,
    "post_close_seconds": 10
  }
}
```

| Field | Type | Default | Min | Max | Description |
|-------|------|---------|-----|-----|-------------|
| `pre_close_seconds` | int | 30 | 0 | 300 | Seconds before bar close to start listening |
| `post_close_seconds` | int | 5 | 0 | 60 | Seconds after bar close to continue listening |

---

## UI Design

### Location

**"Scheduling"** tab in the Provider Config Modal. Content varies based on provider type (`class_subtype`).

### Historical Provider UI

```
┌─────────────────────────────────────────────────────────────┐
│  Provider Settings: EODHD                              [X]  │
├─────────────────────────────────────────────────────────────┤
│  [Trading]  [Scheduling]  [Data]  [API Secrets]             │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Data Pull Timing                                           │
│  ────────────────                                           │
│  Configure when this provider fetches new data.             │
│  Data for the previous day is typically available at        │
│  midnight UTC.                                              │
│                                                             │
│  Delay after midnight (UTC):                                │
│                                                             │
│  ○────────────────●───────────────────○                     │
│  0h              6h                  24h                    │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  ⏰ Daily data will be pulled at 06:00 UTC          │   │
│  │     (01:00 AM in your timezone, EST)                │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  Quick select: [Midnight] [6 AM] [Noon] [Custom...]         │
│                                                             │
│                                        [Cancel] [Save]      │
└─────────────────────────────────────────────────────────────┘
```

**Elements:**
- Slider (0-24 hours) with tick marks
- Live preview showing exact UTC time + user's local timezone
- Preset buttons for common values

### Live Provider UI

```
┌─────────────────────────────────────────────────────────────┐
│  Provider Settings: KRAKEN                             [X]  │
├─────────────────────────────────────────────────────────────┤
│  [Trading]  [Scheduling]  [API Secrets]                     │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Live Data Timing                                           │
│  ────────────────                                           │
│  Configure when to start and stop listening for bar data.   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │         BAR PERIOD            │ CLOSE │  BUFFER     │   │
│  │  ◄─────────────────────────── │ ───── │ ─────────►  │   │
│  │                          ▲         ▲          ▲     │   │
│  │                     start listening │     stop listening │
│  │                        (45s before) │       (10s after)  │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  Start listening before bar close:                          │
│  ┌──────────┐                                               │
│  │ 45       │ seconds    (default: 30)                      │
│  └──────────┘                                               │
│                                                             │
│  Continue listening after bar close:                        │
│  ┌──────────┐                                               │
│  │ 10       │ seconds    (default: 5)                       │
│  └──────────┘                                               │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  ℹ️  For 1-hour bars closing at 14:00:00 UTC:       │   │
│  │     • Connection opens at 13:59:15 UTC              │   │
│  │     • Listening stops at 14:00:10 UTC               │   │
│  │     • Total window: 55 seconds                      │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│                                        [Cancel] [Save]      │
└─────────────────────────────────────────────────────────────┘
```

**Elements:**
- Visual timeline diagram showing the listening window
- Two numeric inputs with clear labels
- Defaults shown inline for reference
- Preview box with concrete example times

### Why This UI Works

**Historical:**
- Slider is intuitive for "pick a time" use case
- No cron knowledge required
- Timezone conversion prevents confusion

**Live:**
- Visual diagram explains the timing model
- Separate inputs for before/after are clearer than a single "buffer" concept
- Example calculation makes the impact concrete

---

## Backend Changes

### Historical Provider Scheduling

**In `DataHub.refresh_subscriptions()`** (`handlers/collection.py`):

```python
# Current:
offset_seconds = 0 if prov_type == ProviderType.HISTORICAL else -1*DEFAULT_LIVE_OFFSET

# New:
if prov_type == ProviderType.HISTORICAL:
    preferences = self._provider_preferences.get(provider_name, {})
    delay_hours = preferences.get("scheduling", {}).get("delay_hours", 0)
    offset_seconds = delay_hours * 3600
else:
    # Live provider handling (see below)
```

### Live Provider Scheduling

**In `DataHub.refresh_subscriptions()`**:

```python
elif prov_type == ProviderType.REALTIME:
    preferences = self._provider_preferences.get(provider_name, {})
    pre_close = preferences.get("scheduling", {}).get("pre_close_seconds", DEFAULT_LIVE_OFFSET)
    offset_seconds = -1 * pre_close
```

**In `DataHub.get_data()`** (for live providers):

```python
preferences = self._provider_preferences.get(provider_name, {})
pre_close = preferences.get("scheduling", {}).get("pre_close_seconds", DEFAULT_LIVE_OFFSET)
post_close = preferences.get("scheduling", {}).get("post_close_seconds", 5)

timeout = pre_close + post_close + 30  # 30s safety buffer
kwargs = {'timeout': timeout}
```

**In `LiveDataProvider.get_live()`** (or override per-provider):

```python
# Currently uses self.close_buffer_seconds property
# Could be updated to accept post_close_seconds as parameter
# Or: provider reads from self.preferences if available
```

### Schema Declaration

**In `quasar/lib/providers/core.py`**:

```python
class HistoricalDataProvider(DataProvider):
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
        }
    }


class LiveDataProvider(DataProvider):
    CONFIGURABLE = {
        **DataProvider.CONFIGURABLE,
        "scheduling": {
            "pre_close_seconds": {
                "type": int,
                "default": 30,
                "min": 0,
                "max": 300,
                "description": "Seconds before bar close to start listening"
            },
            "post_close_seconds": {
                "type": int,
                "default": 5,
                "min": 0,
                "max": 60,
                "description": "Seconds after bar close to continue listening"
            }
        }
    }
```

---

## Implementation Notes

### Existing Infrastructure

- `OffsetCronTrigger` already supports positive and negative offsets (`quasar/lib/common/offset_cron.py`)
- `preferences` JSONB column exists in `code_registry` (PR #45)
- Config modal with tabs exists (`web/src/views/registry/ProviderConfigModal.js`)
- `class_subtype` distinguishes Historical/Live/IndexProvider

### New Work Required

1. **Backend**:
   - Add `CONFIGURABLE` declarations to base provider classes
   - Read scheduling preferences in `refresh_subscriptions()`
   - Read post-close preference in `get_data()` for live providers
   - Add validation for scheduling fields in config handler

2. **Frontend**:
   - Add "Scheduling" tab to config modal
   - Render different UI based on `class_subtype`
   - Historical: slider with preview
   - Live: dual inputs with timeline diagram

3. **Validation**:
   - Historical: 0 ≤ delay_hours ≤ 24
   - Live: 0 ≤ pre_close_seconds ≤ 300, 0 ≤ post_close_seconds ≤ 60

### Testing

- Unit test: Historical offset applies correctly to cron trigger
- Unit test: Live pre-close offset applies as negative value
- Unit test: Live timeout calculation uses both pre and post values
- Integration test: Jobs fire at expected times with configured offsets
- UI test: Correct fields render based on provider type

---

## Design Decisions

1. **Separate fields for live pre/post** — Clearer than a single "buffer" value; users understand "before" and "after" intuitively

2. **Hours for historical, seconds for live** — Historical delays are typically hours; live buffers are typically seconds. Using appropriate units avoids awkward conversions

3. **Defaults match current hardcoded values** — No behavior change for unconfigured providers:
   - Historical delay: 0 (runs at default cron time)
   - Live pre-close: 30 seconds (matches `DEFAULT_LIVE_OFFSET`)
   - Live post-close: 5 seconds (matches typical `close_buffer_seconds`)

4. **Max pre-close of 300 seconds (5 minutes)** — Generous upper bound for slow connections; unlikely anyone needs more

5. **Max post-close of 60 seconds** — Even the laggiest feeds should deliver within a minute of bar close
