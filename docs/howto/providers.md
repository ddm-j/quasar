# How to Build a Provider Adaptor (Novice Friendly)

This guide shows you how to turn any market data source into a Quasar “provider adaptor.” You will learn what to implement, see small code examples, and test your work with the devtools harness. No prior experience with WebSockets or advanced Python is required.

---

## 1) What you are building
- A **provider adaptor** converts an external data API (HTTP or WebSocket) into Quasar’s standard bar format.
- Output is always OHLCV bars with these keys: `ts`, `sym`, `o`, `h`, `l`, `c`, `v`.
- Bars flow into the platform so strategies and the DataHub can consume them.

## 2) Glossary (plain language)
- **Adaptor / provider**: Your class that talks to the vendor API and yields bars.
- **Symbol**: The market identifier string that your vendor uses (e.g., `AAPL`, `BTC/USD`).
- **Bar**: One time bucket of data (Open, High, Low, Close, Volume).
- **Interval**: How long each bar is (`1min`, `5min`, `15min`, `30min`, `1h`, `4h`, `1d`, `1w`, `1M`).
- **Historical vs live**: Historical pulls past bars over HTTP; live streams current bars over WebSocket.
- **Secrets / context**: API keys or tokens provided through `DerivedContext`; in dev you can just pass a plain dict.

## 3) Prereqs and where code lives
- Python 3.11+, dependencies from `pyproject.toml` installed.
- You only need a Python file that can be imported. Example layout:
  - `my_dev_dir/your_adaptor_code.py` (contains your provider class)
  - `my_dev_dir/your_config.json` (points to the class with a dotted path like `"provider": "your_adaptor_code:MyProvider"`, see later sections)
- The devtools (see later sections) can then be ran from this folder

## 4) Choose your provider type
| Use this when | Base class | Typical transport |
| --- | --- | --- |
| You fetch past bars | `HistoricalDataProvider` | HTTP/REST |
| You stream current bars | `LiveDataProvider` | WebSocket |

Tip: If your source offers both, start with historical to validate symbols and bar shape, then add live.

## 5) Minimal skeletons
### Historical template
```python
from datetime import date, datetime, timezone
from quasar.lib.providers import HistoricalDataProvider, Interval, Bar, SymbolInfo

class MyHistorical(HistoricalDataProvider):
    name = "MY_HIST"  # must be unique

    async def get_available_symbols(self) -> list[SymbolInfo]:
        return []  # fill with your symbols

    async def get_history(self, sym: str, start: date, end: date, interval: Interval):
        # call your HTTP API here, then yield bars oldest → newest
        yield Bar(ts=start, sym=sym, o=100, h=101, l=99, c=100, v=1000)
```
Optional: override `get_history_many(reqs)` if your API supports batching; otherwise the base class loops `get_history`.

### Live template
```python
from quasar.lib.providers import LiveDataProvider, Interval, Bar
import websockets, json

class MyLive(LiveDataProvider):
    name = "MY_LIVE"
    close_buffer_seconds = 2  # keep listening after bar close

    async def get_available_symbols(self):
        return []  # fetch from your REST endpoint if available

    async def _connect(self):
        return await websockets.connect("wss://example")

    async def _subscribe(self, interval: Interval, symbols: list[str]) -> dict:
        return {"op": "subscribe", "symbols": symbols, "interval": interval}

    async def _unsubscribe(self, symbols: list[str]) -> dict:
        return {"op": "unsubscribe", "symbols": symbols}

    async def _parse_message(self, message: str) -> list[Bar]:
        data = json.loads(message)
        # convert incoming payload into one or more Bar objects
        return []
```
The base `get_live` handles: connect → subscribe → listen until the next interval boundary → unsubscribe → return one bar per symbol.

## 6) Implement the required methods (checklist)
### For both types
- `name`: short, unique identifier (e.g., `EODHD`, `KRAKEN`).
- `get_available_symbols`: return a list of `SymbolInfo` dicts (provider name, provider_id if any, symbol, name, exchange, asset_class, base_currency, quote_currency). Keep strings non-empty in strict mode.

### Historical specifics
- Implement `get_history(sym, start, end, interval)` and yield bars **oldest → newest**, covering the requested range.
- Support at least the intervals your API can serve; validate unsupported intervals with a clear error.
- Keep numbers finite; timestamps should be timezone-aware UTC when possible.

### Live specifics
- `close_buffer_seconds`: extra seconds to wait after the bar boundary so you capture the last ticks.
- `_connect`: open the WebSocket and return the connection.
- `_subscribe` / `_unsubscribe`: build payloads your venue expects.
- `_parse_message`: return a list of Bars; ignore heartbeat/keepalive messages by returning `None` or `[]`.

## 7) Learn from the built-ins (short highlights)
- `quasar.lib.providers.examples.eodhd.EODHDProvider` (historical): maps Quasar intervals to EODHD, filters exchanges, and yields UTC timestamps.
- `quasar.lib.providers.examples.kraken.KrakenProvider` (live): builds WebSocket subscribe messages with interval mapping, parses OHLC payloads, trims timestamp precision, and returns bars.

## 8) Test with devtools (fast feedback)
Devtools validate your output shape. Run the commands from the directory where Python can import your adaptor and where your config lives (for the built-in examples, that’s the repo root).

### Example configs (runnable)
Historical stub (uses the included deterministic stub):
```json
{
  "provider_type": "historical",
  "provider": "quasar.lib.providers.devtools.stubs:HistoricalStub",
  "secrets": {},
  "requests": [
    {"sym": "TEST.A", "start": "2024-01-01", "end": "2024-01-03", "interval": "1d"}
  ]
}
```

Live stub:
```json
{
  "provider_type": "live",
  "provider": "quasar.lib.providers.devtools.stubs:LiveStub",
  "secrets": {},
  "interval": "1min",
  "symbols": ["AAA", "BBB"]
}
```

### CLI
```bash
python -m quasar.lib.providers.devtools bars --config path/to/hist.json --limit 100
python -m quasar.lib.providers.devtools bars --config path/to/live.json --limit 10
python -m quasar.lib.providers.devtools symbols --config path/to/hist.json
```
- `provider_type` in the config decides historical vs live.
- `--limit` caps collected bars (defaults: 500 hist, 50 live).
- Strict validation is on by default; add `--no-strict` to relax checks.

### Python API (same validation)
```python
from quasar.lib.providers.devtools import run_historical, run_live, run_symbols

bars = run_historical(config_dict)
ticks = run_live(config_dict)
symbols = run_symbols(config_dict)
```

## 9) Final checklist and troubleshooting
- Bars include all fields: `ts, sym, o, h, l, c, v`; numbers are finite; timestamps are UTC.
- Historical bars are sorted oldest → newest and match the requested window.
- Live adaptor unsubscribes cleanly; `_parse_message` ignores heartbeats and returns Bars only.
- `get_available_symbols` returns meaningful metadata; required string fields are present.
- Devtools validations pass (try strict first, then `--no-strict` only for debugging).
- Unsupported intervals or bad inputs raise clear errors.

Next: once your adaptor passes devtools it is ready to be registered (uploaded) to the platform. 

