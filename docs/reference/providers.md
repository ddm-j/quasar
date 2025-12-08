# Core Library: Providers

Base classes and provider implementations.

## Provider base classes

::: quasar.lib.providers.core

## EODHD provider

::: quasar.lib.providers.eodhd

## Kraken provider

::: quasar.lib.providers.kraken

## Developer harness

Use `quasar.lib.providers.devtools` to exercise historical and live provider subclasses without bringing up the full stack.

- Python API: `run_historical(config)` / `run_live(config)` / `run_symbols(config)` always validate outputs (strict by default).
- CLI (bars): `python -m quasar.lib.providers.devtools bars --config ./config.json --limit 100`
- CLI (symbols): `python -m quasar.lib.providers.devtools symbols --config ./config.json`
- Config must include `provider_type` (`historical` or `live`) and `provider`.
- Strict mode is on by default; disable with `--no-strict` (CLI) or `strict=False` (API).
- Example configs and stub providers live under `quasar/lib/providers/devtools/examples/` and `quasar.lib.providers.devtools.stubs`.

Minimal examples:

```bash
python -m quasar.lib.providers.devtools bars \
  --config quasar/lib/providers/devtools/examples/historical_stub.json
```

Fetch symbols:

```bash
python -m quasar.lib.providers.devtools symbols \
  --config quasar/lib/providers/devtools/examples/historical_stub.json
```

```python
from quasar.lib.providers.devtools import run_live

config = {
    "provider_type": "live",
    "provider": "quasar.lib.providers.devtools.stubs:LiveStub",
    "interval": "1min",
    "symbols": ["AAA", "BBB"],
    "secrets": {},
}

bars = run_live(config)
```

