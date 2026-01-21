# Architectural Patterns

This document describes the patterns and conventions used throughout the Quasar codebase.

## Inheritance Hierarchy

The codebase uses multi-level ABC inheritance for interface contracts:

- `DataProvider(ABC)` base: `quasar/lib/providers/core.py:113`
  - `HistoricalDataProvider(DataProvider)`: `quasar/lib/providers/core.py:231`
  - `LiveDataProvider(DataProvider)`: `quasar/lib/providers/core.py:245`
- `DatabaseHandler(ABC)`: `quasar/lib/common/database_handler.py:8` - async pool management
- `APIHandler(ABC)`: `quasar/lib/common/api_handler.py:8` - FastAPI lifecycle

Services inherit from both `DatabaseHandler` and `APIHandler` for consistent resource management.

## Provider Registration (Decorator Pattern)

Providers auto-register via `@register_provider` decorator:

```python
@register_provider
class MyProvider(HistoricalDataProvider):
    name = "MY_PROVIDER"
```

- Registry definition: `quasar/lib/providers/__init__.py:16-24`
- Examples: `quasar/lib/providers/examples/eodhd.py:8`, `kraken.py:15`, `databento.py:12`

## Constructor-Based Dependency Injection

Dependencies passed through constructors, enabling mock injection for tests:

- `DataProvider` receives `DerivedContext`: `quasar/lib/providers/core.py:104-111`
- `DatabaseHandler` accepts `dsn` or existing `pool`: `quasar/lib/common/database_handler.py:19-35`
- Test fixtures inject mocks: `tests/conftest.py`

## Async Context Managers

Resources use `__aenter__`/`__aexit__` for lifecycle management:

- Provider session management: `quasar/lib/providers/core.py:171-183`
- WebSocket connections: `quasar/lib/providers/core.py:360-363`

Pattern ensures proper cleanup on exceptions.

## Async Generators for Streaming

`AsyncIterator[Bar]` enables memory-efficient data streaming:

- `get_history()` yields bars: `quasar/lib/providers/core.py:237-242`
- Implementation examples: `quasar/lib/providers/examples/databento.py:151-209`

## Rate Limiting & Concurrency

Class-level `RATE_LIMIT` and `CONCURRENCY` with `AsyncLimiter` and `Semaphore`:

- Class variables: `quasar/lib/providers/core.py:100-101`
- Limiter init: `quasar/lib/providers/core.py:104-111`
- Usage tracking: `quasar/lib/providers/core.py:144-148`
- Per-provider overrides: `quasar/lib/providers/examples/databento.py:16`

## Secret Management (Context-Based)

Centralized encrypted secrets via `SystemContext` singleton and `DerivedContext`:

- `SystemContext` with HKDF key derivation: `quasar/lib/common/context.py:6-48`
- `DerivedContext` for runtime decryption: `quasar/lib/common/context.py:51-99`
- `SecretStore` multi-mode loading (local/auto/aws): `quasar/lib/common/secret_store.py:30-71`

## Data Transfer Objects

Lightweight type-safe structures without Pydantic overhead for internal data:

- `Bar(TypedDict)`: `quasar/lib/providers/core.py:38-42`
- `Req(NamedTuple)`: `quasar/lib/providers/core.py:45-46`
- `SymbolInfo(TypedDict)`: `quasar/lib/providers/core.py:49-63`

Pydantic reserved for API boundaries: `quasar/services/*/schemas.py`

## Enum Pattern with Validation

Generated enums from YAML with normalization and runtime guards:

- Enums with aliases: `quasar/lib/enums.py:6-57`
- Runtime validation against DB: `quasar/lib/common/enum_guard.py:1-77`
- Generation: `make enums` runs `scripts/gen_enums.py`

## Custom Timeout Decorator

Function-level async timeout enforcement:

- Decorator: `quasar/lib/providers/core.py:56-88`
- Usage: `quasar/lib/providers/core.py:348`

## Trading Calendar Abstraction

Pluggable calendars for different asset classes. Assets without an exchange (e.g., crypto)
default to "always open" behavior:

- `ForexCalendar` (24/5): `quasar/lib/common/calendar.py:21-79`
- Registration: `quasar/lib/common/calendar.py:81-85`

## Matcher & Mapper Pattern

Specialized classes for identity resolution:

- `IdentityMatcher`: `quasar/services/registry/matcher.py:26-150`
- `AutomatedMapper`: `quasar/services/registry/mapper.py:35-200`

Both inherit `DatabaseHandler` for pool access.

## Testing Patterns

Centralized fixtures in `tests/conftest.py`:

- `mock_asyncpg_pool`, `mock_asyncpg_conn` - database mocking
- `mock_secret_store`, `mock_system_context` - secret mocking
- `datahub_with_mocks`, `registry_with_mocks` - fully-mocked service instances
- `datahub_client`, `registry_client` - FastAPI TestClient instances

## Multi-Mode Configuration

Environment-based config with fallback strategies:

- Secret modes (auto/local/aws): `quasar/lib/common/secret_store.py:27-71`
- Environment variables: `QUASAR_SECRET_MODE`, `CORS_ORIGINS`

## Provider Type Dispatch

Enum-based behavior dispatch:

- `ProviderType.HISTORICAL` / `ProviderType.REALTIME`: `quasar/lib/providers/core.py:51-54`
- Routing by type: `quasar/lib/providers/core.py:155-165`
