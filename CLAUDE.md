# Quasar

Automated trading platform for API-based strategy execution and portfolio management across multiple brokers.

## Tech Stack

**Backend**: Python 3.12+, FastAPI, asyncpg, APScheduler, aiohttp, websockets
**Frontend**: React 18, Vite, Redux, CoreUI Pro
**Database**: PostgreSQL 17 with TimescaleDB
**Infrastructure**: Docker Compose, Nginx (production)

## Project Structure

```
quasar/                    # Main Python package
  lib/
    common/                # Shared utilities (database, API, secrets, calendar)
    providers/             # Data provider abstractions and examples
      core.py              # Base classes: DataProvider, HistoricalDataProvider, LiveDataProvider
      examples/            # Built-in providers: EODHD, Kraken, Databento
      devtools/            # CLI for provider development and testing
    enums.py               # Generated enums (AssetClass, Interval)
  services/
    registry/              # Asset management, code uploads, mappings (port 8080)
    datahub/               # Data aggregation, job scheduling (port 8081)
  seeds/manifests/         # Sample configuration manifests
web/                       # React frontend
  src/views/               # Page components
  src/components/          # Reusable UI components
tests/                     # pytest test suite
db/schema/                 # SQL migration files (00-09)
docs/                      # MkDocs documentation
scripts/gen_enums.py       # Enum generation from YAML
```

## Key Entry Points

| Service  | File                              | Port | Purpose                    |
|----------|-----------------------------------|------|----------------------------|
| Registry | `quasar/services/registry/app.py` | 8080 | Assets, code, mappings API |
| DataHub  | `quasar/services/datahub/app.py`  | 8081 | Data collection, scheduling|
| Frontend | `web/src/index.js`                | 3000 | React dashboard            |

## Commands

### Python
```bash
pip install -e ".[dev]"                    # Install for development
pytest                                     # Run tests
pytest --cov=quasar --cov-report=html      # Run with coverage
make enums                                 # Generate enums from YAML (CI checks drift)
```

### Frontend
```bash
cd web && npm install                      # Install dependencies
npm start                                  # Dev server (Vite, port 3000)
npm run build                              # Production build
npm run lint                               # ESLint
```

### Docker
```bash
docker-compose up --build                  # Start all services
# TimescaleDB: localhost:5432
# Registry: localhost:8080
# DataHub: localhost:8081
```

### Provider Development
```bash
python -m quasar.lib.providers.devtools bars --config config.json --limit 100
python -m quasar.lib.providers.devtools symbols --config config.json
```

## Database Schema

Schemas in `db/schema/` numbered 00, 01, 02, ..., style:
- `02_registry.sql` - Code registry tables
- `03_assets.sql` - Asset catalog
- `04_asset_mapping.sql` - Cross-provider mappings
- `06_historical_data.sql` - Historical OHLC (hypertable)
- `07_live_data.sql` - Live tick data (hypertable)

Migrations in `db/migrations` (none presently). Migrations are performed only when the user requests them to be created to add functionality to an existing deployment. Otherwise, the main DB changes will be made to schemas.

## Testing

- When running any python for this project (including tests), use the "quasar" anaconda environment, which is setup with all required dependencies.
- Fixtures in `tests/conftest.py` provide mocked database pools, secret stores, and service instances
- Use `datahub_client` / `registry_client` fixtures for API testing with FastAPI TestClient
- Coverage excludes example providers (`quasar/lib/providers/examples/*`)

## Commits, Comments, and PRs

Never add attribution text like "Co-authored by Claude". Never include anything about claude or anthropic unless explicitly asked.

## CI/CD

GitHub Actions (`.github/workflows/test.yml`):
1. Python 3.12 setup
2. Install with dev extras
3. Generate enums and check for drift (`git diff --exit-code`)
4. Run pytest with coverage
5. Upload to Codecov

## Additional Documentation

Check these files when working on specific areas:

| Topic | File |
|-------|------|
| Architectural patterns | `.claude/docs/architectural_patterns.md` |
| Architecture diagram | `docs/architecture.md` |
| Provider development | `docs/howto/providers.md` |
| Registry service API | `docs/reference/registry.md` |
| DataHub service API | `docs/reference/datahub.md` |
| Lib common utilities | `docs/reference/lib-common.md` |
| Docstring conventions | `docs/contributing/docstrings.md` |
| Git workflow | `docs/git-workflow.md` |
