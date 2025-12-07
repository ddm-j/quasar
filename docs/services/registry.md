# Registry Service

Purpose: Manage provider code, assets, mappings, and data subscriptions. Coordinates with DataHub for validation and symbol discovery.

## Run locally

- Docker Compose: `docker compose up registry`
- Direct: set env vars (`DATABASE_URL`, `QUASAR_SECRET_KEY`, provider keys) then `uvicorn quasar.services.registry.app:app --reload --port 8080`

## Configuration

- `DATABASE_URL`: PostgreSQL/TimescaleDB connection string
- `QUASAR_SECRET_KEY`: secret for signing or encryption helpers
- Provider credentials: passed through for validation/discovery

## Endpoints

- REST (FastAPI) served from `quasar.services.registry.app:app`
- API schema exposed via FastAPI `/docs` and `/openapi.json`

## Tests

- Run service tests: `pytest tests/services/registry`
- Keep API contract changes reflected in tests and docstrings

## Notes

- Registry talks to DataHub over HTTP (port 8080 inside Docker).
- Keep schemas in sync with DB migrations under `db/schema/`.

