# Useful Development Commands

This document lists out common commands used during development.

## Docker Compose
### Up/Down
- `docker compose up -d` (detached mode, no logs)
- `docker compose down`
### Logs
- `docker compose logs -f`
- `docker compose logs -f container_name`
### Restarts
- `docker compose restart container_name`
### From Scratch Rebuild
- `docker compose down -v`
- `docker compose build --no-cache`
- `docker compose up -d`

## Testing
### Backend
- `pytest`

## Development / Testing of Provider Classes
Can run in bars or symbols mode, make sure to make the distinction.
- `python -m quasar.lib.providers.devtools [bars/symbols] --config path/to/hist.json`

## Database
### PSQL Terminal
- `docker exec -it quasardb psql -h localhost -U postgres -d postgres`
### Migration
Migrating DB can be done via `bash` shell:
- `cat db/migrations/migration_file_name.sql | docker exec -i quasardb psql -v ON_ERROR_STOP=1 -h localhost -U postgres -d postgres`