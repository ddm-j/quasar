# Enums source of truth

This directory holds per-enum YAML definitions that act as the single source for generated artifacts (Python/TS/SQL) for backend/frontend/db.

## Files
- `asset-classes.yml`: canonical asset classes plus aliases.
- `intervals.yml`: canonical intervals plus cron map (drives DB seeds) and minimal aliases.

## YAML shape
- `canonical`: ordered list of canonical identifiers (lowercase_with_underscores).
- `aliases`: map of alternate labels -> canonical identifiers.
- `cron` (intervals only): map interval -> cron string.
- `labels`/`notes`: optional, can be added later if needed for UI or docs.

## Extension guidelines
- Add new canonical entries only when required by providers/brokers; keep names stable and lowercase_with_underscores.
- When adding aliases, prefer common industry terms; avoid overlapping or ambiguous aliases.
- For intervals, do not change canonical values without updating the DB seed, backend literals, and cron map in YAML (regenerate artifacts).

## Current decisions (Phase 1/4 source)
- Asset classes (canonical): equity, fund, etf, bond, crypto, currency, future, option, index, commodity, derivative, cfd, warrant, adr, preferred, mutual_fund, money_market, rates, mbs, muni, structured_product.
- Asset class aliases: stock→equity, fx→currency, futures→future, perp→future, perps→future, index_option→option, bond_etf→etf, mmf→money_market, adr_pref→preferred.
- Intervals (canonical): 1min, 5min, 15min, 30min, 1h, 4h, 1d, 1w, 1M. Aliases: one_minute→1min, daily→1d. Cron per interval defined in `intervals.yml`.

## Generated artifacts
- Backend: `quasar/lib/enums.py`
- Frontend: `web/src/enums.ts`
- DB seed/lookup: `db/schema/01_enums_generated.sql` emitted from YAML by `scripts/gen_enums.py`.

## Regeneration and guardrails
- Command: `make enums` regenerates all artifacts above.
- CI drift check: `.github/workflows/test.yml` runs `make enums` then `git diff --exit-code`; build fails if artifacts drift from YAML.
- Optional runtime check: set `ENUM_GUARD_MODE=warn|strict` to compare generated enums against DB lookup tables (`asset_class`, `accepted_intervals`) at service startup (default `off`). Use `warn` in clean deploys; `strict` only when you want startup failure on mismatch.
