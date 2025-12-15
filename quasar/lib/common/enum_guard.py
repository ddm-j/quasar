"""Optional runtime guard to ensure enums match database lookup tables."""

from __future__ import annotations

import logging
from asyncpg import Pool, UndefinedTableError

from quasar.lib.enums import ASSET_CLASSES, INTERVALS

logger = logging.getLogger(__name__)


async def _fetch_codes(pool: Pool, table: str, column: str) -> set[str] | None:
    """Fetch a set of codes from a table; return None if table missing."""
    query = f"SELECT {column} FROM {table}"
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query)
            return {str(row[column]) for row in rows}
    except UndefinedTableError:
        logger.warning("Enum guard: table %s missing; skipping check", table)
        return None
    except Exception as exc:
        logger.warning("Enum guard: failed reading %s.%s: %s", table, column, exc)
        return None


async def validate_enums(pool: Pool, strict: bool = False) -> None:
    """Compare generated enums with DB lookup tables."""
    db_assets = await _fetch_codes(pool, "asset_class", "code")
    db_intervals = await _fetch_codes(pool, "accepted_intervals", "interval")

    issues: list[str] = []

    if db_assets is not None:
        missing = set(ASSET_CLASSES) - db_assets
        extra = db_assets - set(ASSET_CLASSES)
        if missing:
            issues.append(f"asset_class missing {sorted(missing)}")
        if extra:
            issues.append(f"asset_class has extras {sorted(extra)}")

    if db_intervals is not None:
        missing = set(INTERVALS) - db_intervals
        extra = db_intervals - set(INTERVALS)
        if missing:
            issues.append(f"accepted_intervals missing {sorted(missing)}")
        if extra:
            issues.append(f"accepted_intervals has extras {sorted(extra)}")

    if not issues:
        logger.info("Enum guard: DB lookup tables match generated enums")
        return

    msg = "; ".join(issues)
    if strict:
        raise RuntimeError(f"Enum guard failed: {msg}")
    logger.warning("Enum guard warning: %s", msg)
