#!/usr/bin/env python3
"""
Generate backend/frontend enum artifacts from YAML sources.
- Input: enums/asset-classes.yml, enums/intervals.yml
- Output: quasar/lib/enums.py, web/src/enums.ts (TS), db/schema/00_enums_generated.sql

Deterministic: sorted canonical/alias keys; stable formatting.
Validation: non-empty canonicals, no duplicates, aliases target canonicals.
"""
from __future__ import annotations
import json
import sys
from pathlib import Path
from typing import Any
import yaml

ROOT = Path(__file__).resolve().parent.parent
ENUMS_DIR = ROOT / "enums"
BACKEND_OUT = ROOT / "quasar" / "lib" / "enums.py"
FRONTEND_OUT = ROOT / "web" / "src" / "enums.ts"
SQL_OUT = ROOT / "db" / "schema" / "01_enums_generated.sql"

ASSET_CLASSES_YAML = ENUMS_DIR / "asset-classes.yml"
INTERVALS_YAML = ENUMS_DIR / "intervals.yml"


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def validate_enum(name: str, data: dict[str, Any]) -> tuple[list[str], dict[str, str], dict[str, str]]:
    canonical = data.get("canonical") or []
    aliases = data.get("aliases") or {}
    cron = data.get("cron") or {}
    if not isinstance(canonical, list) or not all(isinstance(x, str) for x in canonical):
        raise ValueError(f"{name}: canonical must be a list of strings")
    if not canonical:
        raise ValueError(f"{name}: canonical cannot be empty")
    canon_set = set()
    for c in canonical:
        if c in canon_set:
            raise ValueError(f"{name}: duplicate canonical value: {c}")
        canon_set.add(c)
    if not isinstance(aliases, dict):
        raise ValueError(f"{name}: aliases must be a mapping")
    norm_aliases: dict[str, str] = {}
    for k, v in aliases.items():
        if not isinstance(k, str) or not isinstance(v, str):
            raise ValueError(f"{name}: aliases keys/values must be strings")
        if v not in canon_set:
            raise ValueError(f"{name}: alias target not in canonical: {k}->{v}")
        norm_aliases[k] = v
    norm_cron: dict[str, str] = {}
    if cron:
        if not isinstance(cron, dict):
            raise ValueError(f"{name}: cron must be a mapping")
        for k, v in cron.items():
            if not isinstance(k, str) or not isinstance(v, str):
                raise ValueError(f"{name}: cron keys/values must be strings")
            if k not in canon_set:
                raise ValueError(f"{name}: cron key not in canonical: {k}")
            norm_cron[k] = v
        missing = [c for c in canonical if c not in norm_cron]
        if missing:
            raise ValueError(f"{name}: cron missing for intervals: {missing}")
    # Sort canonicals and aliases deterministically
    canonical_sorted = list(canonical)
    aliases_sorted = dict(sorted(norm_aliases.items(), key=lambda kv: kv[0]))
    cron_sorted = dict(sorted(norm_cron.items(), key=lambda kv: kv[0])) if norm_cron else {}
    return canonical_sorted, aliases_sorted, cron_sorted


def render_backend(asset_classes: list[str], asset_aliases: dict[str, str], intervals: list[str], interval_aliases: dict[str, str]) -> str:
    # Produce Python enums and helpers
    def fmt_list(seq: list[str]) -> str:
        return "[" + ", ".join(repr(x) for x in seq) + "]"

    return f'''"""Generated enum definitions. Do not edit by hand."""
from __future__ import annotations
from enum import Enum
from typing import Iterable

class AssetClass(str, Enum):
    {"\n    ".join(f"{c.upper()} = '{c}'" for c in asset_classes)}

class Interval(str, Enum):
    {"\n    ".join(f"I_{c.upper().replace('M','M')} = '{c}'" for c in intervals)}

ASSET_CLASSES = tuple(e.value for e in AssetClass)
INTERVALS = tuple(e.value for e in Interval)

ASSET_CLASS_ALIAS_MAP = {json.dumps(asset_aliases, indent=4)}
INTERVAL_ALIAS_MAP = {json.dumps(interval_aliases, indent=4)}
ASSET_CLASS_CANONICAL_MAP = {{k.lower(): k for k in ASSET_CLASSES}}
INTERVAL_CANONICAL_MAP = {{k.lower(): k for k in INTERVALS}}

def _normalize(value: str | None, aliases: dict[str, str], canonical_map: dict[str, str]) -> str | None:
    if value is None:
        return None
    v = value.strip()
    if not v:
        return None
    v_lower = v.lower()
    if v_lower in aliases:
        return aliases[v_lower]
    if v_lower in canonical_map:
        return canonical_map[v_lower]
    return v_lower  # leave as-is; caller may decide to reject

def normalize_asset_class(value: str | None) -> str | None:
    return _normalize(value, ASSET_CLASS_ALIAS_MAP, ASSET_CLASS_CANONICAL_MAP)

def normalize_interval(value: str | None) -> str | None:
    return _normalize(value, INTERVAL_ALIAS_MAP, INTERVAL_CANONICAL_MAP)
'''  # noqa: E501


def render_frontend(asset_classes: list[str], asset_aliases: dict[str, str], intervals: list[str], interval_aliases: dict[str, str]) -> str:
    def fmt_array(seq: list[str]) -> str:
        return "[" + ", ".join(f"'{x}'" for x in seq) + "]"
    def fmt_obj(mapping: dict[str, str]) -> str:
        items = ",\n  ".join(f"'{k}': '{v}'" for k, v in mapping.items())
        return "{\n  " + items + "\n}"

    return f'''// Generated enum definitions. Do not edit by hand.
export const ASSET_CLASSES = {fmt_array(asset_classes)};
export const INTERVALS = {fmt_array(intervals)};

export const ASSET_CLASS_ALIASES = {fmt_obj(asset_aliases)};
export const INTERVAL_ALIASES = {fmt_obj(interval_aliases)};
export const ASSET_CLASS_CANONICAL_MAP = Object.fromEntries(ASSET_CLASSES.map((c) => [c.toLowerCase(), c]));
export const INTERVAL_CANONICAL_MAP = Object.fromEntries(INTERVALS.map((c) => [c.toLowerCase(), c]));

export function normalizeAssetClass(value) {{
  if (value == null) return null;
  const v = value.trim();
  if (!v) return null;
  const lower = v.toLowerCase();
  if (ASSET_CLASS_ALIASES[lower]) return ASSET_CLASS_ALIASES[lower];
  if (ASSET_CLASS_CANONICAL_MAP[lower]) return ASSET_CLASS_CANONICAL_MAP[lower];
  return lower; // leave as-is; caller may decide to reject
}}

export function normalizeInterval(value) {{
  if (value == null) return null;
  const v = value.trim();
  if (!v) return null;
  const lower = v.toLowerCase();
  if (INTERVAL_ALIASES[lower]) return INTERVAL_ALIASES[lower];
  if (INTERVAL_CANONICAL_MAP[lower]) return INTERVAL_CANONICAL_MAP[lower];
  return lower; // leave as-is; caller may decide to reject
}}
'''


def render_sql(asset_classes: list[str], intervals: list[str], interval_cron: dict[str, str]) -> str:
    asset_values = ",\n    ".join(f"('{ac}')" for ac in asset_classes)
    interval_values = ",\n    ".join(f"('{i}', '{interval_cron[i]}')" for i in intervals)
    return f"""-- Generated from enums YAML. Do not edit by hand.
-- Asset classes lookup
CREATE TABLE IF NOT EXISTS asset_class (
    code TEXT PRIMARY KEY
);
INSERT INTO asset_class (code) VALUES
    {asset_values}
ON CONFLICT (code) DO NOTHING;

-- Accepted intervals with cron
CREATE TABLE IF NOT EXISTS accepted_intervals (
    interval TEXT PRIMARY KEY,
    cron TEXT NOT NULL
);
INSERT INTO accepted_intervals (interval, cron) VALUES
    {interval_values}
ON CONFLICT (interval) DO NOTHING;
"""


def main() -> int:
    asset_data = load_yaml(ASSET_CLASSES_YAML)
    interval_data = load_yaml(INTERVALS_YAML)

    asset_classes, asset_aliases, _ = validate_enum("asset-classes", asset_data)
    intervals, interval_aliases, interval_cron = validate_enum("intervals", interval_data)

    backend = render_backend(asset_classes, asset_aliases, intervals, interval_aliases)
    frontend = render_frontend(asset_classes, asset_aliases, intervals, interval_aliases)
    sql = render_sql(asset_classes, intervals, interval_cron)

    BACKEND_OUT.parent.mkdir(parents=True, exist_ok=True)
    FRONTEND_OUT.parent.mkdir(parents=True, exist_ok=True)
    SQL_OUT.parent.mkdir(parents=True, exist_ok=True)

    BACKEND_OUT.write_text(backend, encoding="utf-8")
    FRONTEND_OUT.write_text(frontend, encoding="utf-8")
    SQL_OUT.write_text(sql, encoding="utf-8")

    print("Generated:")
    print(f"- {BACKEND_OUT}")
    print(f"- {FRONTEND_OUT}")
    print(f"- {SQL_OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
