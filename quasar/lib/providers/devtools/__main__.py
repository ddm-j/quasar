"""CLI entrypoint for provider dev harnesses."""

from __future__ import annotations

import argparse

from quasar.lib.providers import ProviderType

from .historical import run_historical
from .live import run_live
from .symbols import run_symbols
from .utils import configure_dev_logging, load_config, parse_provider_type


def _add_bars_flags(parser: argparse.ArgumentParser, default_limit: int | None) -> None:
    parser.add_argument("--config", required=True, help="Path to config JSON/YAML")
    parser.add_argument("--limit", type=int, default=default_limit, help="Max items to collect (defaults depend on provider_type)")
    strict_group = parser.add_mutually_exclusive_group()
    strict_group.add_argument("--strict", dest="strict", action="store_true", help="Enable strict validation checks")
    strict_group.add_argument("--no-strict", dest="strict", action="store_false", help="Disable strict validation checks")
    parser.set_defaults(strict=True)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Provider dev harness (always validates)")
    subparsers = parser.add_subparsers(dest="command", required=True)

    bars = subparsers.add_parser("bars", help="Run a historical or live provider (config selects type)")
    _add_bars_flags(bars, default_limit=None)

    symbols = subparsers.add_parser("symbols", help="Fetch available symbols from a provider")
    symbols.add_argument("--config", required=True, help="Path to config JSON/YAML")
    strict_group = symbols.add_mutually_exclusive_group()
    strict_group.add_argument("--strict", dest="strict", action="store_true", help="Enable strict validation checks")
    strict_group.add_argument("--no-strict", dest="strict", action="store_false", help="Disable strict validation checks")
    symbols.set_defaults(strict=True)

    return parser.parse_args(argv)


def _dispatch(command: str, args: argparse.Namespace) -> tuple[str, list]:
    cfg = load_config(args.config)
    if command == "bars":
        provider_type = parse_provider_type(cfg.get("provider_type"))
        limit = args.limit
        if limit is None:
            limit = 500 if provider_type == ProviderType.HISTORICAL else 50
        if provider_type == ProviderType.HISTORICAL:
            return "bars", run_historical(config=cfg, strict=args.strict, limit=limit)
        if provider_type == ProviderType.REALTIME:
            return "bars", run_live(config=cfg, strict=args.strict, limit=limit)
        raise ValueError("Unknown provider_type for bars; expected historical or live")
    if command == "symbols":
        return "symbols", run_symbols(config=cfg, strict=args.strict)
    raise ValueError(f"Unknown command {command}")


def main(argv: list[str] | None = None) -> None:
    configure_dev_logging()
    args = _parse_args(argv)
    kind, items = _dispatch(args.command, args)
    noun = "symbol(s)" if kind == "symbols" else "bar(s)"
    print(f"Collected {len(items)} {noun}")


if __name__ == "__main__":
    main()

