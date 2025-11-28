# Quasar

[![Tests](https://github.com/ddm-j/quasar/actions/workflows/test.yml/badge.svg)](https://github.com/ddm-j/quasar/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/ddm-j/quasar/graph/badge.svg)](https://codecov.io/gh/ddm-j/quasar)

An automated trading platform for API-based strategy execution and portfolio management.

## Overview

Quasar enables traders who can code to build and run automated trading strategies across multiple brokers. The platform handles data collection, strategy execution, and portfolio aggregation.

## Architecture

- **Registry** — Manages strategies, data subscriptions, and provider configurations
- **Datahub** — Aggregates data needs and fetches OHLC/tick data from providers
- **Providers** — Adaptors for data sources (EODHD, Kraken, etc.)
- **Web** — Frontend dashboard (Vite + React)
- **TimescaleDB** — Time-series database for market data

## Development

In active development. Not production quality yet.

Proprietary — All rights reserved.

