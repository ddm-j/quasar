# σ₄: Active Context
*v1.1 | Created: 2025-01-27 | Updated: 2025-12-04*  
*Π: DEVELOPMENT | Ω: EXECUTE*

## 🔮 Current Focus

**Phase**: DEVELOPMENT (Π₃)  
**Mode**: EXECUTE (Ω₄)

- Stand up the Strategy Engine surface area (contracts + service) so Registry uploads have somewhere to land.  
- Capture concrete requirements for representative strategies to guide runtime/storage decisions.  
- Wire the new microservice into Docker Compose with proper secrets/volumes.

## 🔄 Recent Changes

### Concepting: Strategy Engine Prototypes
- Added `docs/strategy_engine_concepts.md` describing the shared runtime contract plus two prototypes (MA crossover, multi-asset optimizer).  
- Document spells out required inputs, expected outputs, order conversion, and logging/persistence per prototype so backend + frontend share the same assumptions.

### Library: Strategy Scaffolding
- New `quasar/lib/strategies/` package:
  - `base.py`: defines `StrategyConfig`, `StrategyContext`, `TargetPosition`, `OrderIntent`, `StrategyResult`, and the `BaseStrategy` ABC.
  - `templates.py`: `MovingAverageCrossoverStrategy` scaffold + typed config.  
  - `__init__.py`: exports so user uploads can import from `quasar.lib.strategies`.
- Purpose: give users a minimal-but-real contract to subclass while we finish the runtime.

### Service: Strategy Engine Skeleton
- Created `quasar/services/strategy_engine/` (core, app, Dockerfile).  
- Features today: `/api/strategy-engine/health` and `/internal/strategy/validate` (verifies file exists under `/app/dynamic_strategies`).  
- Shares `DatabaseHandler`/`APIHandler` lifecycle so it matches Registry/DataHub patterns.

### Infrastructure: Compose & Secrets
- `docker-compose.yml` now includes the new service (port 8082) with:
  - Separate `dynamic_strategies` volume for uploaded strategy code.
  - Access to `system_context` secret + `QUASAR_SYSTEM_CONTEXT` env var.
- Enables `docker compose up -d strategy_engine` without touching existing containers.

### Registry alignment
- Registry upload handler now has a validation endpoint reserved for strategies, mirroring provider/broker behavior, so hooking it up later will be straightforward.

## 🏁 Next Steps

1. Extend Registry to accept `class_type="strategy"` uploads and forward to StrategyEngine validator.  
2. Define DB schema for `strategy_instance`, runs, orders, and policy sets.  
3. Add scheduler skeleton inside StrategyEngine (even if it just logs for now).  
4. Sketch broker adaptor contract + PortfolioManager touchpoints so strategy outputs have a destination.

## 📌 Active Areas

- **Backend services**: Registry, DataHub, and newly scaffolded StrategyEngine.  
- **Shared libs**: Strategy abstractions ready for uploaded code.  
- **Infrastructure**: Compose topology & secrets updated to support strategy development.

## 🔍 Areas Requiring Attention

1. Strategy runtime still a stub (no scheduling, no broker routing).  
2. Broker adaptor framework remains unimplemented.  
3. Portfolio aggregation has no data feed yet—depends on StrategyEngine metrics.  
4. Frontend lacks strategy management UI.  
5. User-facing docs still need to be updated to reflect the StrategyEngine work.

## 📋 Innovation Artifacts (recent)

- `docs/strategy_engine_concepts.md` – prototype behavior, data needs, output contract, and logging story.  
- Older chart/layout research docs are archived but not part of the present sprint.



