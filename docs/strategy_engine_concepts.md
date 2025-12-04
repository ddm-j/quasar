# Strategy Engine Concept Prototypes

This document captures the working assumptions for the first two strategy archetypes we will support in Quasar.  
It focuses on the inputs each strategy receives, the outputs they must emit, how those outputs become broker orders,  
and how every stage is logged so the Portfolio Manager can surface accurate performance views.

---

## 1. Shared Strategy Runtime Contract

### 1.1 Execution timeline (per interval)
1. **Scheduler** – APScheduler triggers every accepted interval (e.g., `1d`, `4h`) per `strategy_instance.interval`.
2. **Data hydration**
   - Fetch historical bars from `historical_data` up to the last completed bar.
   - Fetch in-progress bar from `live_data`, merge with the historical series on `common_symbol`.
3. **Context assembly**
   - `StrategyContext` is passed to the strategy instance and includes:
     - `clock`: interval metadata, timestamps of the current/previous bars, next trigger time.
     - `data`: helper with APIs `get_series(symbol, lookback)` and `get_latest_bar(symbol)`.
     - `positions`: current positions per broker silo (quantity, cost basis, base currency).
     - `cash`: available buying power per broker silo after reserving risk buffers.
     - `policies`: read-only access to policy settings (max risk, pause rules).
     - `broker_capabilities`: allowed order types, lot sizes, trading calendar.
4. **Strategy execution**
   - Strategies expose `on_bar(context)` and return outputs (see 1.2).
5. **Policy wrap**
   - Policy engine validates signals vs. constraints (exposure limits, kill switches).
6. **Order generation & routing**
   - Convert strategy output into broker-specific order intents.
   - Send through BrokerRouter (HTTP call to adaptor) within seconds SLA.
7. **Persistence & telemetry**
   - Store run record, signals, generated orders, fills, and derived metrics.

### 1.2 Strategy outputs
Strategies can choose either or both of the following forms:

| Output type         | Description                                                                 | Consumption pipeline                                                  |
|---------------------|-----------------------------------------------------------------------------|------------------------------------------------------------------------|
| `TargetPosition`    | Desired net position per (`common_symbol`, `broker_account`). Example: 0 or +100 shares. | Differ vs. current holdings, create `OrderIntent` list automatically. |
| `OrderList`         | Explicit orders with side, quantity, limit type, time-in-force, optional tags. | Forwarded as-is; policies still validate before routing.              |

Both payloads include optional annotations (`signal_strength`, `indicator_values`, `notes`) that are persisted for later visualization.

### 1.3 Logging & persistence

| Table / log stream         | Purpose                                                                               |
|----------------------------|---------------------------------------------------------------------------------------|
| `strategy_run`             | One row per scheduler invocation (status, timings, bar interval, context hash).      |
| `strategy_signal`          | Flattened view of signals/indicators produced by the strategy output.                |
| `strategy_order`           | Orders requested by the strategy pre-policy.                                         |
| `strategy_order_event`     | Order lifecycle events (accepted, partially filled, filled, rejected).               |
| `strategy_position_snapshot` | Positions per broker silo after the run, in native units and USD.                   |
| `strategy_metric`          | Derived KPIs (PnL, drawdown, realized vol) per run, consumed by PortfolioManager.     |

All logs reference `strategy_instance_id` and `strategy_run_id` so the Portfolio Manager can reconstruct histories or replay a run in the UI.

---

## 2. Prototype A – Long-Only Moving Average Crossover

### 2.1 Objective
Trade a single asset long-only based on two moving averages (fast vs. slow). Enter long when `fast_ma > slow_ma`, exit (move to cash) otherwise.

### 2.2 Configuration schema
- `common_symbol`: canonical asset identifier.
- `interval`: bar size (e.g., `1d`, `4h`).
- `fast_window`: integer (e.g., 20 bars).
- `slow_window`: integer (e.g., 50 bars).
- `max_position`: absolute units allowed (ties to broker lot sizes).
- `broker_allocations`: percentage split per broker silo.
- `policy_set_id`: references reusable risk configuration (daily loss, stop behavior).

### 2.3 Inputs provided to the strategy
- **Price series**: `context.data.get_series(common_symbol, max(fast, slow) + buffer)` returning merged historical + latest live bar series.
- **Current position & cash**: `context.positions[broker]`, `context.cash[broker]`.
- **Execution metadata**: trading calendar, next open/close times (used to avoid overnight submissions if disallowed).
- **Policy flags**: e.g., `context.policies.can_enter_new_positions`.

### 2.4 Strategy logic
1. Ensure enough data points to compute both MAs.
2. Compute fast/slow simple moving averages on the merged time series.
3. Determine target state:
   - If `fast_ma > slow_ma` and within risk budgets → target position = `max_position`.
   - Else → target position = `0`.
4. Emit `TargetPosition` per broker silo (scaled by allocation percentages).
5. Include annotations: MA values, crossover delta, boolean `is_signal`.

### 2.5 Order generation
- StrategyEngine compares target vs. current holdings per broker.
- Generates `OrderIntent` (market-on-close/next open, or configurable order type).
- Applies rounding/lot rules based on `broker_capabilities`.
- Sends through BrokerRouter; order IDs are linked back to `strategy_run_id`.

### 2.6 Logging & persistence
- `strategy_signal`: store fast/slow MA, crossover boolean, target position.
- `strategy_order`: record each generated order with rationale tag `ma_crossover`.
- `strategy_position_snapshot`: after fills, capture new holdings.
- `strategy_metric`: daily PnL, hit rate, time since last crossover.

These logs enable dashboards to show when crossovers occurred, how many bars the strategy stayed invested, and the PnL curve.

---

## 3. Prototype B – Multi-Asset Efficient Frontier Weighting

### 3.1 Objective
Allocate capital across a basket of assets (e.g., equities, bonds, crypto) based on mean-variance optimization. Rebalance on a scheduled interval (e.g., weekly).

### 3.2 Configuration schema
- `asset_universe`: list of `common_symbol` entries with optional per-broker overrides.
- `interval`: typically `1d` or `1w`.
- `lookback_window`: bars used to compute returns/covariance (e.g., 252 daily bars).
- `risk_aversion`: lambda in the Markowitz optimizer.
- `rebalance_threshold`: minimum drift before placing trades (bps).
- `broker_allocations`: each asset may route to a different broker; include per-silo weight caps.
- `policy_set_id`: references limits (max leverage, exposure per asset class).

### 3.3 Inputs provided to the strategy
- **Price matrix**: `context.data.get_panel(asset_universe, lookback_window)` returning aligned return series for each asset (merged historical + live close).
- **Latest valuations**: `context.data.get_latest_bar(symbol)` for current prices used to translate weights into units.
- **Current portfolio**: aggregated positions and cost basis per asset & broker.
- **Capital context**: total USD capital per broker silo, plus global capital figure for reporting.
- **Risk constraints**: e.g., max weight per asset class, min cash buffer.

### 3.4 Strategy logic
1. Compute log returns and covariance matrix for the lookback window.
2. Run efficient frontier optimization (could use preset “max Sharpe” or target vol) respecting weight caps.
3. Compare optimized weights vs. current weights; ignore assets whose drift is below `rebalance_threshold`.
4. Translate new weights into target share counts per broker silo using latest prices and available capital.
5. Emit both `TargetPosition` map and metadata block:
   - Per-asset target weight and expected return.
   - Portfolio target volatility and Sharpe ratio.
   - Rebalance reason (scheduled vs. drift-triggered).

### 3.5 Order generation
- For each asset/broker pair, StrategyEngine calculates delta quantity.
- Policy module enforces aggregate constraints (e.g., net exposure <= 100% + leverage buffer).
- Orders can be batched (VWAP/iceberg) if broker supports advanced types; otherwise default to limit orders near last price.
- Because execution may span multiple assets, order submission is wrapped in a transaction-like batch so that failures can be rolled back or logged as partial rebalance.

### 3.6 Logging & persistence
- `strategy_signal`: store optimized weights, covariance stats, target portfolio metrics.
- `strategy_order`: capture rebalance orders with `batch_id` referencing the run.
- `strategy_metric`: record realized weights after fills, tracking error vs. target, realized vol.
- `strategy_position_snapshot`: multi-asset holdings in native units and USD.
- `strategy_metric` also stores optimization diagnostics (condition number, constraints hit) for debugging.

These artifacts allow the frontend to render weight timelines, show why rebalances occurred, and compare realized vs. target allocations.

---

## 4. Aggregation & Future Viewing

- **Per-strategy dashboards**
  - Use `strategy_signal` for indicator charts (MA lines, weight stacks).
  - Use `strategy_order_event` + broker fills for trade blotters.
  - Use `strategy_metric` for cumulative PnL, drawdown, exposure over time.

- **Portfolio-level aggregation**
  - PortfolioManager consumes snapshots and metrics, converts everything to USD using FX rates already captured by providers.
  - Broker silos remain separate for capital accounting, but aggregated metrics sum exposures, PnL, and risk contributions in USD.
  - Weight adjustments in the UI update `strategy_broker_allocation`; StrategyEngine reads the new targets on the next run.

- **Audit & replay**
  - Because every run has deterministic inputs (bar set hash, config hash, code hash), we can reproduce a signal by re-running the same data through the strategy class if needed.
  - Logs are structured so that compliance exports (orders, fills, signals) can be generated per strategy or for the whole portfolio.

This concept document will guide the first implementation of the StrategyEngine and ensure both simple (MA crossover) and complex (multi-asset optimization) strategies fit into the same runtime and persistence pipeline.


