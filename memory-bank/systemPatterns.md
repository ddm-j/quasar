# σ₂: System Patterns
*v1.0 | Created: 2025-01-27 | Updated: 2025-01-27*
*Π: DEVELOPMENT | Ω: INITIALIZING*

## 🏛️ Architecture Overview

Quasar follows a microservices architecture with clear separation of concerns:

```
Frontend (React/Vite) 
    ↓ HTTP
Backend Services (FastAPI)
    ├── Registry Service (Port 8080)
    ├── DataHub Service (Port 8081)
    └── StrategyEngine Service (Port 8082)
    ↓ PostgreSQL
TimescaleDB (Port 5432)
```

## 🧩 Core Components

### Registry Service
**Purpose**: Central management service for code, assets, and configurations

**Responsibilities**:
- Manage uploaded provider/broker code
- Track available securities from each provider
- Create and manage asset mappings (provider ↔ broker)
- Manage data subscriptions
- Coordinate with DataHub for provider validation

**Key Patterns**:
- Code registry stored in database
- Dynamic code loading for providers
- Asset mapping system for symbol translation

### DataHub Service
**Purpose**: Data collection and aggregation engine

**Responsibilities**:
- Load and execute registered data provider code
- Schedule data collection jobs (APScheduler)
- Fetch historical and live market data
- Store data in TimescaleDB
- Validate provider code on behalf of Registry

**Key Patterns**:
- Provider abstraction via base classes
- Scheduled job execution for data collection
- Support for both REST and WebSocket providers
- Rate limiting for external API calls

### StrategyEngine Service
**Purpose**: (WIP) strategy validation/execution surface

**Current Responsibilities**:
- Provide an internal `/strategy/validate` endpoint so Registry can vet uploaded strategies.
- Expose health/readiness endpoints for Compose/Nginx.

**Planned Responsibilities**:
- Manage strategy instances, scheduling, broker routing, and policy enforcement.
- Emit per-run telemetry consumed by the Portfolio Manager.

**Key Patterns (established now)**:
- Mirrors Registry/DataHub lifecycle via shared `DatabaseHandler`/`APIHandler`.
- Uses dedicated `/app/dynamic_strategies` volume + `system_context` secret just like providers.

### Provider System
**Purpose**: Pluggable data source adaptors

**Structure**:
- `core.py`: Base classes for providers
- `eodhd.py`: Historical data provider example
- `kraken.py`: Live data provider example

**Key Patterns**:
- Abstract base classes for provider interface
- Users subclass to create custom providers
- Code uploaded and executed dynamically
- Support for both historical (OHLC) and live (tick) data

### Database Layer
**Purpose**: Time-series data storage and metadata

**Key Tables**:
- `historical_data`: OHLCV bars for historical providers
- `live_data`: OHLCV bars for live providers
- `code_registry`: Registered provider/broker code
- `assets`: Available securities per provider
- `asset_mapping`: Symbol mappings between providers/brokers
- `provider_subscription`: Active data collection subscriptions

**Patterns**:
- TimescaleDB for time-series optimization
- Hypertables for partitioned time-series data
- Foreign key relationships for data integrity

## 🔄 Communication Patterns

### Service-to-Service
- **Registry ↔ DataHub**: Bidirectional HTTP (port 8080)
- **Services → Database**: PostgreSQL async connections (asyncpg)
- **DataHub → External APIs**: HTTPS/WSS with rate limiting

### Frontend-to-Backend
- **Development**: Direct HTTP to services (CORS enabled)
- **Production**: Nginx reverse proxy routing

## 🛡️ Security Patterns

- Secret management via Docker secrets
- System context stored securely
- CORS configuration for frontend access
- API key encryption for external providers

## 📦 Deployment Patterns

- Docker Compose for local development
- Containerized services with health checks
- Volume mounts for dynamic provider code
- Environment-based configuration

## 🔮 Design Decisions

1. **Microservices**: Separation allows independent scaling and development
2. **Dynamic Code Loading**: Enables user-uploaded providers without service restarts
3. **TimescaleDB**: Optimized for time-series market data queries
4. **Async/Await**: Python async for concurrent data collection
5. **Provider Abstraction**: Base classes ensure consistent provider interface

