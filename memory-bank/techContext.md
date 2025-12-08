# σ₃: Technical Context
*v1.0 | Created: 2025-01-27 | Updated: 2025-01-27*
*Π: DEVELOPMENT | Ω: INITIALIZING*

## 🛠️ Technology Stack

### Backend
- **Language**: Python 3.12+
- **Framework**: FastAPI 0.115.0+
- **ASGI Server**: Uvicorn (standard)
- **Database Driver**: asyncpg 0.30.0
- **HTTP Client**: aiohttp 3.11.18
- **WebSocket**: websockets 15.0.1
- **Scheduling**: APScheduler 3.11.0
- **Rate Limiting**: aiolimiter 1.2.1
- **AWS Integration**: boto3 1.38.8
- **Cryptography**: cryptography 44.0.3
- **HTTP Requests**: Requests 2.32.3

### Database
- **Database**: TimescaleDB (PostgreSQL 17)
- **Extension**: TimescaleDB for time-series optimization
- **Connection**: asyncpg for async PostgreSQL access

### Frontend
- **Framework**: React 18.2.0
- **Build Tool**: Vite 5.2.6
- **UI Library**: CoreUI Pro React 5.0.0
- **State Management**: Redux 5.0.1
- **Routing**: React Router DOM 6.22.3
- **Charts**: Chart.js 4.4.2, Lightweight Charts 4.1.3
- **Styling**: Sass 1.72.0

### Infrastructure
- **Containerization**: Docker & Docker Compose
- **Reverse Proxy**: Nginx (production)
- **Package Management**: setuptools, pip

### Testing
- **Test Framework**: pytest 8.0.0+
- **Async Testing**: pytest-asyncio 0.23.0+
- **Mocking**: pytest-mock 3.12.0+
- **HTTP Testing**: httpx 0.27.0+
- **Coverage**: pytest-cov 4.1.0+

## 🔧 Development Environment

### Python Environment
- **Python Version**: >=3.12
- **Package Manager**: pip (via pyproject.toml)
- **Project Structure**: Package-based (`quasar/`)

### Node.js Environment
- **Package Manager**: npm
- **Build Tool**: Vite
- **Location**: `web/` directory

### Database Setup
- **Container**: `timescale/timescaledb:latest-pg17`
- **Initialization**: SQL scripts in `db/schema/`
- **Port**: 5432

## 📁 Project Structure

```
quasar/
├── quasar/
│   ├── lib/
│   │   ├── common/          # Shared utilities
│   │   └── providers/       # Provider base classes & examples
│   └── services/
│       ├── registry/        # Registry microservice
│       └── datahub/         # DataHub microservice
├── web/                     # Frontend React app
├── db/
│   └── schema/              # Database schema files
├── tests/                   # Test suite
├── scripts/                 # Utility scripts
└── docker-compose.yml       # Service orchestration
```

## 🔌 External Dependencies

### Data Providers
- **EODHD**: Historical OHLCV data (HTTPS REST API)
- **Kraken**: Real-time cryptocurrency data (WSS + HTTPS REST API)

### Infrastructure Services
- **AWS**: For secret storage (boto3)

## ⚙️ Configuration

### Environment Variables
- `DSN`: Database connection string
- `LOGLEVEL`: Logging level (DEBUG/INFO/etc.)
- `CORS_ORIGINS`: Allowed frontend origins
- `QUASAR_SECRET_FILE`: Path to secrets file
- `QUASAR_SYSTEM_CONTEXT`: Path to system context

### Docker Secrets
- `system_context`: System encryption context
- `quasar`: Quasar-specific secrets

## 🚀 Build & Deployment

### Backend
- Build via setuptools (`pyproject.toml`)
- Docker images built from service Dockerfiles
- Services run as Python modules

### Frontend
- Build via Vite (`npm run build`)
- Production: Static files served by Nginx
- Development: Vite dev server (port 3000)

## 📊 Testing Configuration

- **Test Path**: `tests/`
- **Coverage**: HTML reports in `htmlcov/`
- **Async Mode**: Auto (pytest-asyncio)
- **Coverage Exclusions**: Template providers (examples/eodhd.py, examples/kraken.py)

## 🔐 Security Considerations

- Secrets managed via Docker secrets
- Cryptography library for encryption
- CORS configured per environment
- API keys stored securely

