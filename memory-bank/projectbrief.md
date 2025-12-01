# σ₁: Project Brief
*v1.0 | Created: 2025-01-27 | Updated: 2025-01-27*
*Π: DEVELOPMENT | Ω: INITIALIZING*

## 🏆 Overview

Quasar is an automated trading platform designed for traders who can code. The platform enables users to build and execute automated trading strategies across multiple brokers through a flexible adaptor system. Users create custom "adaptors" for data providers, trading desks, and strategies, while Quasar handles data collection, strategy execution, and portfolio aggregation.

## 📋 Requirements

### Core Requirements
- [R₁] Support multiple data providers (historical and live data sources)
- [R₂] Enable custom data provider adaptors via code upload
- [R₃] Aggregate data collection needs across all strategies
- [R₄] Execute data collection at correct intervals (scheduled jobs)
- [R₅] Store collected data (OHLC and live tick data) in time-series database
- [R₆] Support multiple trading strategies running simultaneously
- [R₇] Portfolio management with strategy fund allocation weights
- [R₈] Aggregated performance tracking across multiple brokers and currencies
- [R₉] Registry service for managing uploaded code, securities, strategies, and subscriptions
- [R₁₀] Web frontend for user interaction and monitoring

### Technical Requirements
- [R₁₁] Python microservice backend (FastAPI)
- [R₁₂] TimescaleDB for time-series data storage
- [R₁₃] JavaScript frontend (React + Vite)
- [R₁₄] Docker containerization for services
- [R₁₅] RESTful API communication between services
- [R₁₆] Support for both REST and WebSocket data providers

## 🎯 Success Criteria

- Users can upload custom data provider code
- Data collection runs automatically based on subscriptions
- Multiple strategies can run concurrently
- Portfolio performance is accurately aggregated
- System handles multiple brokers and currencies
- Frontend provides clear visibility into system state

## 🔄 Project Status

**Current Phase**: DEVELOPMENT (Π₃)
- Backend services (Registry, DataHub) are implemented
- Provider system with example implementations (EODHD, Kraken)
- Database schema defined and implemented
- Frontend exists but is significantly behind backend
- Test coverage in place for backend services

## 📝 Notes

- Target users are developers who can write code
- Frontend development is a lower priority currently
- System is not production-ready yet
- Proprietary project - all rights reserved

