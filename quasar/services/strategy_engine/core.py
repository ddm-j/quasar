from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import asyncpg
from fastapi import HTTPException

from quasar.lib.common.api_handler import APIHandler
from quasar.lib.common.database_handler import DatabaseHandler
from quasar.services.strategy_engine.schemas import (
    StrategyValidateRequest,
    StrategyValidateResponse,
)

logger = logging.getLogger(__name__)

ALLOWED_DYNAMIC_PATH = Path("/app/dynamic_strategies")


class StrategyEngine(DatabaseHandler, APIHandler):
    """Thin scaffold for the future strategy engine microservice."""

    name = "StrategyEngine"

    def __init__(
        self,
        dsn: str | None = None,
        pool: Optional[asyncpg.Pool] = None,
        api_host: str = "0.0.0.0",
        api_port: int = 8082,
    ) -> None:
        DatabaseHandler.__init__(self, dsn=dsn, pool=pool)
        APIHandler.__init__(self, api_host=api_host, api_port=api_port)

    def _setup_routes(self) -> None:
        logger.info("StrategyEngine: registering API routes")
        self._api_app.router.add_api_route(
            "/api/strategy-engine/health",
            self.handle_health,
            methods=["GET"],
        )
        self._api_app.router.add_api_route(
            "/internal/strategy/validate",
            self.handle_validate_strategy,
            methods=["POST"],
            response_model=StrategyValidateResponse,
        )

    async def start(self) -> None:
        """Bootstraps DB connections and API server."""
        await self.init_pool()
        await self.start_api_server()
        logger.info("%s started", self.name)

    async def stop(self) -> None:
        """Shuts down background resources."""
        await self.close_pool()
        await self.stop_api_server()
        logger.info("%s stopped", self.name)

    async def handle_health(self) -> dict[str, str]:
        """Simple readiness endpoint."""
        return {"service": self.name, "status": "ok"}

    async def handle_validate_strategy(
        self, request: StrategyValidateRequest
    ) -> StrategyValidateResponse:
        """
        Placeholder validation hook called by the Registry.

        Future versions will perform dynamic imports and enforce stricter rules.
        For now we simply ensure the referenced file exists inside the allowed
        strategies directory.
        """

        path = Path(request.file_path).resolve()
        if not str(path).startswith(str(ALLOWED_DYNAMIC_PATH)):
            logger.warning("Rejected strategy outside of sandbox: %s", path)
            raise HTTPException(
                status_code=403,
                detail=f"Strategy file must live under {ALLOWED_DYNAMIC_PATH}",
            )
        if not path.is_file():
            logger.warning("Strategy file not found during validation: %s", path)
            raise HTTPException(status_code=404, detail="Strategy file not found")

        logger.info("Strategy validation placeholder invoked for %s", path.name)
        return StrategyValidateResponse(
            status="pending",
            class_name=path.stem,
            subclass_type="unknown",
            details="Validation logic not implemented yet.",
        )

