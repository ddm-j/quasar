"""Shared FastAPI handler base with lifecycle helpers."""

from typing import Optional
from abc import ABC, abstractmethod
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import asyncio
import os

import logging
logger = logging.getLogger(__name__)


class APIHandler(ABC):
    """Serve a FastAPI application and manage its lifecycle."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-friendly service name used for logging and titles."""
        ...

    def __init__(
            self,
            api_host: str = '0.0.0.0',
            api_port: int = 8080) -> None: 
        """Configure and create the FastAPI application.

        Args:
            api_host (str): Host interface to bind to.
            api_port (int): Port number to expose the API on.
        """

        # API Server
        self._api_host = api_host
        self._api_port = api_port
        self._api_app = FastAPI(title=f"{self.name} API")
        self._server: Optional[uvicorn.Server] = None
        self._server_task: Optional[asyncio.Task] = None

        # Setup CORS - allow configurable origins via environment variable
        # CORS_ORIGINS can be a comma-separated list: "http://localhost:3000,http://192.168.1.100:3000"
        cors_origins_env = os.getenv("CORS_ORIGINS", "http://localhost:3000")
        cors_origins = [origin.strip() for origin in cors_origins_env.split(",")]
        logger.debug(f"{self.name} CORS allowed origins: {cors_origins}")
        
        self._api_app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
            expose_headers=["*"],
        )

        # Setup Routes
        self._setup_routes()

    @abstractmethod
    def _setup_routes(self) -> None:
        """Register API routes on ``self._api_app``."""
        pass

    async def start_api_server(self) -> None:
        """Start the internal API server."""
        config = uvicorn.Config(
            self._api_app,
            host=self._api_host,
            port=self._api_port,
            log_level="info",
            access_log=False,  # We handle logging ourselves
        )
        self._server = uvicorn.Server(config)
        
        # Run server in background task
        self._server_task = asyncio.create_task(self._server.serve())
        logger.info(f"{self.name} Internal API server started on http://{self._api_host}:{self._api_port}")

    async def stop_api_server(self) -> None:
        """Stop the internal API server and await shutdown."""
        if self._server:
            self._server.should_exit = True
            # Wait for server to shutdown
            if self._server_task:
                try:
                    await asyncio.wait_for(self._server_task, timeout=5.0)
                except asyncio.TimeoutError:
                    logger.warning(f"{self.name} API server shutdown timeout")
                except Exception as e:
                    logger.error(f"{self.name} Error stopping API server: {e}")
                finally:
                    self._server_task = None
            logger.info(f"{self.name} Internal API server stopped.")