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
    """A class that serves a FastAPI web application and manages it's lifecycle."""

    @property
    @abstractmethod
    def name(self) -> str:                   # Name of the API handler
        ...

    def __init__(
            self,
            api_host: str = '0.0.0.0',
            api_port: int = 8080) -> None: 

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
        """
        Subclasses must implement this method to add their routes 
        to self._api_app.
        Example: self._api_app.router.add_api_route('/status', self.handle_status, methods=['GET'])
        Or use APIRouter: router = APIRouter(); router.get('/status')(self.handle_status)
        """
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
        """Stop the internal API server."""
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