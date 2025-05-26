from typing import Optional
from abc import ABC, abstractmethod
from aiohttp import web
import aiohttp_cors

import logging
logger = logging.getLogger(__name__)

class APIHandler(ABC):
    """A class that serves an aiohttp web application and manages it's lifecycle."""

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
        self._api_app = web.Application()
        self._api_runner: Optional[web.AppRunner] = None
        self._api_site: Optional[web.TCPSite] = None

        # Setup CORS
        self._cors = aiohttp_cors.setup(self._api_app, defaults={
            "http://localhost:3000": aiohttp_cors.ResourceOptions(
                allow_credentials=True,
                expose_headers="*",
                allow_headers="*",
                allow_methods=["GET", "POST"],
            ),
        })

        # Setup Routes
        self._setup_routes()

    @abstractmethod
    def _setup_routes(self) -> None:
        """
        Subclasses must implement this method to add their routes 
        to self._api_app.
        Example: self._api_app.add_routes([web.get('/status', self.handle_status)])
        """
        pass

    async def start_api_server(self) -> None:
        """Start the internal API server."""
        self._api_runner = web.AppRunner(self._api_app)
        await self._api_runner.setup()
        self._api_site = web.TCPSite(self._api_runner, self._api_host, self._api_port)
        await self._api_site.start()
        logger.info(f"{self.name} Internal API server started on http://{self._api_host}:{self._api_port}")

    async def stop_api_server(self) -> None:
        """Stop the internal API server."""
        if self._api_site:
            await self._api_site.stop()
            logger.info(f"{self.name} Internal API server stopped.")
        if self._api_runner:
            await self._api_runner.cleanup()
            logger.info(f"{self.name} Internal API runner cleaned up.")