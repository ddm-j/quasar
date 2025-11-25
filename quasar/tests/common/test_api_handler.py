"""Tests for APIHandler."""
import pytest
from unittest.mock import Mock, AsyncMock, MagicMock, patch
import asyncio
import uvicorn

from quasar.common.api_handler import APIHandler


class ConcreteAPIHandler(APIHandler):
    """Concrete implementation of APIHandler for testing."""
    name = "TestAPIHandler"
    
    def _setup_routes(self) -> None:
        """Set up test routes."""
        self._api_app.get("/test")(lambda: {"status": "ok"})


class TestAPIHandler:
    """Tests for APIHandler."""
    
    @pytest.mark.asyncio
    async def test_start_api_server_starts_server_in_background(self):
        """Test that start_api_server starts FastAPI server in background."""
        handler = ConcreteAPIHandler(api_host="127.0.0.1", api_port=0)
        
        # Mock uvicorn.Server
        mock_server = MagicMock()
        mock_server.serve = AsyncMock()
        mock_server_task = AsyncMock()
        
        with patch('quasar.common.api_handler.uvicorn.Server', return_value=mock_server):
            with patch('quasar.common.api_handler.asyncio.create_task', return_value=mock_server_task):
                await handler.start_api_server()
                
                assert handler._server == mock_server
                assert handler._server_task == mock_server_task
    
    @pytest.mark.asyncio
    async def test_stop_api_server_stops_server_normally(self):
        """Test that stop_api_server gracefully stops server."""
        handler = ConcreteAPIHandler()
        handler._server = MagicMock()
        handler._server.should_exit = False
        handler._server_task = AsyncMock()
        
        # Mock task that completes successfully
        async def mock_wait():
            await asyncio.sleep(0.01)
            return None
        
        handler._server_task = asyncio.create_task(mock_wait())
        
        await handler.stop_api_server()
        
        assert handler._server.should_exit is True
        assert handler._server_task is None
    
    @pytest.mark.asyncio
    async def test_stop_api_server_handles_timeout(self):
        """Test that stop_api_server handles timeout gracefully."""
        handler = ConcreteAPIHandler()
        handler._server = MagicMock()
        handler._server.should_exit = False
        
        # Mock task that never completes (timeout)
        async def mock_wait_forever():
            await asyncio.sleep(10)
        
        handler._server_task = asyncio.create_task(mock_wait_forever())
        
        # Should complete within timeout (5 seconds) and log warning
        await handler.stop_api_server()
        
        assert handler._server.should_exit is True
        assert handler._server_task is None
    
    @pytest.mark.asyncio
    async def test_stop_api_server_noop_if_no_server(self):
        """Test that stop_api_server does nothing if server not started."""
        handler = ConcreteAPIHandler()
        handler._server = None
        handler._server_task = None
        
        # Should not raise error
        await handler.stop_api_server()
    
    def test_init_creates_fastapi_app(self):
        """Test that __init__ creates FastAPI app with correct title."""
        handler = ConcreteAPIHandler()
        
        assert handler._api_app is not None
        assert handler._api_host == "0.0.0.0"
        assert handler._api_port == 8080
    
    def test_init_sets_custom_host_and_port(self):
        """Test that __init__ accepts custom host and port."""
        handler = ConcreteAPIHandler(api_host="127.0.0.1", api_port=9000)
        
        assert handler._api_host == "127.0.0.1"
        assert handler._api_port == 9000

