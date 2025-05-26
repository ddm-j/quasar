import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, date, timezone, timedelta

from quasar.datahub.core import DataHub, DEFAULT_LOOKBACK
from quasar.providers.core import Bar, Req

class AsyncIter:    
    def __init__(self, items):    
        self.items = list(items)  # Convert to list to support multiple iterations

    def __aiter__(self):    
        return self

    async def __anext__(self):    
        if not self.items:
            raise StopAsyncIteration
        return self.items.pop(0)

class TestDataHub(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        """Set up test fixtures for each test method"""
        # Create the mock pool
        self.mock_pool = AsyncMock()
        self.mock_conn = AsyncMock()
        
        # Create a proper async context manager
        class AsyncContextManager:
            def __init__(self, conn):
                self.conn = conn
            async def __aenter__(self):
                return self.conn
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

        # Set up the pool.acquire to return our context manager
        self.mock_pool.acquire = lambda: AsyncContextManager(self.mock_conn)

        self.mock_secret_store = AsyncMock()
        
        # Configure default mock behaviors
        self.mock_conn.fetch.return_value = []
        self.mock_secret_store.get.return_value = {"api_key": "test"}
        
        self.datahub = DataHub(
            secret_store=self.mock_secret_store,
            pool=self.mock_pool,
            refresh_seconds=1
        )

    async def asyncTearDown(self):
        """Clean up after each test"""
        await self.datahub.stop()

    async def test_initialization(self):
        """Test DataHub initialization requirements"""
        # Should raise when neither pool nor dsn provided
        with self.assertRaises(ValueError):
            DataHub(secret_store=self.mock_secret_store)

        # Should accept DSN string
        hub = DataHub(
            secret_store=self.mock_secret_store,
            dsn="postgresql://test:test@localhost:5432/test"
        )
        self.assertIsNone(hub._pool)
        self.assertEqual(hub._dsn, "postgresql://test:test@localhost:5432/test")

        # Should accept pool directly
        hub = DataHub(
            secret_store=self.mock_secret_store,
            pool=self.mock_pool
        )
        self.assertEqual(hub._pool, self.mock_pool)

    async def test_start_and_stop(self):
        """Test startup and shutdown behavior"""
        # Test start with DSN
        test_dsn = "postgresql://test:test@localhost:5432/test"
        hub = DataHub(secret_store=self.mock_secret_store, dsn=test_dsn)
        
        with patch('asyncpg.create_pool', new_callable=AsyncMock) as mock_create_pool:
            mock_create_pool.return_value = self.mock_pool
            await hub.start()
            mock_create_pool.assert_called_once_with(test_dsn)
        
        # Test stop behavior
        await hub.stop()
        self.assertTrue(len(hub.job_keys) == 0)

    async def test_refresh_subscriptions_providers(self):
        """Test subscription refresh logic"""
        # Mock subscription data
        mock_subscriptions = [
            {
                "provider": "TestProvider",
                "interval": "1d",
                "cron": "0 0 * * *",
                "syms": ["AAPL", "MSFT"]
            }
        ]
        self.mock_pool.fetch.return_value = mock_subscriptions

        # Test Unable to Load Provider Config
        self.datahub._providers = {}
        self.mock_secret_store.get.side_effect = Exception("Provider not found")
        await self.datahub._refresh_subscriptions()
        self.assertEqual(len(self.datahub._providers), 0)

        # Test Successful Provider Class Load
        self.datahub._providers = {}
        self.mock_secret_store.get.side_effect = None
        with patch('quasar.datahub.core.load_provider') as mock_load:
            mock_provider = MagicMock()
            mock_load.return_value = lambda **kwargs: mock_provider
            
            await self.datahub._refresh_subscriptions()
            
            self.assertIn("TestProvider", self.datahub._providers)
            self.assertEqual(len(self.datahub.job_keys), 1)

        # Test Unsuccessful Provider Class Load
        self.datahub._providers = {}
        self.mock_secret_store.get.side_effect = None
        with patch('quasar.datahub.core.load_provider') as mock_load:
            mock_provider = MagicMock()
            mock_load.side_effect = Exception("Failed to load provider code")
            
            await self.datahub._refresh_subscriptions()
            
            self.assertNotIn("TestProvider", self.datahub._providers)
            self.assertEqual(len(self.datahub.job_keys), 0)

        # Test Remove Provider
        self.datahub._providers = {"ObsoleteProvider": AsyncMock()}
        self.mock_secret_store.get.side_effect = None
        with patch('quasar.datahub.core.load_provider') as mock_load:
            mock_provider = MagicMock()
            mock_load.return_value = lambda **kwargs: mock_provider
            
            await self.datahub._refresh_subscriptions()
            
            self.assertNotIn("ObsoleteProvider", self.datahub._providers)
            self.assertEqual(len(self.datahub.job_keys), 1)

    async def test_build_reqs(self):
        """Test request building logic"""
        today = datetime.now(timezone.utc).date()
        symbols = ["AAPL", "MSFT"]
        
        # Test with existing data
        self.mock_conn.fetch.return_value = [
            {"sym": "AAPL", "d": today - timedelta(days=1)},
            {"sym": "MSFT", "d": today - timedelta(days=2)}
        ]
        
        reqs = await self.datahub._build_reqs("TestProvider", "1d", symbols)
        
        self.assertEqual(len(reqs), 1)  # Only MSFT should need updating
        self.assertEqual(reqs[0].sym, "MSFT")
        self.assertEqual(reqs[0].interval, "1d")

    async def test_insert_bars(self):
        """Test bar insertion logic"""
        test_bars = [
            Bar(
                ts=datetime.now(timezone.utc),
                sym="AAPL",
                o=150.0,
                h=151.0,
                l=149.0,
                c=150.5,
                v=1000000
            )
        ]

        await self.datahub._insert_bars("TestProvider", "1d", test_bars)

        self.mock_conn.copy_records_to_table.assert_called_once()
        args = self.mock_conn.copy_records_to_table.call_args[1]
        self.assertEqual(args['records'][0][1], "AAPL")  # Check symbol
        self.assertEqual(args['records'][0][2], "TestProvider")  # Check provider

    async def test_provider_job(self):
        """Test the provider job execution"""
        symbols = ["AAPL"]

        # Setup mock provider response
        bar = Bar(
            ts=datetime.now(timezone.utc),
            sym="AAPL",
            o=150.0,
            h=151.0,
            l=149.0,
            c=150.5,
            v=1000000
        ) 

        # Create async iterator for mock response
        mock_provider = MagicMock()
        mock_provider.get_history_many.return_value = AsyncIter([bar])
        self.datahub._providers["TestProvider"] = mock_provider

        # Mock build_reqs to return a test request
        test_req = Req(
            sym="AAPL",
            start=date.today() - timedelta(days=1),
            end=date.today(),
            interval="1d"
        )

        with patch.object(self.datahub, '_build_reqs', 
                        new_callable=AsyncMock,
                        return_value=[test_req]):
            # Execute job
            await self.datahub._provider_job("TestProvider", "1d", symbols)

        # Verify interactions
        mock_provider.get_history_many.assert_called_once()
        self.mock_conn.copy_records_to_table.assert_called_once()

if __name__ == '__main__':
    unittest.main()