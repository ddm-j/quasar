import asyncio, os, logging
from quasar.datahub import DataHub
from quasar.common.secret_store import SecretStore

level = os.getenv("LOGLEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, level, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s"
)

async def main():
    # Load Environment
    dsn   = os.environ["DSN"]                      # provided by compose
    mode  = os.getenv("QUASAR_SECRET_MODE", "auto")

    # Create Secret Store
    store = SecretStore(mode=mode)

    # Create DataHub
    hub = DataHub(secret_store=store, dsn=dsn)
    await hub.start()

    logging.info("DataHub started → DSN=%s", dsn)
    try:
        await asyncio.Event().wait()              # keep running
    finally:
        await hub.stop()
        logging.info("DataHub stopped")

if __name__ == "__main__":
    asyncio.run(main())