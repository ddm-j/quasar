import asyncio, os, logging
from quasar.registry import Registry

level = os.getenv("LOGLEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, level, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s"
)

async def main():
    # Load Environment
    dsn   = os.environ["DSN"]                      # provided by compose

    # Create Registry
    reg = Registry(dsn=dsn)
    await reg.start()

    logging.info("Registry started → DSN=%s", dsn)
    try:
        await asyncio.Event().wait()              # keep running
    finally:
        await reg.stop()
        logging.info("Registry stopped")

if __name__ == "__main__":
    asyncio.run(main())