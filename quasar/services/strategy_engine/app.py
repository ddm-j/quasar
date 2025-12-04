import asyncio
import logging
import os

from quasar.services.strategy_engine import StrategyEngine

level = os.getenv("LOGLEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, level, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)


async def main() -> None:
    dsn = os.environ["DSN"]
    engine = StrategyEngine(dsn=dsn)
    await engine.start()

    logging.info("StrategyEngine started → DSN=%s", dsn)
    try:
        await asyncio.Event().wait()
    finally:
        await engine.stop()
        logging.info("StrategyEngine stopped")


if __name__ == "__main__":
    asyncio.run(main())

