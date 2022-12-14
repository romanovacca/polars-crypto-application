import asyncio
import configparser

from src.utils.helpers import logger
from src.utils.orderbook import Orderbook

logger = logger("main")
config = configparser.ConfigParser()
config.read("config.ini")


async def main():
    logger.info("Started main.")
    orderbook = Orderbook(base_currencies=["USDT"], window_size="5m", base_path="data/")

    logger.info("Initializing Binance exchange client.")
    client = await orderbook.set_client(
        key=config["BINANCE"]["API_KEY"], secret=config["BINANCE"]["API_SECRET"]
    )

    logger.info("Start retrieving all data.")
    await orderbook.get_orderbook(action="update", to_write=True)

    logger.info("Done processing all data.")
    await client.close_connection()


if __name__ == "__main__":
    asyncio.run(main())
