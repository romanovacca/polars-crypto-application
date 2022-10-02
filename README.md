# Description
This code is part of a multi series project, where data will be retrieved from the binance exchange using Polars and asyncio.
Next to that technical indicators are created and strategies are built on top of them.

## Documentation

The documentation can be found [here](https://medium.com/@romanovacca),
as it is described in the blogposts.

# Requirements
In order to run the code, you must have the following:
* Binance key/secret in the `config.ini` with read permissions.
* A directory `data/XXX`, where the data will be stored. (e.g BTC or USDT)

## Getting started
The code is created using Python 3.10.

To create a python env run: <br>
```conda create --name <env> python=3.10 --file requirements.txt```


# Usage

#####  part2/get_data.py - Retrieve data from Binance exchange


```python
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

```
----
<br>
<br>

#####  part2/polars_ttmsqueeze_breakouts.py - Get a list of coins breaking out

```python
import os
import time
from typing import List

from part2.src.indicators.ttm_squeeze import TTMSqueeze
from part2.src.utils.dataloader import DataLoader
from part2.src.utils.helpers import logger

logger = logger("polars_ttm_squeeze_breakouts")


def run_indicator_for_all_files(
    path: str, intervals: List, squeeze_indicator: TTMSqueeze
):
    dataloader = DataLoader()

    for file in os.listdir(path):
        if file.endswith(".csv"):
            breakout = squeeze_indicator.run(
                path=path, file=file, intervals=intervals, dataloader=dataloader
            )
            if breakout:
                logger.info(breakout)

    return


def main_polars():
    logger.info("Started main.")

    ttm_squeeze = TTMSqueeze()
    start_time = time.time()
    run_indicator_for_all_files(
        path="data/USDT/",
        intervals=["1h", "2h", "4h", "8h", "12h", "1d", "2d", "3d", "1w"],
        squeeze_indicator=ttm_squeeze,
    )
    logger.info("whole loop --- %s seconds ---" % (time.time() - start_time))


if __name__ == "__main__":
    main_polars()

```
