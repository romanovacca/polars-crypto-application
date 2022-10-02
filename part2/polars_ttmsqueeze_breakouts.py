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
