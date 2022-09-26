import os

import polars as pl

from part2.src.utils.helpers import logger

logger = logger("breakout")


def is_consolidating(df, file, percentage=0.2):
    breakout_range = 15
    recent_candlesticks = df[-breakout_range:]

    max_close = recent_candlesticks["close"].max()
    min_close = recent_candlesticks["close"].min()

    threshold = 1 - (percentage / 100)
    if min_close > (max_close * threshold):
        timestamp = recent_candlesticks[-1, "timestamp"]
        ticker = file.split("-")[0]
        # action = Settings.CONSOLIDATION
        timescale = file.split("-")[1]
        # write(timestamp, ticker, action, timescale, args.percentage)
        print(f"{ticker} is consolidating {timestamp}")
        return

    return False


def is_breaking_out(df, file, percentage=2.0):
    last_close = df[-1, "close"]
    breakout_range = 15
    if is_consolidating(df[:-1], file, percentage=percentage):
        recent_candlesticks = df[-breakout_range:]

        if last_close > recent_candlesticks["close"].max():
            timestamp = recent_candlesticks[-1, "timestamp"]
            ticker = file.split("-")[0]
            # action = Settings.BREAKINGOUT
            timescale = file.split("-")[1]
            # write(timestamp, ticker, action, timescale, args.percentage)
            print(f"{ticker} is breaking out {timestamp}")
            return

    return False


def main(
    data_directory="/Users/romanovacca/Documents/Coding/git_projects/polars-crypto-application-part-1/part-1/data/USDT/",
):
    logger.info("")

    for file in os.listdir(data_directory):
        if file.endswith(".csv"):
            df = pl.read_csv(data_directory + file)
            if len(df) > 0:
                is_breaking_out(df, file)

    logger.info("Finished calculating breakouts.")


if __name__ == "__main__":
    main()
