from typing import List

import polars as pl

from part2.src.utils.indicator import Indicator


class AverageTrueRange(Indicator):
    def __init__(
        self,
        name: str = "Average True Range",
        type: List = ["Volatility"],
        window: int = 20,
    ):
        super().__init__(name, type, window)

    def get_name(self):
        return self.name

    def get_type(self):
        return self.type

    def get_window(self):
        return self.window

    def run(self, df: pl.DataFrame) -> pl.DataFrame:
        """Calculates the true range and average true range

        :param df:  Dataframe which contains data to calculate the indicator
        """
        df = df.select(
            [
                pl.col("sma"),
                (pl.col("high") - pl.col("low")).abs().alias("true_range"),
                (pl.col("high") - pl.col("low"))
                .abs()
                .rolling_mean(self.window)
                .alias("average_true_range"),
            ]
        )
        return df
