from typing import List

import polars as pl

from part2.src.utils.indicator import Indicator


class BollingerBands(Indicator):
    def __init__(
        self,
        name: str = "Bollinger Bands",
        type: List = ["Volatility"],
        window: int = 20,
        multiplier: int = 2,
    ):
        super().__init__(name, type, window)
        self.multiplier = multiplier

    def get_name(self):
        return self.name

    def get_type(self):
        return self.type

    def get_window(self):
        return self.window

    def run(self, df: pl.DataFrame) -> pl.DataFrame:
        """Calculates the upper and lower bounds of the bollinger bands.

        :param df: Dataframe which contains data to calculate the indicator
        """
        df = df.select(
            [
                pl.col("close").rolling_mean(self.window).alias("sma"),
                pl.col("close").rolling_std(self.window).alias("std"),
                (
                    pl.col("close").rolling_mean(self.window)
                    + (self.multiplier * pl.col("close").rolling_std(self.window))
                ).alias("upper_band"),
                (
                    pl.col("close").rolling_mean(self.window)
                    - (self.multiplier * pl.col("close").rolling_std(self.window))
                ).alias("lower_band"),
            ]
        )
        return df
