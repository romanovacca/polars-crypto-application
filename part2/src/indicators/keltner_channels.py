import polars as pl

from part2.src.indicators.average_true_range import AverageTrueRange
from part2.src.indicators.indicator import Indicator


class KeltnerChannels(Indicator):
    def __init__(self, window: int, multiplier: int = 2):
        self.name = "Keltner Channels"
        self.type = ["Volatility"]
        self.window = window
        self.multiplier = multiplier

    def get_name(self):
        return self.name

    def get_type(self):
        return self.type

    def get_window(self):
        return self.window

    def run(self, df: pl.DataFrame) -> pl.DataFrame:
        """Calculates the Keltner Channels by using the Average True Range.

        :param df: Dataframe which contains data to calculate the indicator.
        """
        average_true_range = AverageTrueRange(self.window)
        df = average_true_range.run(df)

        df = df.select(
            [
                (
                    pl.col("sma") + (pl.col("average_true_range") * self.multiplier)
                ).alias("upper_keltner"),
                (
                    pl.col("sma") - (pl.col("average_true_range") * self.multiplier)
                ).alias("lower_keltner"),
            ]
        )
        return df
