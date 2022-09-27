import pandas as pd

from part2.src.pandas.indicators.average_true_range import AverageTrueRange
from part2.src.pandas.indicators.indicator import Indicator


class KeltnerChannels(Indicator):
    def __init__(self, window: int = 20, multiplier: int = 2):
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

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculates the Keltner Channels by using the Average True Range.

        :param df: Dataframe which contains data to calculate the indicator.
        """
        average_true_range = AverageTrueRange(self.window)
        df = average_true_range.run(df)

        df["upper_keltner"] = df["sma"].values + (
            df["average_true_range"].values * self.multiplier
        )
        df["lower_keltner"] = df["sma"].values - (
            df["average_true_range"].values * self.multiplier
        )
        return df
