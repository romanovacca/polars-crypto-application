from typing import List

import pandas as pd

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

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculates the upper and lower bounds of the bollinger bands.

        :param df: Dataframe which contains data to calculate the indicator
        """

        df["sma"] = df["close"].rolling(window=self.window).mean()
        df["std"] = df["close"].rolling(window=self.window).std()
        df["upper_band"] = df["sma"].values + (self.multiplier * df["std"].values)
        df["lower_band"] = df["sma"].values - (self.multiplier * df["std"].values)
        return df
