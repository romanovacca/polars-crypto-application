import pandas as pd

from part2.src.pandas.indicators.indicator import Indicator


class AverageTrueRange(Indicator):
    def __init__(self, window: int = 20):
        self.name = "Average True Range"
        self.type = ["Volatility"]
        self.window = window

    def get_name(self):
        return self.name

    def get_type(self):
        return self.type

    def get_window(self):
        return self.window

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculates the true range and average true range

        :param df:  Dataframe which contains data to calculate the indicator
        """
        df["true_range"] = abs(df["high"] - df["low"])
        df["average_true_range"] = df["true_range"].rolling(window=self.window).mean()
        return df
