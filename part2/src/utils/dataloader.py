import re
from typing import List

import polars as pl


class DataLoader:
    def __init__(self):
        __name__ = "DataLoader"

    def read_file_to_interval_polars(
        self, path: str, file: str, interval: str
    ) -> pl.DataFrame:
        """Creates a Polars dataframe in a specific time interval.

        :param path: Directory that contains the data.
        :param file: Name of the file.
        :param interval: Interval the dataframe should have.
        """
        df = pl.read_csv(path + "/" + file, parse_dates=True)

        if len(df) > 0:
            df = self.to_interval_polars(df, interval)
        return df

    def to_interval_polars(self, df: pl.DataFrame, interval: str) -> pl.DataFrame:
        """Resamples the dataframe to the according interval.

        :param df: Dataframe that has to be resampled.
        :param interval: Interval the dataframe should have.
        """

        # rule, n = self.process_interval(interval)

        df_resampled = df.groupby_dynamic("timestamp", every=interval).agg(
            [
                pl.col("open").first(),
                pl.col("high").max(),
                pl.col("low").min(),
                pl.col("close").last(),
                pl.col("symbol").first(),
            ]
        )

        return df_resampled

    # @staticmethod
    # def process_interval(interval: str):
    #     rule = re.search(r"[A-z]+", interval).group()
    #     n = int(re.search("\d+", interval).group())
    #     return rule, n
