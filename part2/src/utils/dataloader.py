import pandas as pd
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

    def read_file_to_interval_pandas(
        self, path: str, file: str, interval: str
    ) -> pd.DataFrame:
        """Reads a file and converts it to requested timestamp using pandas

        :param path: Directory that contains the data.
        :param file: Name of the file.
        :param interval: Interval the dataframe should have.
        """
        df = pd.read_csv(path + "/" + file)
        df.index = pd.DatetimeIndex(df["timestamp"])
        if len(df) > 0:
            df = self.to_interval_pandas(df, interval)
        return df

    def to_interval_pandas(self, df: pd.DataFrame, interval: str) -> pd.DataFrame:
        """Resamples the dataframe to the according interval.

        :param df: Dataframe that has to be resampled.
        :param interval: Interval the dataframe should have.
        """

        df_resampled = pd.DataFrame()
        df_resampled["open"] = df["open"].resample(interval).first()
        df_resampled["high"] = df["high"].resample(interval).max()
        df_resampled["low"] = df["low"].resample(interval).min()
        df_resampled["close"] = df["close"].resample(interval).last()
        df_resampled["symbol"] = df["symbol"][0]
        return df_resampled

    def to_interval_polars(self, df: pl.DataFrame, interval: str) -> pl.DataFrame:
        """Resamples the dataframe to the according interval.

        :param df: Dataframe that has to be resampled.
        :param interval: Interval the dataframe should have.
        """

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
