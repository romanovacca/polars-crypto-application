from typing import List

import polars as pl
from polars import col

from part2.src.indicators.bollinger_bands import BollingerBands
from part2.src.indicators.keltner_channels import KeltnerChannels
from part2.src.utils.indicator import Indicator


class TTMSqueeze(Indicator):
    def __init__(
        self,
        name: str = "ttm_squeeze",
        type: List = ["Volatility", "Momentum"],
        window: int = 20,
        bb_multiplier: int = 2,
        kc_multiplier: int = 2,
    ):
        """

        :param window: Length of the interval that should be used.
        :param bb_multiplier:
        :param kc_multiplier:
        """
        super().__init__(name, type, window)
        self.bb_multiplier = bb_multiplier
        self.kc_multiplier = kc_multiplier

    def get_name(self):
        return self.name

    def get_type(self):
        return self.type

    def get_window(self):
        return self.window

    def run(self, path: str, file: str, intervals: List, dataloader) -> List:
        """Performs calculation to determine if a squeeze is present.

        Returns a list that contains all the symbols that are breaking out of a squeeze.

        :param path: Directory that contains the relevant file.
        :param file: Name of the relevant file.
        :param intervals: Interval timeframe that should be checked.
        :param dataloader: Instance of module that contains function wrt data.
        """
        breakouts = self._scan_for_squeeze_breakouts_polars(
            path=path, file=file, intervals=intervals, dataloader=dataloader
        )
        return breakouts

    def _scan_for_squeeze_breakouts_polars(
        self, path: str, file: str, intervals: List, dataloader
    ) -> List:
        breakouts = []
        for i, interval in enumerate(intervals):
            if i == 0:
                # first iteration read the file and change it to a set interval
                df = dataloader.read_file_to_interval_polars(path, file, interval)

                # if the file doesn't have row go to the next one
                if len(df) == 0:
                    break
            else:
                # second iteration no need to read file again but change the df interval
                df = dataloader.to_interval_polars(df, interval)

            df = self._gather_squeeze_indicators_polars(df)
            is_breaking_out_of_squeeze = self.is_breaking_out_polars(df)

            if is_breaking_out_of_squeeze:
                symbol = file.split("-")[0]
                breakouts.append(f"{symbol} is breaking out at {interval} interval.")
        return breakouts

    def _gather_squeeze_indicators_polars(self, df: pl.DataFrame) -> pl.DataFrame:
        """Gathers the data for the different indicators that are needed for
        calculating if a squeeze is present or not.

        :param df: Dataframe which contains data to calculate the indicator
        """

        bollinger_bands = BollingerBands(multiplier=self.bb_multiplier)
        keltner_channels = KeltnerChannels(multiplier=self.kc_multiplier)

        df = df.hstack(bollinger_bands.run(df))
        df = df.hstack(keltner_channels.run(df))
        df = df.hstack(self._calculate_squeeze_polars(df))
        return df

    def _calculate_squeeze_polars(self, df: pl.DataFrame) -> pl.DataFrame:
        """Calculates if a squeeze is present or not.

        :param df: Dataframe which contains data to calculate the indicator
        """
        df = df.select(
            [
                (
                    (col("lower_band") > col("lower_keltner"))
                    & (col("upper_band") < col("upper_keltner"))
                ).alias("squeeze_on"),
                (
                    (col("lower_band") < col("lower_keltner"))
                    & (col("upper_band") > col("upper_keltner"))
                ).alias("squeeze_off"),
            ]
        )
        return df

    def is_breaking_out_polars(self, df: pl.DataFrame) -> bool:
        """Checks if the symbol is breaking out of the squeeze at the current
        timestamp.

        To determine if a squeeze is over, the last datapoint should not be in a
        squeeze, while the datapoint before that, was in a squeeze. This is the exact
        moment it has broken out of a squeeze range.

        :param df: Dataframe which contains data to determine if symbol is breaking out.
        """
        if len(df) >= 2:
            last_value_off = df[-1, "squeeze_off"]
            second_to_last_value_on = df[-2, "squeeze_on"]
            if last_value_off and second_to_last_value_on:
                return True
