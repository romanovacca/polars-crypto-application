from typing import List

import pandas as pd

from part2.src.pandas.indicators.bollinger_bands import BollingerBands
from part2.src.pandas.indicators.keltner_channels import KeltnerChannels
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
        breakouts = self._scan_for_squeeze_breakouts_pandas(
            path=path, file=file, intervals=intervals, dataloader=dataloader
        )
        return breakouts

    def _scan_for_squeeze_breakouts_pandas(
        self, path: str, file: str, intervals: List, dataloader
    ) -> List:
        breakouts = []
        for i, interval in enumerate(intervals):
            if i == 0:
                # first iteration read the file and change it to a set interval
                df = dataloader.read_file_to_interval_pandas(path, file, interval)

                # if the file doesn't have row go to the next one
                if len(df) == 0:
                    break

            else:
                # second iteration no need to read file again but change the df interval
                df = dataloader.to_interval_pandas(df, interval)

            df = self._gather_squeeze_indicators_pandas(df)
            is_breaking_out_of_squeeze = self.is_breaking_out_pandas(df)

            if is_breaking_out_of_squeeze:
                symbol = file.split("-")[0]
                breakouts.append(f"{symbol} is breaking out at {interval} interval.")
        return breakouts

    def _gather_squeeze_indicators_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Gathers the data for the different indicators that are needed for
        calculating if a squeeze is present or not.

        :param df: Dataframe which contains data to calculate the indicator
        """

        bollinger_bands = BollingerBands(multiplier=self.bb_multiplier)
        keltner_channels = KeltnerChannels(multiplier=self.kc_multiplier)

        df = bollinger_bands.run(df)
        df = keltner_channels.run(df)
        df = self._calculate_squeeze_pandas(df)
        return df

    def _calculate_squeeze_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculates if a squeeze is present or not.

        :param df: Dataframe which contains data to calculate the indicator
        """
        df["squeeze_on"] = (df["lower_band"] > df["lower_keltner"]) & (
            df["upper_band"] < df["upper_keltner"]
        )
        df["squeeze_off"] = (df["lower_band"] < df["lower_keltner"]) & (
            df["upper_band"] > df["upper_keltner"]
        )
        return df

    def is_breaking_out_pandas(self, df: pd.DataFrame) -> bool:
        """Checks if the symbol is breaking out of the squeeze at the current
        timestamp.

        To determine if a squeeze is over, the last datapoint should not be in a
        squeeze, while the datapoint before that, was in a squeeze. This is the exact
        moment it has broken out of a squeeze range.

        :param df: Dataframe which contains data to determine if symbol is breaking out.
        """
        if len(df) >= 2:
            last_value_off = df["squeeze_off"].values[-1]
            second_to_last_value_on = df["squeeze_on"].values[-2]
            if last_value_off and second_to_last_value_on:
                return True
