import asyncio
import os.path
import time
from datetime import datetime
from typing import List

import backoff
import polars as pl
from binance import AsyncClient
from binance.exceptions import BinanceAPIException

from src.utils.helpers import deprecated_coins, logger

logger = logger("orderbook")


class Orderbook:
    def __init__(
        self,
        base_currencies: List,
        window_size: str,
        base_path: str,
        # dd-mm-yyyy
        start_date: str = "01-01-2020",
    ):
        """Retrieves all the orderbook data from a crypto-exhange and stores it in a
        predefined location.

        When retrieving data for pairs that have been retrieved before, the new data
        will be appended to the existing file.

        :param base_currencies: symbols against which data can be fetched such as BTC
               ETH, DOT
        :param start_date: The date from which the data should be retrieved.
        """
        self.base_currencies = base_currencies
        self.window_size = window_size
        self.start_date = start_date
        self.base_path = base_path
        self.current_currency = None
        self.current_path = None
        self.deprecated_coins = deprecated_coins

    async def get_available_pairs(self) -> None:
        """Returns information about which pairs are available on binance.

        Note: This will only return pairs that match the given base_currencies.
        """

        all_possible_pairs = await self.retrieve_all_possible_pairs()

        self.filter_possible_pairs(all_possible_pairs=all_possible_pairs)
        return

    async def retrieve_all_possible_pairs(self) -> List:
        """Retrieves the names all possible pairs from the exchange."""
        all_possible_pairs = []

        all_tickers_information = await self.client.get_all_tickers()
        for ticker in range(len(all_tickers_information)):
            all_possible_pairs.append(all_tickers_information[ticker]["symbol"])

        return all_possible_pairs

    def filter_possible_pairs(self, all_possible_pairs: List) -> None:
        """Removes all pairing that have nothing to do with the given base_currencies.

        :param all_possible_pairs: List containing all possible pairs on the exchange.
        """
        self.base_currencies = {
            base_currency: [] for base_currency in self.base_currencies
        }

        for base_currency in self.base_currencies:
            self.base_currencies[
                base_currency
            ] = self.available_symbol_per_base_currency(
                base_currency=base_currency, all_possible_pairs=all_possible_pairs
            )

        return

    def available_symbol_per_base_currency(
        self, base_currency: str, all_possible_pairs: List
    ) -> List:
        """Processes all available pairs to give back only the relevant ones.

        Filters a list of possible pairs, given 3 filters:
        1: Out of all pairs, only get the ones that end with the base_currency
        2: Remove some noise from the list
        3: Remove the coins that are no longer active, but are given back by the api.

        :param base_currency: The base currency against which the data is retrieved.
        :param all_possible_pairs: List of all the possible pairs given a base currency
                                   for a given exchange.
        """
        available_symbols_for_base_currency = [
            pair.rsplit(base_currency)[0]
            for pair in all_possible_pairs
            if pair.endswith(base_currency)
        ]
        available_symbols_for_base_currency = [
            i for i in available_symbols_for_base_currency if len(i) >= 2
        ]
        available_symbols_for_base_currency = [
            ticker
            for ticker in available_symbols_for_base_currency
            if ticker not in self.deprecated_coins
        ]
        if base_currency != "BTC":
            available_symbols_for_base_currency = available_symbols_for_base_currency
        return available_symbols_for_base_currency

    async def get_orderbook(
        self,
        action: str,
        to_write: bool,
    ) -> None:
        """Initialized the retrievement of the orderbook data for every available
        ticker of requested base currency.
        For the function "_get_orderbook_binance", it is required that the string
        provided, is the tickername with BTC appended without spaces such e.g. ETHBTC.

        :param action: The action to perform. Can be "create", "update", "retrieve".
        :param to_write: Whether the data has to be written to a file or not.
        """
        await self.get_available_pairs()

        tasks = []

        for i, base_currency in enumerate(list(self.base_currencies)):
            self.current_currency = base_currency
            self.current_path = self.base_path + self.current_currency

            single_api_call_cost = await self.calculate_single_api_call_cost(
                action=action,
                to_write=to_write,
            )
            if i == 0:
                single_api_call_cost = single_api_call_cost - 2
            maximum_concurrent_calls = self.determine_number_of_concurrent_calls(
                single_api_call_cost=single_api_call_cost, base_currency=base_currency
            )
            # start at 1 to prevent zerodivison when using modulo
            for n, symbol in enumerate(
                iterable=self.base_currencies[base_currency], start=1
            ):
                tasks.append(
                    asyncio.create_task(
                        self._get_orderbook_binance(
                            symbol=symbol, action=action, to_write=to_write
                        )
                    )
                )
                if n % maximum_concurrent_calls == 0:
                    await asyncio.gather(*tasks)

                    time.sleep(self.determine_sleep_period())
                    tasks = []

            if len(tasks) > 0:
                await asyncio.gather(*tasks)

            logger.info("Done")

        return

    def determine_number_of_concurrent_calls(
        self, single_api_call_cost: int, base_currency: str
    ) -> int:
        """Determines how many concurrent calls are possible within a minute.

        Checks how many concurrent calls to the api can be made within a minute for the
        given base_currency.
        The percentage that is used, is to give a margin to be sure that you stay within
        the limit, since sometimes a retry can occur, thus increasing the cost.
        If the actual_number_of_total_calls are less than the max limit, it can be done
        within one minute, else, it has to be divided into smaller chunks.

        :param single_api_call_cost: value representing how much a single api call costs.
        :param base_currency: The base currency against which the data is retrieved.
        """
        API_LIMIT = 1200
        API_CALL_USAGE_PERCENTAGE = 0.80
        actual_number_of_total_calls = len(self.base_currencies[base_currency])

        maximum_number_of_possible_calls = (
            API_LIMIT / single_api_call_cost
        ) * API_CALL_USAGE_PERCENTAGE

        if actual_number_of_total_calls < maximum_number_of_possible_calls:
            return actual_number_of_total_calls
        else:
            return round(maximum_number_of_possible_calls)

    async def calculate_single_api_call_cost(
        self,
        action: str,
        to_write: bool,
    ) -> int:
        """Calculate the cost of a single api call.

        Performs a single call to the api, to check how much it costs to call that api
        giving the start date. The older the given start date is, the higher the cost.
        This is later used to make sure when retrieving all the data, we stay within the
        limits of the api.

        We can ask from the client how much of the limit of the api we have already
        used. But we need to subtract the costs of the creation of the client which
        already costs something.

        :param action: The action to perform. Can be "create", "update", "retrieve".
        :param to_write: Whether the data has to be written to a file or not.
        """
        single_api_call_cost_test_symbol = self.get_single_api_call_cost_test_symbol()
        await self._get_orderbook_binance(
            symbol=single_api_call_cost_test_symbol,
            action=action,
            to_write=to_write,
        )

        single_api_call_cost = int(self.client.response.headers["x-mbx-used-weight"])
        logger.info(f"A single api call will cost: {single_api_call_cost}")
        return single_api_call_cost

    def get_single_api_call_cost_test_symbol(self) -> str:
        """A single call to get information on how much a call will cost to prevent
        rate limits."""
        if self.current_currency.upper() == "BTC":
            symbol = "ETH"
        else:
            raise ValueError(
                f"Current base_currency {self.current_currency} is not "
                f"accounted for yet."
            )
        return symbol

    async def _get_orderbook_binance(
        self,
        symbol: str,
        action: str,
        to_write: bool,
    ) -> None:
        """This functions gathers all the information needed for retrieving the
        orderbook data.

        :param symbol: The tickername + BTC e.g. USDT/BTC
        :param action: The action to perform. Can be "initial load", "update", "retrieve".
        """

        existing_data, filename = self._retrieve_existing_data(
            symbol=symbol, action=action
        )

        (
            oldest_data_point,
            newest_data_point,
        ) = await self.calculate_delta_old_and_new_delta(
            symbol=symbol, start_date=self.start_date, existing_data=existing_data
        )

        orderbook_data = await self._retrieve_orderbook_data(
            symbol=symbol,
            oldest_data_point=oldest_data_point,
            newest_data_point=newest_data_point,
        )
        if to_write:
            logger.info(f"Writing {symbol}/{self.current_currency}")
            self.write_orderbook_data(
                orderbook_data=orderbook_data,
                existing_data_df=existing_data,
                filename=filename,
            )
        return

    async def set_client(self, key: str, secret: str) -> AsyncClient:
        """Creates a connection to the desired crypto exchange api.

        :param key: The api key for the exchange you want to connect to.
        :param secret: The api secret for the exchange you want to connect to.
        """
        self.key = key
        self.secret = secret
        self.client = await AsyncClient.create(api_key=self.key, api_secret=self.secret)
        return self.client

    @backoff.on_exception(backoff.expo, asyncio.TimeoutError, max_tries=3)
    async def _minutes_of_new_data(self, symbol: str) -> int:
        """Retrieves the latest unix timestamp that data is available for given pair

        :param symbol: The tickername + BTC e.g. USDT/BTC
        """

        data = await self.client.get_klines(
            symbol=symbol + self.current_currency.upper(), interval=self.window_size
        )
        return data[-1][0]

    @backoff.on_exception(
        backoff.expo, (asyncio.TimeoutError, BinanceAPIException), max_tries=5
    )
    async def _get_klines_data(
        self,
        symbol: str,
        oldest_data_point: datetime,
        newest_data_point: datetime,
    ) -> List:
        """Retrieves the specified data from the exchange.

        :param symbol: The tickername + BTC e.g. USDT/BTC
        :param oldest_data_point: The oldest available data point from the given startdate.
        :param newest_data_point: The newest available data point.
        """

        # set correct symbol
        symbol = symbol + self.current_currency.upper()
        klines = await self.client.get_historical_klines(
            symbol,
            self.window_size,
            oldest_data_point.strftime("%d %b %Y %H:%M:%S"),
            newest_data_point.strftime("%d %b %Y %H:%M:%S"),
        )
        return klines

    async def calculate_delta_old_and_new_delta(
        self, symbol: str, start_date: str, existing_data: pl.DataFrame
    ) -> tuple[datetime, datetime]:
        """Calculates how much time is between the newest data and given starting point.

        :param symbol: The tickername + BTC e.g. USDT/BTC
        :param existing_data: If applicable, this contains previously saved data of the orderbooks.
        """
        if len(existing_data) > 0:
            oldest_data_point = existing_data["timestamp"][-1]
        else:
            oldest_data_point = datetime.strptime(start_date, "%d-%m-%Y")

        newest_data_point_timestamp = await self._minutes_of_new_data(symbol=symbol)

        newest_data_point = datetime.fromtimestamp(newest_data_point_timestamp / 1000)
        return oldest_data_point, newest_data_point

    @staticmethod
    def determine_sleep_period() -> int:
        """Calculates the time until the start of the next minute"""
        minute = 60
        safety_margin = 2
        current_time = datetime.now()
        sleep_period = (minute - current_time.second) + safety_margin
        return sleep_period

    def _retrieve_existing_data(
        self, symbol: str, action: str
    ) -> tuple[pl.DataFrame, str]:
        """Retrieves existing data from the destination directory or creates empty dataframe

        :param symbol: The tickername + BTC e.g. USDT/BTC.
        :param action: The action to perform. Can be "create", "update", "retrieve".
        """
        filename = "%s/%s-%s-orderbook.csv" % (
            self.current_path,
            symbol,
            self.window_size,
        )
        file_exists = os.path.isfile(filename)

        if file_exists and action == "recreate":
            os.remove(filename)
            df = pl.DataFrame()
        elif file_exists and action == "update":
            df = pl.read_csv(
                file=filename,
                dtypes=[
                    pl.Datetime,
                    pl.Datetime,
                    pl.datatypes.Float64,
                    pl.datatypes.Float64,
                    pl.datatypes.Float64,
                    pl.datatypes.Float64,
                    pl.datatypes.Float64,
                    pl.datatypes.Float64,
                    pl.datatypes.Float64,
                    pl.datatypes.Float64,
                    pl.datatypes.Int64,
                ],
            )
            df = df.with_column(pl.col("timestamp").dt.cast_time_unit("ms"))
        else:
            # initial load
            df = pl.DataFrame()
        return df, filename

    async def _retrieve_orderbook_data(
        self, symbol: str, oldest_data_point: datetime, newest_data_point: datetime
    ) -> pl.DataFrame:
        """Retrieves new data from the exchange and transforms it into the desired
        configuration.

        :param symbol: The tickername + BTC e.g. USDT/BTC.
        :param oldest_data_point: The oldest available data point from the given startdate.
        :param newest_data_point: The newest available data point.
        """
        columns = [
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_av",
            "trades",
            "tb_base_av",
            "tb_quote_av",
            "ignore",
        ]

        klines = await self._get_klines_data(
            symbol=symbol,
            oldest_data_point=oldest_data_point,
            newest_data_point=newest_data_point,
        )

        orderbook_data = pl.DataFrame(klines, columns=columns)
        orderbook_data = orderbook_data.select(
            [
                pl.col(["timestamp", "close_time"]).cast(pl.Datetime("ms")),
                pl.col(
                    [
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "quote_av",
                        "tb_base_av",
                        "tb_quote_av",
                    ]
                ).cast(pl.Float64),
                pl.col("trades"),
                pl.lit(symbol).alias("symbol"),
            ]
        )

        return orderbook_data

    @staticmethod
    def write_orderbook_data(
        orderbook_data: pl.DataFrame, existing_data_df: pl.DataFrame, filename: str
    ) -> None:
        """Writes the retrieved data to disk.

        :param orderbook_data: The newly retrieved orderbook data.
        :param existing_data_df: The dataframe that contains the (if applicable)
        previous saved data that will be appended to.
        :param filename: The name of the file to be saved.
        """
        file_exists = len(existing_data_df) > 0
        if file_exists:
            # drop first row to prevent duplicates when concatenating dataframes
            orderbook_data = orderbook_data[1:]
            orderbook_data = existing_data_df.vstack(orderbook_data)
            add_header = False
        else:
            add_header = True

        # necessary otherwise the record with be written with a "T" between date and time
        orderbook_data = orderbook_data.with_column(
            pl.col("timestamp").dt.strftime("%Y-%m-%d %H:%M:%S")
        )
        orderbook_data.write_csv(file=filename, has_header=add_header)
        return
