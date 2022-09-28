import polars as pl
import pytest


@pytest.fixture()
def mock_dataframe():
    return pl.DataFrame(
        {
            "timestamp": ["2021-01-01 06:00:00"] * 150,
            "open": [float(99), float(55.3)] * 75,
            "high": [float(120), float(55.3)] * 75,
            "low": [float(96), float(59.3)] * 75,
            "close": [float(123), float(55.3)] * 75,
            "symbol": ["TEST"] * 150,
            "sma": [float(105), float(59.3)] * 75,
            "std": [float(120), float(55.3)] * 75,
            "upper_band": [float(99), float(55.3)] * 75,
            "lower_band": [float(105), float(59.3)] * 75,
            "true_range": [float(120), float(55.3)] * 75,
            "average_true_range": [float(99), float(55.3)] * 75,
            "upper_keltner": [float(105), float(59.3)] * 75,
            "lower_keltner": [float(105), float(59.3)] * 75,
            "squeeze_on": [False, True] * 75,
            "squeeze_off": [True, False] * 75,
        }
    )
