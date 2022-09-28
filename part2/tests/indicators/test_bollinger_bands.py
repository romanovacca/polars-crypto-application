import polars as pl

from part2.src.indicators.bollinger_bands import BollingerBands


def test_bollinger_bands_run(mock_dataframe):
    multiplier_value = 2
    bollinger_bands = BollingerBands(multiplier=multiplier_value)
    bb = bollinger_bands.run(mock_dataframe)
    assert isinstance(bb, pl.DataFrame)
    assert bb.columns == ["sma", "std", "upper_band", "lower_band"]
    assert bb[-1, "upper_band"] == 158.60873443616492
    assert bb[-1, "lower_band"] == 19.69126556383506
