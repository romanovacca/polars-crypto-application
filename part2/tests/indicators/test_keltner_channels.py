import polars as pl

from part2.src.indicators.keltner_channels import KeltnerChannels


def test_bollinger_bands_run(mock_dataframe):
    multiplier_value = 2
    keltner_channels = KeltnerChannels(multiplier=multiplier_value)
    kc = keltner_channels.run(mock_dataframe)
    assert isinstance(kc, pl.DataFrame)
    assert kc.columns == ["upper_keltner", "lower_keltner"]
    assert kc[-1, "upper_keltner"] == 87.3
    assert kc[-1, "lower_keltner"] == 31.299999999999997
