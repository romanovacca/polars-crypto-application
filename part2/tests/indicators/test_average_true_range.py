import polars as pl

from part2.src.indicators.keltner_channels import AverageTrueRange


def test_average_true_range_run(mock_dataframe):
    average_true_range = AverageTrueRange()
    atr = average_true_range.run(mock_dataframe)
    assert isinstance(atr, pl.DataFrame)
    assert atr.columns == ["sma", "true_range", "average_true_range"]
    assert atr[-1, "true_range"] == 4.0
    assert atr[-1, "average_true_range"] == 14.0
