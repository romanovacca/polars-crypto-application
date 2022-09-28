import polars as pl

from part2.src.indicators.ttm_squeeze import TTMSqueeze


def test_initialize_TTMSqueeze():
    ttm_squeeze = TTMSqueeze()
    assert ttm_squeeze


def test_is_breaking_out_polars_succes():
    test_df = pl.DataFrame(
        {
            "squeeze_on": [True, False],
            "squeeze_off": [False, True],
        }
    )
    ttm_squeeze = TTMSqueeze()
    output = ttm_squeeze.is_breaking_out_polars(test_df)
    assert output


def test_is_breaking_out_polars_failure(mock_dataframe):
    ttm_squeeze = TTMSqueeze()
    output = ttm_squeeze.is_breaking_out_polars(mock_dataframe)
    assert output is None


def test_calculate_squeeze_polars(mock_dataframe):
    ttm_squeeze = TTMSqueeze()
    output = ttm_squeeze.calculate_squeeze_polars(mock_dataframe)
    assert output.columns == ["squeeze_on", "squeeze_off"]
