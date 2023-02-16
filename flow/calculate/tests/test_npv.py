import numpy_financial as npf
import pandas as pd
from pandas._testing import assert_series_equal

from flow.calculate.npv import net_present_value


def test_net_present_value():
    """Should get same npv results as numpy financial"""
    rate = 0.05
    df = pd.DataFrame(data={"a": [500] * 10, "b": [1000] * 10})

    npv_ours = net_present_value(df=df, cols=["a", "b"], rate=rate)
    npv_ours_nocols = net_present_value(df=df, rate=rate)
    npv_npf = df.apply(lambda column: npf.npv(rate=rate, values=column), axis="rows")

    assert_series_equal(npv_ours, npv_npf)
    assert_series_equal(npv_ours_nocols, npv_npf)
