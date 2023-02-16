from typing import Optional

import numpy as np
import pandas as pd


def net_present_value(df: pd.DataFrame, rate: float, cols: Optional[list[str]] = None):
    """
    Calculate net present value (NPV) of multiple dataframe columns at once

    Args:
        df: The data
        rate: discount rate
        cols: The columns to calculate NPV of (if None, use all columns)

    Returns:
        Net Present Value of cost columns
    """

    value_share = (1 + rate) ** np.arange(0, len(df))
    if cols is None:
        cols = df.columns
    return df[cols].div(value_share, axis=0).sum()
