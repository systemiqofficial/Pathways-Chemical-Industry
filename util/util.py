from functools import wraps
from time import time

import pandas as pd


def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return df with columns flattened from multi-index to normal index for columns
    Args:
        df: input df

    Returns: the df with flattened column index

    """
    df.columns = ["_".join(col).strip() for col in df.columns.values]
    return df


def make_multi_df(df: pd.DataFrame, name: str) -> pd.DataFrame:
    """
    Make a df multi-column-indexed

    Args:
        df: Input df
        name: Name of the multi-index top level

    Returns:
        multi-indexed df
    """
    return pd.concat({name: df}, axis=1)


def first(series: pd.Series):
    """Return first element in a series"""
    return series.values[0]


def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        print("func:%r took: %2.4f sec" % (f.__name__, te - ts))
        return result

    return wrap
