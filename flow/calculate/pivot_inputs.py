import logging

import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def sum_energy_columns(df_pivot: pd.DataFrame) -> pd.DataFrame:
    electricity_cols = [
        col for col in df_pivot["Energy"].columns if "Electricity" in col
    ]
    non_electricity_cols = [
        col for col in df_pivot["Energy"].columns if "Electricity" not in col
    ]

    if "Energy" not in df_pivot.columns.levels[0]:
        df_pivot["Energy", "total"] = 0
    else:
        df_pivot["Energy", "electricity"] = df_pivot["Energy"][electricity_cols].sum(
            axis="columns"
        )
        df_pivot["Energy", "non_electricity"] = df_pivot["Energy"][
            non_electricity_cols
        ].sum(axis="columns")

        # Total electricity calculation
        df_pivot["Energy", "total"] = (
            df_pivot["Energy", "electricity"] + df_pivot["Energy", "non_electricity"]
        )

    return df_pivot


def sum_raw_material_columns(df_pivot: pd.DataFrame) -> pd.DataFrame:
    if "Raw material" in df_pivot.columns.levels[0]:
        df_pivot["Raw material", "total"] = df_pivot["Raw material"].sum(axis="columns")
    else:
        df_pivot["Raw material", "total"] = 0

    return df_pivot


def pivot_inputs(df: pd.DataFrame, values: str) -> pd.DataFrame:
    """
    Pivot inputs data to wide format

    Args:
        df: Dataframe with inputs in long format
        values: Name of the columsn with values

    Returns:
        Dataframe with inputs in wide format
    """
    df_pivot = df.pivot_table(
        index=["chemical", "technology", "year", "region"],
        values=values,
        columns=["category", "name"],
        aggfunc="sum",
    ).fillna(0)

    df_pivot = sum_energy_columns(df_pivot)
    df_pivot = sum_raw_material_columns(df_pivot)

    return df_pivot
