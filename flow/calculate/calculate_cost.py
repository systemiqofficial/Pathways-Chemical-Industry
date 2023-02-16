import logging

import pandas as pd

from config import (CARBON_PRICE, CARBON_PRICE_ADJUSTMENT,
                    CCS_PRICE_ADJUSTMENT, POWER_PRICE_ADJUSTMENT)
from flow.calculate.pivot_inputs import pivot_inputs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def calculate_carbon_cost(
    df_ccs_price: pd.DataFrame,
    df_carbon_price: pd.DataFrame,
    df_emissions: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculate the total cost of carbon
    Args:
        df_ccs_price: CCS prices
        df_carbon_price: Carbon tax
        df_emissions: Emissions per process

    Returns:
        Carbon cost per process
    """

    df_cost = df_emissions.join(df_carbon_price, on="year", how="right").join(
        df_ccs_price, on=["year", "region"]
    )

    if CARBON_PRICE:
        df_cost["carbon"] = (
            df_cost["scope_1"] * df_cost["carbon_price"] * CARBON_PRICE_ADJUSTMENT
        )
    else:
        df_cost["carbon"] = 0.0

    df_cost["ccs"] = (
        df_cost["ccs_capacity"] * df_cost["ccs_price"] * CCS_PRICE_ADJUSTMENT
    )

    return df_cost[["carbon", "ccs"]]


def calculate_input_cost(
    df_inputs: pd.DataFrame, df_input_prices: pd.DataFrame
) -> pd.DataFrame:
    """
    Calculate cost of inputs
    Args:
        df_inputs: inputs per process
        df_input_prices: price of inputs

    Returns:
        cost of inputs per process
    """

    # Calculate cost of inputs
    df = df_inputs.merge(df_input_prices, on=["name", "year", "region"])
    df["cost"] = df["input_price"] * df["input"]

    df.loc[df.name.str.contains("Electricity"), "cost"] *= POWER_PRICE_ADJUSTMENT
    df = pivot_inputs(df=df, values="cost")

    return df


def calculate_cost(
    df_emissions: pd.DataFrame,
    df_inputs: pd.DataFrame,
    df_ccs_price: pd.DataFrame,
    df_carbon_price: pd.DataFrame,
    df_input_price: pd.DataFrame,
    df_economics: pd.DataFrame,
) -> pd.DataFrame:
    """

    Args:
        df_emissions: emissions per process/year/region
        df_inputs: inputs per process/year/region
        df_ccs_price: price of CCS over time/region
        df_carbon_price: price of CCS over time/region
        df_input_price: price of inputs over time/region
        df_economics: process economics

    Returns:
        Cost data per process/region/year
    """
    df_carbon_cost = calculate_carbon_cost(
        df_ccs_price=df_ccs_price,
        df_carbon_price=df_carbon_price,
        df_emissions=df_emissions,
    )
    df_cost = calculate_input_cost(df_inputs=df_inputs, df_input_prices=df_input_price)

    df_cost["other", "ccs"] = df_carbon_cost["ccs"]
    df_cost["other", "carbon"] = df_carbon_cost["carbon"]

    df_cost["other", "variable_opex"] = (
        df_cost["other", "ccs"]
        + df_cost["other", "carbon"]
        + df_cost["Energy", "total"]
        + df_cost["Raw material", "total"]
    )

    df_cost = df_cost.join(pd.concat({"economics": df_economics}, axis=1))

    return df_cost
