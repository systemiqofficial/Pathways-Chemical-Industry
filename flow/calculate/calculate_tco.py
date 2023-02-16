import logging

import pandas as pd

from config import DISCOUNT_RATE, ECONOMIC_LIFETIME_YEARS, END_YEAR, START_YEAR
from flow.calculate.npv import net_present_value

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def subset_cost_df(df_cost: pd.DataFrame, start_year: int, plant_lifetime: int):
    """
    Slice dataframe to get a range of years

    Args:
        df_cost: Process costs
        start_year: Start at this year
        plant_lifetime: For this many years

    Returns:
        The subset dataframe
    """
    return df_cost[
        (df_cost.index >= start_year) & (df_cost.index < (start_year + plant_lifetime))
    ]


def calculate_npv_costs(df_cost: pd.DataFrame, year) -> pd.DataFrame:
    """
    Calculate the Net Present Value (NPV) of costs

    Args:
        year: Current year or none
        df_cost: Process costs
        cols: Columns to discount

    Returns:
        Discounted
    """

    df_cost.index = df_cost.index.droplevel(
        ["chemical", "technology", "origin", "region"]
    )
    if year is None:
        df_cost = df_cost[
            (df_cost.index >= START_YEAR) & (df_cost.index <= END_YEAR)
        ].apply(
            lambda row: net_present_value(
                rate=DISCOUNT_RATE,
                df=subset_cost_df(
                    df_cost=df_cost,
                    start_year=row.name,
                    plant_lifetime=ECONOMIC_LIFETIME_YEARS,
                ),
            ),
            axis=1,
        )
    else:
        df_cost = pd.DataFrame(
            net_present_value(
                subset_cost_df(
                    df_cost=df_cost,
                    start_year=year,
                    plant_lifetime=ECONOMIC_LIFETIME_YEARS,
                ),
                DISCOUNT_RATE,
            )
        ).T
        df_cost["year"] = year
        df_cost = df_cost.set_index("year")
    return df_cost


def discount_costs(df_cost: pd.DataFrame, year) -> pd.DataFrame:
    """
    Discount costs with a fixed discounting rate

    Args:
        df_cost: Process costs

    Returns:
        Discounted costs
    """

    discounting_cols = {
        "operations_and_maintenance": ("economics", "operations_and_maintenance"),
        "energy_electricity": ("Energy", "electricity"),
        "ccs": ("other", "ccs"),
        "carbon": ("other", "carbon"),
        "energy_non_electricity": ("Energy", "non_electricity"),
        "total_volume": ("spec", "total_yearly_volume"),
        "raw_material_total": ("Raw material", "total"),
        "plant_lifetime": ("spec", "plant_lifetime"),
    }

    # Keep only discounting columns
    df = df_cost[list(discounting_cols.values())]

    # Flatten column multi-index to speed things up
    df.columns = discounting_cols.keys()

    # Discount all costs over time
    df_discount = df.groupby(["chemical", "origin", "technology", "region"]).apply(
        calculate_npv_costs, year
    )

    # Calculate var opex and total energy cost
    df_discount["energy_total"] = (
        df_discount["energy_electricity"] + df_discount["energy_non_electricity"]
    )

    df_discount["variable_opex"] = (
        df_discount["raw_material_total"]
        + df_discount["energy_total"]
        + df_discount["ccs"]
        + df_discount["carbon"]
    )

    return df_discount.drop(columns="plant_lifetime")


def calculate_tco(
    df_cost: pd.DataFrame, df_spec: pd.DataFrame, year=None
) -> pd.DataFrame:
    """
    Calculate TCO (total cost of ownership) per technology

    Args:
        df_cost: Costs per technology
        df_spec: Specifications per technology

    Returns:
        TCO and LCOX per technology
    """

    df_cost = df_cost.join(pd.concat({"spec": df_spec}, axis=1))

    df_discount = discount_costs(df_cost, year)

    # Join the data, keep only until 2050 as that is what we need
    df = df_cost.join(pd.concat({"discounted": df_discount}, axis=1)).query(
        f"year <= {END_YEAR}"
    )

    cols = [("discounted", col) for col in df["discounted"].columns] + [
        ("economics", "capex_new_build_brownfield"),
        ("economics", "capex_retrofit"),
    ]

    for grp, col in cols:
        # Calculate Total Cost of Ownership contribution: all the costs (normalized per ton per year),
        # times the total volume per year; convert from Mt to t
        df["tco_contribution", col] = (
            df[grp, col] * df["spec", "total_yearly_volume"] * 1e6
        )

        # Go to levelized cost contribution by dividing by total volume of product produced in the economic lifetime
        df["lcox_contribution", col] = df["tco_contribution", col] / (
            df["discounted", "total_volume"] * 1e6
        )

    # Get TCO/LCOX by summing components
    components = {
        "new_build_brownfield": [
            "capex_new_build_brownfield",
            "variable_opex",
            "operations_and_maintenance",
        ],
        "retrofit": [
            "capex_retrofit",
            "variable_opex",
            "operations_and_maintenance",
        ],
    }
    for key in components:
        for cost in ["tco", "lcox"]:
            df[cost, key] = df[f"{cost}_contribution"][components[key]].sum(axis=1)

    # Keep only what we need
    df = df[
        [
            "Energy",
            "Raw material",
            "other",
            "economics",
            "discounted",
            "tco",
            "lcox",
            "lcox_contribution",
        ]
    ]

    return df
