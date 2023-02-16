import logging
from functools import reduce

import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def calculate_emissions_scope_1(
    df_inputs: pd.DataFrame,
    df_emission_factors: pd.DataFrame,
    df_ccs_rate: pd.DataFrame,
    df_emissions_shares: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculate scope 1 emissions: emissions from the inputs used such as gas and coal

    Args:
        df_inputs: Inputs per process
        df_emission_factors: Emission factors
        df_ccs_rate: CCS data
        df_emissions_shares: emissions share per technology and year

    Returns:
        Dataframe with scope 1 emissions
    """

    # Keep only scope 1
    df_emission_factors = df_emission_factors[df_emission_factors.scope == "1"]
    df_emissions_shares['category'] = 'Raw material'

    # Calc pre CCU scope 1 emissions
    df_emissions = (
        df_inputs.groupby(
            ["chemical", "technology", "category", "name", "year", "region"],
            as_index=False,
        )
        .sum()
        .merge(df_emission_factors, on=["name", "year", "region"], how="left")
        .merge(
            df_emissions_shares,
            on=["chemical", "technology", "name", "year", "category"],
            how="left",
        )
    )
    #remove all non scope 1 related things (e.g. electricity)
    df_emissions=df_emissions[df_emissions.scope=="1"]

    # Default emissions share is 1
    df_emissions.emissions_share.fillna(1, inplace=True)
    # Default emissions factor is 0
    df_emissions.emission_factor.fillna(0, inplace=True)

    # Emissions in t CO2 / t_chemical pre CCUS
    # Emissions share describes how much of ingoing raw material turns into CO2 (Carbon that ends up in
    # molecule disregarded). Default 1 (e.g. ammonia). for cracker based cases 0 based on master template input.
    # OFF-gases do not have emissions share (i.e. 1) as they capture emissions of steam cracker an refineries. Naphtha emission factor 0.
    df_emissions["scope_1_pre_ccus"] = (
        df_emissions["input"]
        * df_emissions["emission_factor"]
        * df_emissions["emissions_share"]
    )
    #df_emissions.loc[df_emissions["category"] != "Energy", ["scope_1_pre_ccus"]] = df_emissions.loc[df_emissions["category"] != "Energy", ["scope_1_pre_ccus"]].values * df_emissions.loc[df_emissions["category"] != "Energy", ["emissions_share"]].values

    # Aggregate
    df_emissions = (
        df_emissions.groupby(
            ["chemical", "technology", "year", "region"], as_index=False
        )
        .agg({"scope_1_pre_ccus": "sum"})
        .fillna(0)
    )

    # Add CCS data and calculate final scope 1 emissions
    df_emissions = df_emissions.merge(
        df_ccs_rate, on=["chemical", "technology", "year"], how="left"
    ).set_index(["chemical", "technology", "year", "region"])

    df_emissions["scope_1"] = df_emissions["scope_1_pre_ccus"] * df_emissions[
        "emissions_rate"
    ].fillna(1)

    # Calculate CCS capacity
    df_emissions["ccs_capacity"] = df_emissions["scope_1_pre_ccus"] * df_emissions[
        "ccs_rate"
    ].fillna(0)
    return df_emissions[["scope_1", "ccs_capacity"]]


def calculate_emissions_scope_2(
    df_inputs: pd.DataFrame, df_emission_factors: pd.DataFrame
) -> pd.DataFrame:
    """
    Calculate scope 2 emissions: indirect emissions of purchased electricity
    Args:
        df_inputs: Inputs per process
        df_emission_factors: Emission factors

    Returns:
        Dataframe with scope 2 emissions
    """

    # Keep only scope 2
    df_emission_factors = df_emission_factors[df_emission_factors.scope == "2"]

    df_emissions = df_inputs.merge(
        df_emission_factors,
        on=["name", "year", "region"],
        how="outer",
    )

    df_emissions["scope_2"] = df_emissions["input"] * df_emissions[
        "emission_factor"
    ].fillna(0)

    return df_emissions.groupby(["chemical", "technology", "year", "region"]).agg(
        {"scope_2": "sum"}
    )


def calculate_emissions_scope_3(
    df_inputs: pd.DataFrame,
    df_emission_factors: pd.DataFrame,
    df_emissions_shares: pd.DataFrame,
    stream_type: str,
) -> pd.DataFrame:
    """

    Args:
        upstream: Scope 3 upstream emission
        df_inputs: Inputs per process
        df_emission_factors: Emission factors

    Returns:
        Dataframe with scope 3 emissions
    """

    # Keep only scope 3
    df_emission_factors = df_emission_factors[df_emission_factors.scope == stream_type]

    if stream_type == "3_downstream":
        df_emission_factors = df_emission_factors.rename(columns={"name": "chemical"})
        df_emissions = df_inputs.merge(
            df_emission_factors, on=["chemical", "year", "region"], how="outer"
        )
        df_emissions[f"scope_{stream_type}"] = df_emissions.input * df_emissions.emission_factor.fillna(0)
    else:
        df_emissions = df_inputs.merge(
            df_emission_factors,
            on=["name", "year", "region"],
            how="right"
        )
        df_emissions_shares['category'] = 'Raw material'
        df_emissions = df_emissions.merge(
            df_emissions_shares,
            on=["chemical", "technology", "name", "year", "category"],
            how="left",
        )
        df_emissions.emission_factor = df_emissions.emission_factor.fillna(0)
        df_emissions.emissions_share = df_emissions.emissions_share.fillna(1)
        # by default, all raw material or energy turn into CO2. most raw material for carbon based molecules set via
        # emissions share, but for ammonia, ammonium nitrate and urea done here.
        # need to add that emissions share should never be applied to energy, only raw material.
        df_emissions[f"scope_{stream_type}"] = df_emissions.apply(calc_scope_3_upstream, axis=1)

    return df_emissions.groupby(["chemical", "technology", "year", "region"]).agg(
        {f"scope_{stream_type}": "sum"}
    )


def combine_emissions_data(dfs: list) -> pd.DataFrame:
    """
    Combine emissions data into one dataframe, adding total emissions
    Args:
        dfs: List of dataframes with emissions data

    Returns:
        Dataframe with scope 1,2,3 and total emissions
    """

    # Outer join to keep all processes and years
    df_emissions = reduce(
        lambda left, right: pd.merge(
            left, right, on=["chemical", "technology", "year", "region"], how="outer"
        ),
        dfs,
    )

    df_emissions = df_emissions.fillna(0)

    # "emissions_scope_1_2_3_upstream_only
    df_emissions["scope_1_2"] = df_emissions["scope_1"] + df_emissions[
        "scope_2"
    ]

    df_emissions["scope_1_2_3_upstream"] = (
        df_emissions["scope_1"]
        + df_emissions["scope_2"]
        + df_emissions["scope_3_upstream"]
    )

    df_emissions["total"] = (
        df_emissions["scope_1"]
        + df_emissions["scope_2"]
        + df_emissions["scope_3_upstream"]
        + df_emissions["scope_3_downstream"]
    )

    return df_emissions


def calculate_emissions_aggregate(
    df_inputs: pd.DataFrame,
    df_emission_factors: pd.DataFrame,
    df_ccs_rate: pd.DataFrame,
    df_emissions_shares: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculate the scope 1,2,3 emissions per process/year/region
    Args:
        df_inputs: Inputs per process
        df_emission_factors: Emissions factors per input
        df_ccs_rate: CCS rate per process and year
        df_emissions_shares: emissions share per technology and year

    Returns:
        Dataframe with emissions
    """

    df_scope_1 = calculate_emissions_scope_1(
        df_inputs=df_inputs,
        df_emission_factors=df_emission_factors,
        df_ccs_rate=df_ccs_rate,
        df_emissions_shares=df_emissions_shares,
    )

    df_scope_2 = calculate_emissions_scope_2(
        df_inputs=df_inputs,
        df_emission_factors=df_emission_factors,
    )

    df_scope_3_upstream = calculate_emissions_scope_3(
        df_inputs=df_inputs,
        df_emission_factors=df_emission_factors,
        df_emissions_shares=df_emissions_shares,
        stream_type="3_upstream",
    )

    df_scope_3_downstream = calculate_emissions_scope_3(
        df_inputs=df_inputs,
        df_emission_factors=df_emission_factors,
        df_emissions_shares=df_emissions_shares,
        stream_type="3_downstream",
    )

    return combine_emissions_data(
        [df_scope_1, df_scope_2, df_scope_3_upstream, df_scope_3_downstream]
    )


def calc_scope_3_upstream(df_emissions):
    """
    # raw material emissions from bio: all incorporated carbon (1-emission share) should get
    # negative scope 3 upstream emissions, no CO2 emissions beyond that. emissions share default 1 for ammonia,
    # ammonium nitrate and urea
    """

    if df_emissions.emission_factor < 0 and df_emissions.category != "Energy":
        emissions = df_emissions.input * (1 - df_emissions.emissions_share) * df_emissions.emission_factor
    #raw material emissions from fossil: all get fugative emissions
    elif df_emissions.emission_factor >= 0 and df_emissions.category != "Energy":
        emissions = df_emissions.input * df_emissions.emission_factor
    #For energy related emissions that come from bio - no emissions
    elif df_emissions.emission_factor < 0 and df_emissions.category == "Energy":
        emissions = 0
    #For energy related emissions that come from fossil - fugative emissions associated with all energy
    elif df_emissions.emission_factor >= 0 and df_emissions.category == "Energy":
        emissions = df_emissions.input * df_emissions.emission_factor
    #Everything else, associate with emissions
    else:
        emissions = df_emissions.input * df_emissions.emission_factor

    return emissions

