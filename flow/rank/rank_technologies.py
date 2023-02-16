import logging

import numpy as np
import pandas as pd

from config import (INITIAL_TECH_ALLOWED_UNTIL_YEAR, LOG_LEVEL,
                    NUMBER_OF_BINS_RANKING)
from flow.import_data.intermediate_data import IntermediateDataImporter
from util.util import flatten_columns

logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)


def get_rank_config(rank_type: str, pathway: str):
    """
    Configuration to use for ranking

    For each rank type (new_build, retrofit, decommission), and each scenario,
    the dict items represent what to rank on, and in which order.

    For example:

    "new_build": {
        "me": {
            "type_of_tech_destination": "max",
            "lcox": "min",
            "emissions_scope_1_2_delta": "min",
            "emissions_scope_3_upstream_delta": "min",
        }

    indicates that for the new_build rank, in the most_economic scenario, we favor building:
    1. Higher tech type (i.e. more advanced tech)
    2. Lower levelized cost of chemical
    3. Lower scope 1/2 emissions
    4. Lower scope 3 emissions

    in that order!
    """

    config = {
        "new_build": {
            # NOTE: Emissions "min": Abated tech has some emissions X, Delta= Destination - Origin (non-existent greenfield)
            # Delta is positive and should be minimal (=minimum emissions of new plant)
            "me": {
                "type_of_tech_destination": "max",
                "lcox": "min",
                "emissions_scope_1_2_delta": "min",
                "emissions_scope_3_upstream_delta": "min",
            },
            "fa": {
                "type_of_tech_destination": "max",
                "emissions_scope_1_2_delta": "min",
                "emissions_scope_3_upstream_delta": "min",
                "lcox": "min",
            },
            "nf": {
                "type_of_tech_destination": "max",
                "emissions_scope_1_2_3_upstream_delta": "min",
                "lcox": "min",
            },
            "nfs": {
                "type_of_tech_destination": "max",
                "emissions_scope_1_2_3_upstream_delta": "min",
                "lcox": "min",
            },
            "bau": {
                "lcox": "min",
                "emissions_scope_1_2_delta": "min",
                "emissions_scope_3_upstream_delta": "min",
            },
        },
        "retrofit": {
        # NOTE: Emissions "max": Destination tech has some emissions X, origin tech has emissions Y, Y>X, Delta= Destination X - Origin Y
        # The absolute value is taken and should be maximum
            "me": {
                "type_of_tech_origin": "min",
                "type_of_tech_destination": "max",
                "lcox": "min",
                "emissions_scope_1_2_delta": "max",
                "emissions_scope_3_upstream_delta": "max",
            },
            "fa": {
                "type_of_tech_origin": "min",
                "type_of_tech_destination": "max",
                "emissions_scope_1_2_delta": "max",
                "emissions_scope_3_upstream_delta": "max",
                "lcox": "min",
            },
            "nf": {
                "type_of_tech_origin": "min",
                "type_of_tech_destination": "max",
                "emissions_scope_1_2_3_upstream_delta": "max",
                "lcox": "min",
            },
            "nfs": {
                "type_of_tech_origin": "min",
                "type_of_tech_destination": "max",
                "emissions_scope_1_2_3_upstream_delta": "max",
                "lcox": "min",
            },
            "bau": {
                "lcox": "min",
                "emissions_scope_1_2_delta": "max",
                "emissions_scope_3_upstream_delta": "max",
            },
        },
        "decommission": {
            # NOTE: Most expensive or highest emission tech should be retired first
            # Destination = the tech that will be retired. Origin = non-existent
            # Destination - 0 --> max for emissions sorting.
            "me": {
                "type_of_tech_destination": "min",
                "lcox": "max",
                "emissions_scope_1_2_delta": "max",
                "emissions_scope_3_upstream_delta": "max",
            },
            "fa": {
                "type_of_tech_destination": "min",
                "emissions_scope_1_2_delta": "max",
                "emissions_scope_3_upstream_delta": "max",
                "lcox": "max",
            },
            "nf": {
                "type_of_tech_destination": "min",
                "emissions_scope_1_2_3_upstream_delta": "max",
                "lcox": "max",
            },
            "nfs": {
                "type_of_tech_destination": "min",
                "emissions_scope_1_2_3_upstream_delta": "max",
                "lcox": "max",
            },
            "bau": {
                "lcox": "max",
                "emissions_scope_1_2_delta": "max",
                "emissions_scope_3_upstream_delta": "max",
            },
        },
    }

    return config[rank_type][pathway]


def bin_ranking(rank_array: np.array, n_bins: int = NUMBER_OF_BINS_RANKING) -> np.array:
    """
    Bin the ranking, i.e. values that are close together end up in the same bin

    Args:
        rank_array: The array with values we want to rank
        n_bins: the number of bins we want to

    Returns:
        array with binned values
    """
    _, bins = np.histogram(rank_array, bins=n_bins)
    return np.digitize(rank_array, bins=bins)


def _add_binned_rankings(
    df_rank: pd.DataFrame, n_bins: int = NUMBER_OF_BINS_RANKING
) -> pd.DataFrame:
    """Add binned values for the possible ranking columns"""
    for rank_var in [
        "emissions_scope_1_2_delta",
        "emissions_scope_3_upstream_delta",
        "emissions_scope_1_2_3_upstream_delta",
        "lcox",
    ]:
        df_rank[rank_var + "_binned"] = bin_ranking(df_rank[rank_var], n_bins=n_bins)

    return df_rank


def rank_per_year(
    df_rank: pd.DataFrame,
    rank_type: str,
    pathway: str,
    initial_tech_allowed_until_year: int,
    year: int = None,
) -> pd.DataFrame:
    """
    Rank technologies in df according to rank_var. If close to each other based on rank_var

    Args:
        df_rank: Dataframe with technologies for ranking
        rank_type: 'decommission', 'new_build' or 'decommission'
        pathway: pathway name
        year: for this year (else infer this from the dataframe)
        initial_tech_allowed_until_year: self explanatory

    Returns:
        Dataframe with ranked technologies for this year
    """
    if year is None:
        year = df_rank.year.values[0]

    config = get_rank_config(rank_type=rank_type, pathway=pathway)

    # For new build, for the first years don't rank on type of tech
    if (year <= initial_tech_allowed_until_year) and (rank_type == "new_build"):
        config.pop("type_of_tech_destination", None)

    # We rank on the binned versions of each variable (except type_of_tech)
    vars = []
    for var in config.keys():
        if "type_of_tech" not in var:
            var += "_binned"
        vars.append(var)

    # Get things in the right order, according to the ranking config:
    #  If minimum -> rank ascending (low cost = low rank = good)
    #  If maximum -> rank descending (low cost = high rank = bad)
    ascending = [l == "min" for l in list(config.values())]

    df_rank = df_rank.groupby(["chemical", "year"]).apply(_add_binned_rankings)

    df_rank = df_rank.sort_values(vars, ascending=ascending).copy()

    # Concatenate variables to be able to rank
    df_rank["vars_concat"] = df_rank.apply(
        lambda row: "".join([str(row[v]) for v in vars]), axis="columns"
    )

    # Make rank based on concatenated variables
    df_rank["rank"] = pd.factorize(df_rank.vars_concat)[0] + 1

    return df_rank


def _add_cost_data(rank_type, df_cost, df_rank):
    """Add CAPEX and LCOX data"""
    if rank_type in ["decommission", "new_build"]:
        df_lcox = df_cost[
            [
                ("lcox", "new_build_brownfield"),
                ("economics", "capex_new_build_brownfield"),
            ]
        ]
        df_lcox = (
            flatten_columns(df_lcox)
            .reset_index()
            .rename(
                columns={
                    "technology": "destination",
                    "lcox_new_build_brownfield": "lcox",
                    "economics_capex_new_build_brownfield": "capex",
                }
            )
        )
        df_rank = df_rank[df_rank.origin == "Non-existent"]
        df_rank = df_rank.merge(
            df_lcox, on=["chemical", "origin", "destination", "year", "region"]
        )
    else:
        df_lcox = flatten_columns(
            df_cost[
                [
                    ("lcox", "new_build_brownfield"),
                    ("lcox", "retrofit"),
                    ("economics", "capex_new_build_brownfield"),
                    ("economics", "capex_retrofit"),
                ]
            ]
        ).reset_index()

        df_lcox_retrofit = df_lcox.rename(
            columns={
                "technology": "destination",
                "lcox_retrofit": "lcox",
                "economics_capex_retrofit": "capex",
            }
        ).drop(
            columns=[
                "lcox_new_build_brownfield",
                "economics_capex_new_build_brownfield",
            ]
        )

        df_lcox_decommission_new_build = df_lcox.rename(
            columns={
                "technology": "destination",
                "lcox_new_build_brownfield": "lcox",
                "economics_capex_new_build_brownfield": "capex",
            }
        ).drop(columns=["lcox_retrofit", "economics_capex_retrofit"])

        df_rank_retrofit = df_rank[df_rank.retrofit_type == "normal"].merge(
            df_lcox_retrofit, on=["chemical", "origin", "destination", "year", "region"]
        )
        # Create origin list for normal retrofit
        decommission_new_build_tech = (
            df_lcox_decommission_new_build.origin.unique().tolist()
        )
        decommission_new_build_tech.remove("Non-existent")

        df_lcox_decommission_new_build = df_lcox_decommission_new_build[
            ~df_lcox_decommission_new_build.origin.isin(decommission_new_build_tech)
        ]
        df_lcox_decommission_new_build = df_rank[
            df_rank.retrofit_type == "decommission_new_build"
        ].merge(
            df_lcox_decommission_new_build,
            on=["chemical", "destination", "year", "region"],
        )
        df_lcox_decommission_new_build.drop(columns="origin_y", inplace=True)
        df_lcox_decommission_new_build.rename(
            columns={"origin_x": "origin"}, inplace=True
        )
        df_rank_decommission_newbuild = df_lcox_decommission_new_build[
            df_lcox_decommission_new_build.origin
            != df_lcox_decommission_new_build.destination
        ]
        df_rank = pd.concat([df_rank_retrofit, df_rank_decommission_newbuild])
    return df_rank


def _add_emissions_data(df_emissions, df_tech_transitions, rank_type):
    """Add emissions from origin and destination tech"""

    def _get_emissions_df(df_emissions: pd.DataFrame, endpoint: str):
        return df_emissions.reset_index()[
            [
                "chemical",
                "region",
                "technology",
                "year",
                "scope_1",
                "scope_1_2",
                "scope_3_upstream",
                "scope_1_2_3_upstream",
            ]
        ].rename(
            columns={
                "technology": endpoint,
                "scope_1": f"emissions_scope_1_{endpoint}",
                "scope_1_2": f"emissions_scope_1_2_{endpoint}",
                "scope_3_upstream": f"emissions_scope_3_upstream_{endpoint}",
                "scope_1_2_3_upstream": f"emissions_scope_1_2_3_upstream_{endpoint}",
            }
        )

    # Add emissions from destination
    df_emissions_destination = _get_emissions_df(df_emissions, endpoint="destination")
    df_rank = df_tech_transitions.merge(
        df_emissions_destination, on=["chemical", "destination"]
    )

    # Add emissions from origin
    if rank_type == "retrofit":
        df_emissions_origin = _get_emissions_df(df_emissions, endpoint="origin")
        df_rank = df_rank.merge(
            df_emissions_origin, on=["chemical", "origin", "region", "year"]
        )

    for scope in ["1_2", "3_upstream", "1_2_3_upstream"]:
        if rank_type == "retrofit":
            df_rank[f"emissions_scope_{scope}_delta"] = (
                df_rank[f"emissions_scope_{scope}_destination"]
                - df_rank[f"emissions_scope_{scope}_origin"]
            )
        else:
            # Retrofit and new build don't have an origin, just a destination tech
            df_rank[f"emissions_scope_{scope}_delta"] = df_rank[
                f"emissions_scope_{scope}_destination"
            ]
    return df_rank


def _filter_tech_transitions(df_tech_transitions, rank_type):
    transition_filter = (
        "CAPEX - retrofit"
        if rank_type == "retrofit"
        else "CAPEX - new build brownfield"
    )
    # Only keep transitions that we need. For decommission, keep new build capex, as decommission cost is 5% of that
    df_tech_transitions = (
        df_tech_transitions[df_tech_transitions.name == transition_filter]
        .drop(columns=["component", "category", "name"])
        .drop_duplicates(["chemical", "origin", "destination"])
    )
    return df_tech_transitions


def _add_decommission_new_build_options(df_tech, df_tech_transitions):
    """Add decommission plus new build in the same location as a retrofit option"""
    df_origin = df_tech[["chemical", "technology"]].rename(
        columns={"technology": "origin"}
    )
    df_destination = df_tech[["chemical", "technology"]].rename(
        columns={"technology": "destination"}
    )

    df_decommission_new_build = df_origin.merge(df_destination, on="chemical")
    df_decommission_new_build["retrofit_type"] = "decommission_new_build"
    df_tech_transitions["retrofit_type"] = "normal"

    return pd.concat([df_tech_transitions, df_decommission_new_build])


def rank_tech(
    df_tech_transitions: pd.DataFrame,
    df_tech: pd.DataFrame,
    df_cost: pd.DataFrame,
    df_emissions: pd.DataFrame,
    rank_type: str,
    pathway: str,
    year: int = None,
) -> pd.DataFrame:
    """
    Rank technologies for every year

    Args:
        df_tech: Dataframe with tech state
        df_emissions: Dataframe with emissions
        df_tech_transitions: Dataframe with allowed tech transitions
        df_cost: Dataframe with TCO and emissions per technology
        rank_type: type of ranking; can be new_build, decommission or retrofit
        pathway: pathway name
        year: optional, rank only for this year
    Returns:
        Dataframe with ranking
    """
    logger.info(f"Making ranking for {rank_type}")

    df_tech_transitions = _filter_tech_transitions(df_tech_transitions, rank_type)

    if rank_type == "retrofit":
        df_tech_transitions = _add_decommission_new_build_options(
            df_tech=df_tech, df_tech_transitions=df_tech_transitions
        )

    df_tech_transitions = _add_tech_types(
        df_tech_transitions=df_tech_transitions, df_tech=df_tech, rank_type=rank_type
    )

    df_rank = _add_emissions_data(
        df_emissions=df_emissions,
        df_tech_transitions=df_tech_transitions,
        rank_type=rank_type,
    )

    df_rank = _add_cost_data(rank_type=rank_type, df_cost=df_cost, df_rank=df_rank)

    # Do the actual ranking per year!
    if year is not None:
        # Only one year (when re-ranking)
        df_rank = rank_per_year(
            df_rank=df_rank.query(f"year == {year}"),
            rank_type=rank_type,
            pathway=pathway,
            initial_tech_allowed_until_year=INITIAL_TECH_ALLOWED_UNTIL_YEAR,
        )
    else:
        # All years (for initial ranking)
        df_rank = df_rank.groupby(
            ["year", "chemical"], as_index=False, group_keys=True
        ).apply(
            rank_per_year,
            rank_type=rank_type,
            pathway=pathway,
            initial_tech_allowed_until_year=INITIAL_TECH_ALLOWED_UNTIL_YEAR,
        )

    return df_rank.set_index(["chemical", "origin", "destination", "region", "year"])


def _add_tech_types(
    df_tech_transitions: pd.DataFrame, df_tech: pd.DataFrame, rank_type: str
) -> pd.DataFrame:
    """Add the tech types (origin and destination) to the tech transitions data"""
    df_tech.type_of_tech = df_tech.type_of_tech.replace(
        {"Initial": 1, "Transition": 2, "End-state": 3}
    )

    def _get_df_tech(endpoint):
        return df_tech[["chemical", "technology", "type_of_tech"]].rename(
            columns={"technology": endpoint, "type_of_tech": f"type_of_tech_{endpoint}"}
        )

    df_tech_transitions = df_tech_transitions.merge(
        _get_df_tech("destination"), on=["chemical", "destination"]
    )

    if rank_type == "retrofit":
        df_tech_transitions = df_tech_transitions.merge(
            _get_df_tech("origin"), on=["chemical", "origin"]
        )

    return df_tech_transitions


def make_rankings(pathway, sensitivity, chemicals, model_scope):
    """
    Make rankings for new builds, retrofits and decommission.

    Uses the calculated variables from `calulate_variables`, data on allowed tech transitions,
    and the pathway configuration and tech_asc to specify the rankings.

    """
    importer = IntermediateDataImporter(
        pathway=pathway,
        sensitivity=sensitivity,
        chemicals=chemicals,
        model_scope=model_scope,
    )

    df_tech_transitions = importer.get_tech_transitions()
    df_cost = importer.get_process_data(data_type="cost")
    df_emissions = importer.get_process_data(data_type="emissions")
    df_tech = importer.get_tech()

    for rank_type in ["retrofit", "new_build", "decommission"]:
        df_rank = rank_tech(
            df_tech_transitions=df_tech_transitions,
            df_tech=df_tech,
            df_cost=df_cost,
            df_emissions=df_emissions,
            rank_type=rank_type,
            pathway=pathway,
        )

        # Export for each chemical
        for chemical in chemicals:
            df_chemical = df_rank.query(f"chemical == '{chemical}'")
            importer.export_data(
                df=df_chemical,
                filename=f"{rank_type}_rank.csv",
                export_dir=f"ranking/{chemical}",
            )
