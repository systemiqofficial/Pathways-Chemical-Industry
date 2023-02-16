import pandas as pd

from config import METHANOL_DEMAND_TECH, MINIMUM_AGE_DECOMMISSION
from models.plant import PlantStack


def filter_out_fossil(df_rank: pd.DataFrame, df_tech: pd.DataFrame) -> pd.DataFrame:
    df_non_fossil = df_tech.loc[
        ~df_tech.category_detailed.str.contains("Fossil"), ["chemical", "technology"]
    ]
    return df_rank.merge(
        df_non_fossil,
        left_on=["chemical", "destination"],
        right_on=["chemical", "technology"],
        how="inner",
    )


def filter_existing_tech(
    stack: PlantStack, df_rank: pd.DataFrame, chemical: str, feedstock_switch=False
):

    # Keep only tech transitions that have an origin in the current stack
    df_old_tech = stack.get_unique_tech(chemical=chemical)
    df_rank = df_rank.merge(
        df_old_tech, left_on=["origin", "region"], right_on=["technology", "region"]
    )
    # feedstock switch option should be removed as a general retrofitting option
    if not feedstock_switch:
        df_rank = df_rank[
            ~(df_rank.origin.isin(METHANOL_DEMAND_TECH))
            & ~(df_rank.destination.isin(METHANOL_DEMAND_TECH))
        ]

    return df_rank


def keep_only_initial_tech(df_rank: pd.DataFrame, df_tech: pd.DataFrame):
    df_initial = df_tech.loc[
        df_tech.type_of_tech == "Initial", ["chemical", "technology"]
    ]
    return df_rank.merge(
        df_initial,
        left_on=["chemical", "destination"],
        right_on=["chemical", "technology"],
        how="inner",
    )


def remove_new_plants(df_valid: pd.DataFrame, stack: PlantStack, year: int):
    """
    Remove retrofit options that
    - Involve plants that are too new
    - Are decommission + new build
    - Don't remove initial tech
    """

    # Get plant age by chemical/region/tech
    df_plants = pd.DataFrame(
        [
            {
                "chemical": plant.chemical,
                "region": plant.region,
                "origin": plant.technology,
                "age": plant.get_age(year=year),
            }
            for plant in stack.plants
        ]
    )

    df_valid = df_valid.merge(
        df_plants.drop_duplicates(["chemical", "region", "origin"]),
        on=["chemical", "region", "origin"],
    )

    # Remove transitions that are decommission + new build, don't remove initial tech and involve new plants
    invalid_transition_idx = (
        (df_valid.retrofit_type == "decommission_new_build")
        & (df_valid.type_of_tech_origin != "Initial")
        & (df_valid.age < MINIMUM_AGE_DECOMMISSION)
    )

    return df_valid[~invalid_transition_idx]
