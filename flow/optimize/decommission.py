import logging

import pandas as pd

from config import AGE_DEPENDENCY, MODEL_SCOPE, PLANT_SPEC_OVERRIDE
from models.decarbonization import DecarbonizationPathway
from models.plant import PlantStack

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


def get_plant_capacity_mt():
    return PLANT_SPEC_OVERRIDE["assumed_plant_capacity"] * 365 / 1e6


def select_plant_to_decommission(
    stack: PlantStack, df_rank: pd.DataFrame, df_tech: pd.DataFrame, chemical: str
):
    """
    Select plant to decommission, based on cost or emissions

    Args:
        stack:
        df_rank:
        df_tech:

    Returns:

    """

    # Keep only plants that exist
    df_old_tech = stack.get_unique_tech(chemical=chemical)
    df_rank = df_rank.merge(
        df_old_tech,
        left_on=["technology", "region"],
        right_on=["technology", "region"],
    )

    if df_rank.empty:
        raise ValueError("No more plants to decommission!")

    decommission_spec = (
        df_rank[df_rank["rank"] == df_rank["rank"].min()]
        .sample(n=1)
        .to_dict(orient="records")
    )[0]

    # Keep only relevant columns
    decommission_spec = {
        k: v
        for k, v in decommission_spec.items()
        if k in ["technology", "region", "chemical"]
    }

    remove_plants = stack.filter_plants(**decommission_spec)
    remove_plants.sort(key=lambda plant: plant.start_year, reverse=False)

    return remove_plants[0]


def decommission(pathway: DecarbonizationPathway, year: int, chemical: str):
    """
    Decommission plants: close old tech down

    Args:
        chemical: Run for this chemical
        pathway: The decarbonization pathway
        year: Run for this year

    Returns:
        Updated pathway
    """

    df_rank = pathway.get_ranking(
        year=year, chemical=chemical, rank_type="decommission"
    )

    # Get next year's stack
    stack = pathway.get_stack(year=year + 1)

    # Get the tech available now
    df_tech = pathway.tech

    # Only keep tech that this chemical is the primary chemical of
    df_rank = pathway.filter_tech_primary_chemical(
        df_tech=df_rank, chemical=chemical, col="destination"
    )
    df_rank.drop(columns="technology", inplace=True)
    df_rank.rename(columns={"technology":"origin", "destination":"technology"}, inplace=True)

    if (pathway.pathway_name != "bau") and (
        year >= pathway.get_year_earliest_force_decommission()
    ):
        stack = decommission_old_tech(
            pathway=pathway,
            chemical=chemical,
            year=year,
            df_rank=df_rank,
            df_tech=df_tech,
            stack=stack,
        )

    # In the last year, remove all remaining fossil
    if year == (pathway.end_year - 1) and pathway.pathway_name != "bau":
        fossil_plants = stack.get_fossil_plants(chemical=chemical)
        for fossil_plant in fossil_plants:
            stack.remove(fossil_plant)
            pathway.transitions.add(
                transition_type="decommission", year=year, origin=fossil_plant
            )

    # Determine how many plants to decommission
    yearly_volume = stack.get_yearly_volume(chemical=chemical)
    demand = pathway.get_demand(year=year, chemical=chemical)
    surplus = yearly_volume - demand

    # Decommission to follow demand decrease; only decommission if we have > 1 plant capacity surplus
    while surplus > get_plant_capacity_mt():
        try:
            remove_plant = select_plant_to_decommission(
                stack=stack.get_old_plant_stack()
                if (chemical in AGE_DEPENDENCY and MODEL_SCOPE == "World")
                else stack,
                df_rank=df_rank,
                df_tech=df_tech,
                chemical=chemical,
            )

        except ValueError:
            logger.info("No more plants to decommission")
            break

        logger.info("Removing plant with technology %s", remove_plant.technology)

        # Remove the old
        stack.remove(remove_plant)

        surplus -= remove_plant.get_yearly_volume(chemical=chemical)
        pathway.transitions.add(
            transition_type="decommission", year=year, origin=remove_plant
        )

    return pathway.update_stack(year=year + 1, stack=stack)


def decommission_old_tech(pathway, chemical, year, df_rank, df_tech, stack):
    """Additionally, decommission to get rid of old tech"""
    for technology in pathway.decommission_rates.technology.unique():
        if technology in df_rank.technology.values:
            decommission_rate = pathway.get_decommission_rate(
                technology=technology, year=year
            )
            total_volume = sum(
                plant.get_yearly_volume(chemical=chemical)
                for plant in stack.filter_plants(technology=technology)
            )

            decommission_volume = total_volume * decommission_rate

            while decommission_volume > 0 or total_volume <= get_plant_capacity_mt():
                try:
                    remove_plant = select_plant_to_decommission(
                        stack=stack.get_tech_plant_stack(technology=technology),
                        df_rank=df_rank,
                        df_tech=df_tech,
                        chemical=chemical,
                    )
                except ValueError:
                    logger.info("No more plants to decommission")
                    break

                logger.info(
                    "Removing plant with technology %s", remove_plant.technology
                )

                # Remove the old
                stack.remove(remove_plant)
                pathway.transitions.add(
                    transition_type="decommission", year=year, origin=remove_plant
                )

                decommission_volume -= remove_plant.get_yearly_volume(chemical=chemical)

    return stack
