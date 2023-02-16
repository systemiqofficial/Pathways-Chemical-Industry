import logging

from config import INITIAL_TECH_ALLOWED_UNTIL_YEAR, NO_FOSSIL_FROM_YEAR, MODEL_SCOPE
from flow.optimize.constraints import (apply_constraints, apply_regional_cap,
                                       apply_tech_ramp_rate,
                                       filter_available_tech,
                                       remove_initial_tech)
from flow.optimize.util import filter_out_fossil, keep_only_initial_tech
from flow.rank.util import select_best_transition
from models.decarbonization import DecarbonizationPathway
from models.plant import make_new_plant

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


def build_new(pathway: DecarbonizationPathway, year: int, chemical: str):
    """
    Build new plants to follow demand growth of a chemical

    Args:
        pathway: The decarbonization pathway
        year: Year to build plants for

    Returns:
        The updated pathway
    """
    df_rank = pathway.get_ranking(year=year, chemical=chemical, rank_type="new_build")

    # Get the tech available now
    df_tech = pathway.tech

    if pathway.pathway_name != "bau" and year > INITIAL_TECH_ALLOWED_UNTIL_YEAR:
        df_rank = remove_initial_tech(df_rank=df_rank, df_tech=df_tech)

    df_rank = filter_available_tech(
        df_tech=df_tech, df_rank=df_rank, year=year, chemical=chemical
    )

    # Strictly no fossil: only allow initial until 2025, then only non-fossil tech
    if pathway.pathway_name == "nfs":
        if year >= 2025:
            df_rank = filter_out_fossil(df_rank=df_rank, df_tech=df_tech)
        else:
            df_rank = keep_only_initial_tech(df_rank=df_rank, df_tech=df_tech)

    # No new fossil after cutoff year
    elif pathway.pathway_name == "nf" and year >= NO_FOSSIL_FROM_YEAR:
        df_rank = filter_out_fossil(df_rank=df_rank, df_tech=df_tech)

    # Get the old/new year's stack
    old_stack = pathway.get_stack(year=year)
    new_stack = pathway.get_stack(year=year + 1)

    # Get process data
    df_process_data = pathway.get_all_process_data(chemical=chemical, year=year)

    # Determine volume gap
    yearly_volume = new_stack.get_yearly_volume(chemical=chemical)
    demand = pathway.get_demand(year=year, chemical=chemical, build_new=True)
    gap = demand - yearly_volume

    # Build new if we don't cover demand
    while gap > 0:

        df_valid = apply_constraints(
            df_rank=df_rank,
            df_process_data=df_process_data,
            pathway=pathway,
            chemical=chemical,
            year=year,
            filter_naphtha_na=True,
        )

        if MODEL_SCOPE == "World":
            df_valid = apply_regional_cap(stack=new_stack, df_rank=df_valid)

        df_valid = apply_tech_ramp_rate(
            old_stack=old_stack,
            new_stack=new_stack,
            df_rank=df_valid,
            chemical=chemical,
        )

        # Only keep tech that this chemical is the primary chemical of
        df_valid = pathway.filter_tech_primary_chemical(
            df_tech=df_valid, chemical=chemical, col="destination"
        )

        if df_valid.empty:
            logger.info("No more new builds available for %s", year)
            break

        best_transition = select_best_transition(df_rank=df_valid)

        new_plant = make_new_plant(
            best_transition=best_transition,
            df_process_data=df_process_data,
            year=year,
            retrofit=False,
            chemical=chemical,
            df_plant_capacities=pathway.df_plant_capacities,
        )

        new_stack.append(new_plant)
        pathway.transitions.add(
            transition_type="new_build", year=year, destination=new_plant
        )
        gap -= new_plant.get_yearly_volume(chemical=chemical)
        pathway = pathway.update_availability(plant=new_plant, year=year)

    return pathway.update_stack(year=year + 1, stack=new_stack)
