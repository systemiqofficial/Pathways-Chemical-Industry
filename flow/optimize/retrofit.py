import logging

import pandas as pd

from config import (AGE_DEPENDENCY, MODEL_SCOPE, NO_FOSSIL_FROM_YEAR,
                    RETROFIT_CAP, SECOND_RETROFIT_EARLIEST_YEAR)
from flow.optimize.constraints import (apply_constraints, apply_tech_ramp_rate,
                                       filter_available_tech)
from flow.optimize.util import (filter_existing_tech, filter_out_fossil,
                                keep_only_initial_tech, remove_new_plants)
from flow.rank.util import select_best_transition
from models.decarbonization import DecarbonizationPathway
from models.plant import make_new_plant

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


def force_availability_retrofit(
    pathway: DecarbonizationPathway,
    year: int,
    chemical: str,
    new_stack,
    old_stack,
    df_rank: pd.DataFrame,
    df_process_data: pd.DataFrame,
    dict_raw_material: dict,
):
    """
    Retrofit plants that exceed a raw materials cap. This can happen for example
    when a plant is using methanol - black, and black methanol technology disappears
    from the methanol wedge

    Args:
        pathway:
        year:
        chemical:
        new_stack:
        old_stack:
        df_rank:
        df_process_data:
        dict_raw_material:
        list_raw_material:

    Returns:

    """
    for raw_material, value in dict_raw_material.items():

        # create a tuple to subset multi-index dataframe
        if raw_material == "CO2 storage":
            tuple_raw_material = ("emissions", "", "ccs_yearly")
        else:
            tuple_raw_material = ("inputs", "Raw material", raw_material)

        # subset the dataset for origin technologies that use the raw material as input
        retrofit_technology_list = list(
            df_process_data[df_process_data.loc[:, tuple_raw_material] > 0]
            .index.get_level_values("technology")
            .unique()
        )

        df_material_specific_process_data = df_process_data.query(
            f"origin in {retrofit_technology_list}"
        )

        df_material_specific_process_data = (
            df_material_specific_process_data.reset_index()
        )

        # subset data for the technologies that are online in the current stack
        df_material_specific_process_data = df_material_specific_process_data[
            df_material_specific_process_data.origin.isin(
                list(new_stack.get_tech("technology").index)
            )
        ]

        # loop over all technology that are relevant for the raw material
        for technology in df_material_specific_process_data.origin.unique():

            # retrofit for the raw material that is negative
            while value["remaining"] < 0:

                if old_stack.get_tech_plant_stack(technology=technology).empty():
                    logger.info("No more forced retrofits available for %s", year)
                    break

                df_existing = filter_existing_tech(
                    stack=new_stack.get_tech_plant_stack(technology=technology),
                    df_rank=df_rank,
                    chemical=chemical,
                    feedstock_switch=True,
                )

                df_valid = apply_tech_ramp_rate(
                    old_stack=old_stack,
                    new_stack=new_stack,
                    df_rank=df_existing,
                    chemical=chemical,
                )

                # Only keep tech that this chemical is the primary chemical of
                df_valid = pathway.filter_tech_primary_chemical(
                    df_tech=df_valid, chemical=chemical, col="destination"
                )

                # Remove plants that are too new to decommission / build new
                df_valid = remove_new_plants(
                    df_valid=df_valid, stack=old_stack, year=year
                )

                if df_valid.empty:
                    logger.info("No more forced retrofits available for %s", year)
                    break

                best_transition = select_best_transition(df_rank=df_valid)

                new_plant = make_new_plant(
                    best_transition=best_transition,
                    df_process_data=df_process_data,
                    year=year,
                    retrofit=True,
                    chemical=chemical,
                    df_plant_capacities=pathway.df_plant_capacities,
                )

                # When the tech to be removed is not in the plant stack, pass
                remove_plant = new_stack.filter_plants(
                    region=best_transition["region"],
                    technology=best_transition["origin"],
                    chemical=best_transition["chemical"],
                )[0]

                if round(
                    dict_raw_material[raw_material]["used"], 10
                ) * 1e6 < calculate_used_raw_material(remove_plant, raw_material):
                    logger.info(f"Not enough {raw_material} available for %s", year)
                    break

                new_stack.remove(remove_plant)
                old_stack.remove(remove_plant)
                pathway = pathway.update_availability(
                    plant=remove_plant, year=year, remove=True
                )

                new_stack.append(new_plant)
                old_stack.append(new_plant)
                pathway = pathway.update_availability(plant=new_plant, year=year)

                _log_retrofit(best_transition, new_plant, pathway, remove_plant, year)

                value["remaining"] += calculate_used_raw_material(
                    remove_plant, raw_material
                )

                logger.info("Did force availability retrofit")


def calculate_used_raw_material(plant, raw_material):
    if raw_material == "Methanol - Black":
        return plant.methanol_black_yearly
    elif raw_material == "Methanol - Green":
        return plant.methanol_green_yearly
    elif raw_material == "CO2":
        return plant.ccs_total
    elif raw_material == "Biomass":
        return plant.biomass_yearly
    elif raw_material == "Bio-oils":
        return plant.bio_oils_yearly
    elif raw_material == "Pyrolysis oil":
        return plant.pyrolysis_oil_yearly
    elif raw_material == "Waste water":
        return plant.waste_water_yearly
    elif raw_material == "Municipal solid waste RdF":
        return plant.municipal_solid_waste_rdf_yearly


def retrofit(pathway: DecarbonizationPathway, year: int, chemical: str):
    """
    Retrofit plants: replace old tech by clean tech

    Args:
        pathway: The decarbonization pathway
        year: Run for this year

    Returns:
        Updated pathway
    """
    # Get the new year's stack
    old_stack = pathway.get_stack(year=year)
    new_stack = pathway.get_stack(year=year + 1)

    # Determine number of plants to retrofit
    yearly_volume = new_stack.get_yearly_volume(chemical=chemical)
    retrofit_volume = yearly_volume * RETROFIT_CAP

    # Get ranking table
    df_rank = pathway.get_ranking(year=year, chemical=chemical, rank_type="retrofit")

    # Get the tech available now
    df_tech = pathway.tech

    # Only keep tech that this chemical is the primary chemical of
    df_tech = pathway.filter_tech_primary_chemical(df_tech=df_tech, chemical=chemical)

    # Get process data
    df_process_data = pathway.get_all_process_data(chemical=chemical, year=year)

    # Only retrofit revamp tech from 2040
    if year < SECOND_RETROFIT_EARLIEST_YEAR:
        df_rank = df_rank[~(df_rank["type_of_tech_origin"] == 2)]

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

    dict_raw_material = get_availability_dict(pathway, year)

    if dict_raw_material.keys():
        force_availability_retrofit(
            pathway=pathway,
            year=year,
            chemical=chemical,
            new_stack=new_stack,
            old_stack=old_stack,
            df_rank=df_rank,
            df_process_data=df_process_data,
            dict_raw_material=dict_raw_material,
        )

    while retrofit_volume > 0:

        df_valid = apply_constraints(
            df_rank=df_rank,
            df_process_data=df_process_data,
            pathway=pathway,
            chemical=chemical,
            year=year,
        )

        df_valid = filter_existing_tech(
            stack=(
                new_stack.get_new_plant_stack()
                if (chemical in AGE_DEPENDENCY and MODEL_SCOPE == "World")
                else new_stack
            ),
            df_rank=df_valid,
            chemical=chemical,
            feedstock_switch=False,
        )

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

        # Remove plants that are too new to decommission / build new
        df_valid = remove_new_plants(df_valid=df_valid, stack=old_stack, year=year)

        if df_valid.empty:
            logger.info("No more retrofits available for %s", year)
            break

        best_transition = select_best_transition(df_rank=df_valid)

        new_plant = make_new_plant(
            best_transition=best_transition,
            df_process_data=df_process_data,
            year=year,
            retrofit=True,
            chemical=chemical,
            df_plant_capacities=pathway.df_plant_capacities,
        )

        # When the tech to be removed is not in the plant stack, pass
        remove_plant = new_stack.filter_plants(
            region=best_transition["region"],
            technology=best_transition["origin"],
            chemical=best_transition["chemical"],
        )[0]

        # Remove the old
        new_stack.remove(remove_plant)
        pathway = pathway.update_availability(
            plant=remove_plant, year=year, remove=True
        )
        new_stack.append(new_plant)
        retrofit_volume -= remove_plant.get_yearly_volume(chemical=chemical)
        pathway = pathway.update_availability(plant=new_plant, year=year)
        _log_retrofit(best_transition, new_plant, pathway, remove_plant, year)

    return pathway.update_stack(year=year + 1, stack=new_stack)


def _log_retrofit(best_transition, new_plant, pathway, remove_plant, year):
    if best_transition["retrofit_type"] == "normal":
        pathway.transitions.add(
            transition_type="retrofit",
            year=year,
            origin=remove_plant,
            destination=new_plant,
        )
    else:
        pathway.transitions.add(
            transition_type="decommission", year=year, origin=remove_plant
        )
        pathway.transitions.add(
            transition_type="new_build", year=year, destination=new_plant
        )


def get_availability_dict(pathway, year):
    # retrofit based on availability
    df_availability = pathway.get_availability(year=year)
    df_availability["remaining"] = df_availability["cap"] - df_availability["used"]
    df_availability = df_availability[df_availability.remaining < 0]
    df_availability = df_availability.drop_duplicates(
        subset=["name", "cap", "used", "remaining"]
    )
    return {
        k[1]: v
        for k, v in (
            df_availability.set_index(["region", "name"]).to_dict("index")
        ).items()
    }
