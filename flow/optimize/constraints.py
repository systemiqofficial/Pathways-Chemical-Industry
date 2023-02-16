import logging

import pandas as pd

from config import (MAX_PLANTS_RAMP_UP, MAX_TECH_RAMP_RATE, METHANOL_TYPES,
                    REGIONAL_CAP)
from models.decarbonization import DecarbonizationPathway
from models.plant import PlantStack

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


def apply_regional_cap(stack: PlantStack, df_rank: pd.DataFrame):
    """Filter regions where we reach the regional cap"""
    df_regions = stack.get_regional_contribution()
    df_valid_regions = df_regions.loc[df_regions["proportion"] < REGIONAL_CAP, "region"]
    return df_rank.merge(df_valid_regions)


def apply_tech_ramp_rate(
    old_stack: PlantStack, new_stack: PlantStack, df_rank: pd.DataFrame, chemical: str
):
    """Remove tech that would violate the max ramp up rate"""

    rates = (
        new_stack.get_tech(id_vars="technology", chemical=chemical)["capacity"]
        / old_stack.get_tech(id_vars="technology", chemical=chemical)["capacity"]
    )

    # Tech that is new receives an artificially high rate (first n plants will still be allowed bt MAX_PLANTS_RAMP_UP)
    rates.fillna(10, inplace=True)

    # We don't allow relative year on year growth above this rate
    too_much_growth = rates > MAX_TECH_RAMP_RATE

    # Or, when numbers are low, we don't allow absolute growth above this number

    df_number_of_plants = pd.concat(
        [
            new_stack.get_tech(id_vars="technology", chemical=chemical)[
                "number_of_plants"
            ],
            old_stack.get_tech(id_vars="technology", chemical=chemical)[
                "number_of_plants"
            ],
        ],
        axis=1,
    ).fillna(0)

    df_number_of_plants.columns = [
        "number_of_plants_new_stack",
        "number_of_plants_old_stack",
    ]

    df_number_of_plants["plants_difference"] = (
        df_number_of_plants.number_of_plants_new_stack
        - df_number_of_plants.number_of_plants_old_stack
    )

    too_many_plants = (df_number_of_plants["plants_difference"]) > MAX_PLANTS_RAMP_UP

    invalid_rates = rates[too_much_growth & too_many_plants]

    if not invalid_rates.empty:
        logger.debug("Removing tech because of rates, %s", invalid_rates)
        df_rank = df_rank[~df_rank["destination"].isin(invalid_rates.index)]

    return df_rank


def remove_initial_tech(df_rank: pd.DataFrame, df_tech: pd.DataFrame):
    """Filter initial tech out of the ranking df"""
    return df_rank.merge(
        df_tech.loc[df_tech["type_of_tech"] != "Initial", ["technology"]],
        left_on="destination",
        right_on="technology",
    ).drop(columns="technology")


def filter_available_tech(
    df_tech: pd.DataFrame, year: int, df_rank: pd.DataFrame, chemical: str
):
    """Keep only tech available in this year"""
    df_tech = df_tech.loc[
        (year >= df_tech.available_from)
        & (year <= df_tech.available_until)
        & (df_tech.chemical == chemical),
        ["type_of_tech", "technology"],
    ]
    return df_rank.merge(df_tech, left_on="destination", right_on="technology")


def apply_constraints(
    df_rank: pd.DataFrame,
    df_process_data: pd.DataFrame,
    pathway: DecarbonizationPathway,
    chemical: str,
    year: int,
    filter_naphtha_na=False,
):
    """Apply constraints on raw materials and CCS"""
    df_availability = pathway.get_availability(year=year)

    # when used went to negative cap it to zero
    df_availability.loc[
        (df_availability[f"{chemical}_used"] < 0), f"{chemical}_used"
    ] = 0
    df_availability.loc[(df_availability.used < 0), "used"] = 0

    # calculate the available cap sizes
    df_availability[f"{chemical}_remaining"] = (
        df_availability[f"{chemical}_cap"] - df_availability[f"{chemical}_used"]
    )
    df_availability[f"{chemical}_remaining"].clip(lower=0, inplace=True)

    df_availability["remaining"] = df_availability["cap"] - df_availability["used"]
    df_availability.remaining.clip(lower=0, inplace=True)

    for material_constraints in ["CO2 storage", "Biomass"]:
        df_availability.loc[
            (df_availability.name == material_constraints), "remaining"
        ] = df_availability.loc[
            (df_availability.name == material_constraints), f"{chemical}_remaining"
        ]
    # Merge remaining resources (expect for Methanol as it is not regional)
    df_availability_pivot = df_availability.pivot_table(
        values="remaining", index="region", columns="name"
    )
    df_rank = df_rank.merge(df_availability_pivot, on="region")

    # Add remaining methanol green/black
    for methanol_type in METHANOL_TYPES:
        df_rank[methanol_type] = df_availability.loc[
            df_availability.name == methanol_type, "remaining"
        ].values[0]

    # Get data on CCS/biomass
    df_process_data = df_process_data[
        [
            ("emissions", "", "ccs_yearly"),
            ("inputs", "Raw material", "biomass_yearly"),
            ("inputs", "Raw material", "bio_oils_yearly"),
            ("inputs", "Raw material", "pyrolysis_oil_yearly"),
            ("inputs", "Raw material", "waste_water_yearly"),
            ("inputs", "Raw material", "municipal_solid_waste_rdf_yearly"),
            ("inputs", "Raw material", "methanol_green_yearly"),
            ("inputs", "Raw material", "methanol_black_yearly"),
        ]
    ]
    df_process_data.columns = df_process_data.columns.get_level_values(level="name")
    df_process_data = df_process_data.reset_index()
    df_process_data = df_process_data.rename(columns={"technology": "destination"})

    # Do not allow new naphtha technology in US (hardcoded)
    if filter_naphtha_na:
        df_process_data = df_process_data[
            ~(
                (df_process_data.destination.str.contains("Naphtha"))
                & (df_process_data.region.isin(["North America"]))
            )
        ].reset_index(drop=True)

    df_rank = df_rank.merge(
        df_process_data,
        on=["destination", "origin", "region"],
    )
    df_rank = df_rank.fillna(0)

    # Remove tech that would exceed one of the resource caps
    df_rank = df_rank[
        (df_rank["biomass_yearly"] <= df_rank["Biomass"])
        & (df_rank["bio_oils_yearly"] <= df_rank["Bio-oils"])
        & (df_rank["pyrolysis_oil_yearly"] <= df_rank["Pyrolysis oil"])
        & (df_rank["ccs_yearly"] <= df_rank["CO2 storage"])
        & (df_rank["waste_water_yearly"] <= df_rank["Waste water"])
        & (
            df_rank["municipal_solid_waste_rdf_yearly"]
            <= df_rank["Municipal solid waste RdF"]
        )
        & (df_rank["methanol_black_yearly"] <= df_rank["Methanol - Black"])
        & (df_rank["methanol_green_yearly"] <= df_rank["Methanol - Green"])
    ]

    return df_rank
