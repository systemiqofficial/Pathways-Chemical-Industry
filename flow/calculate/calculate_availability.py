import pandas as pd

from models.plant import Plant


def update_availability_from_plant(
    df_availability: pd.DataFrame, plant: Plant, year: int, remove: bool = False
):
    """
    Update availabilities based on a plant that is added or removed

    Args:
        df_availability: Availabilities data
        plant: The plant in consideration
        year: Year the plant is added or removed
        remove: The plant is removed

    Returns:
        The updated availability data
    """

    # Update constraints for chemical and total
    for used in [f"{plant.chemical}_used", "used"]:
        # Update biomass constraint
        df_availability.loc[
            (df_availability.name == "Biomass")
            & (df_availability.year == year)
            & (df_availability.region == plant.region),
            used,
        ] += (
            -plant.biomass_yearly if remove else plant.biomass_yearly
        )
        # Update CCS constraint
        df_availability.loc[
            (df_availability.name == "CO2 storage")
            & (df_availability.year == year)
            & (df_availability.region == plant.region),
            used,
        ] += (
            -plant.ccs_yearly if remove else plant.ccs_yearly
        )
        # Update waste water constraint
        df_availability.loc[
            (df_availability.name == "Waste water")
            & (df_availability.year == year)
            & (df_availability.region == plant.region),
            used,
        ] += (
            -plant.waste_water_yearly if remove else plant.waste_water_yearly
        )
        # Update pyrolysis oil constraint
        df_availability.loc[
            (df_availability.name == "Pyrolysis oil")
            & (df_availability.year == year)
            & (df_availability.region == plant.region),
            used,
        ] += (
            -plant.pyrolysis_oil_yearly if remove else plant.pyrolysis_oil_yearly
        )
        # Update bio-oils constraint
        df_availability.loc[
            (df_availability.name == "Bio-oils")
            & (df_availability.year == year)
            & (df_availability.region == plant.region),
            used,
        ] += (
            -plant.bio_oils_yearly if remove else plant.bio_oils_yearly
        )
        # Update municipal solid waste  constraint
        df_availability.loc[
            (df_availability.name == "Municipal solid waste RdF")
            & (df_availability.year == year)
            & (df_availability.region == plant.region),
            used,
        ] += (
            -plant.municipal_solid_waste_rdf_yearly
            if remove
            else plant.municipal_solid_waste_rdf_yearly
        )
    # Update methanol constraint
    df_availability.loc[
        (df_availability.name == "Methanol - Black") & (df_availability.year == year),
        "used",
    ] += (
        -plant.methanol_black_yearly if remove else plant.methanol_black_yearly
    )
    df_availability.loc[
        (df_availability.name == "Methanol - Green") & (df_availability.year == year),
        "used",
    ] += (
        -plant.methanol_green_yearly if remove else plant.methanol_green_yearly
    )

    return df_availability


def make_empty_methanol_availability():
    """
    Add empty methanol availability (we calculate actual availabilities
    later from the methanol stack, this is just a placeholder)
    """
    dfs = []
    for color in ["Green", "Black"]:
        df = pd.DataFrame(
            data={
                "name": f"Methanol - {color}",
                "region": "World",
                "cap": None,
                "unit": "t/annum",
                "year": range(2020, 2051),
                "used": 0,
            }
        )
        dfs.append(df)
    return pd.concat(dfs)
