import pandas as pd
import pytest

from models.plant import Plant


def _make_plant(chemical, technology):
    data = [
        ("Ethylene", "MTO - Black", "Africa", 100),
        ("Propylene", "MTO - Black", "Africa", 150),
        ("Xylene", "MTO - Black", "Africa", 50),
        ("Benzene", "Refineries Benzene/Xylene + TDP", "Africa", 40),
        ("Xylene", "Refineries Benzene/Xylene + TDP", "Africa", 100),
        ("Benzene", "Refineries Benzene/Toluene/Xylene + TX extraction", "Africa", 24),
    ]
    plant_capacities = pd.DataFrame(
        columns=["chemical", "technology", "region", "assumed_plant_capacity"],
        data=data,
    )

    return Plant(
        technology=technology,
        region="Africa",
        start_year=2020,
        capacity_factor=1,
        chemical=chemical,
        biomass_yearly=0,
        bio_oils_yearly=0,
        pyrolysis_oil_yearly=0,
        waste_water_yearly=0,
        municipal_solid_waste_rdf_yearly=0,
        methanol_black_yearly=0,
        methanol_green_yearly=0,
        ccs_total=0,
        plant_lifetime=30,
        retrofit=False,
        plant_status="new",
        df_plant_capacities=plant_capacities,
    )


def test_get_capacity():
    """Should give back the right capacity when we ask for the byproduct"""

    plant = _make_plant(chemical="Ethylene", technology="MTO - Black")
    assert plant.get_capacity() == 100
    assert plant.get_capacity("Propylene") == 150
    assert plant.get_capacity("Xylene") == 50
    assert plant.get_capacity("Ammonia") == 0

    plant = _make_plant(
        chemical="Benzene", technology="Refineries Benzene/Xylene + TDP"
    )
    assert plant.get_capacity("Benzene") == 40
    assert plant.get_capacity("Xylene") == 100

    plant = _make_plant(
        chemical="Toluene",
        technology="Refineries Benzene/Toluene/Xylene + TX extraction",
    )
    assert plant.get_capacity("Benzene") == 24


def test_byproducts():
    """Should give back the right byproducts"""
    plant = _make_plant(chemical="Ethylene", technology="MTO - Black")
    assert "Propylene" in plant.byproducts
