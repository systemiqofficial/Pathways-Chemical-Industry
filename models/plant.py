from uuid import uuid4

import pandas as pd

from config import METHANOL_SUPPLY_TECH
from util.util import first

# Unabated fossil tech
UNABATED_FOSSIL_TECH = {
    "PDH",
    "Refineries - Propylene",
    "Refineries Benzene/Toluene/Xylene + TX extraction",
    "Refineries Benzene/Xylene + TDP",
    "Refineries Benzene no TX extraction",
    "Refineries Benzene/Xylene + no toluene recovery",
    "Naphtha steam cracking",
    "Ethane steam cracking",
    "SMR Gas + HB",
    "Coal Gasification + HB",
    "Conventional Ammonia to AN - Gas",
    "Conventional Ammonia to AN - Coal",
    "Conventional Ammonia to Urea - Gas",
    "Conventional Ammonia to Urea - Coal",
    "Coal gasification + MeOH synthesis",
    "Natural gas SMR + MeOH synthesis",
}


class Plant:
    def __init__(
        self,
        origin,
        technology,
        region,
        start_year,
        capacity_factor,
        chemical,
        biomass_yearly,
        bio_oils_yearly,
        pyrolysis_oil_yearly,
        waste_water_yearly,
        municipal_solid_waste_rdf_yearly,
        methanol_black_yearly,
        methanol_green_yearly,
        ccs_total,
        ccs_yearly,
        plant_lifetime,
        df_plant_capacities,
        type_of_tech="Initial",
        retrofit=False,
        plant_status="new",
    ):
        self.chemical = chemical
        self.origin = origin
        self.technology = technology
        self.region = region
        self.start_year = start_year
        self.df_plant_capacities = df_plant_capacities
        self.capacities = self.import_capacities()
        self.capacity_factor = capacity_factor
        self.uuid = uuid4().hex
        self.retrofit = retrofit
        self.biomass_yearly = biomass_yearly
        self.bio_oils_yearly = bio_oils_yearly
        self.pyrolysis_oil_yearly = pyrolysis_oil_yearly
        self.waste_water_yearly = waste_water_yearly
        self.municipal_solid_waste_rdf_yearly = municipal_solid_waste_rdf_yearly
        self.methanol_black_yearly = methanol_black_yearly
        self.methanol_green_yearly = methanol_green_yearly
        self.ccs_total = ccs_total
        self.ccs_yearly = ccs_yearly
        self.plant_status = plant_status
        self.plant_lifetime = plant_lifetime
        self.type_of_tech = type_of_tech

    @property
    def byproducts(self):
        return [k for (k, v) in self.capacities.items() if v != 0]

    def get_age(self, year):
        return year - self.start_year

    def import_capacities(self) -> dict:
        """Import plant capacities for the different chemicals that this plant produces"""
        df = self.df_plant_capacities

        # Find the capacities
        df_capacities = df[
            (df.technology == self.technology) & (df.region == self.region)
        ]

        return {
            chemical: df_capacities.loc[
                df_capacities.chemical == chemical, "assumed_plant_capacity"
            ].values[0]
            for chemical in df_capacities.chemical
        }

    def get_capacity(self, chemical=None):
        """Get plant capacity"""
        return self.capacities.get(chemical or self.chemical, 0)

    def get_capacity_all(self):
        """Get plant capacity"""
        return self.capacities

    def get_yearly_volume(self, chemical):
        return self.get_capacity(chemical) * self.capacity_factor


def create_plants(n_plants: int, df_plant_capacities: pd.DataFrame, **kwargs) -> list:
    """Convenience function to create a list of plants at once"""
    return [
        Plant(df_plant_capacities=df_plant_capacities, **kwargs)
        for _ in range(n_plants)
    ]


class PlantStack:
    def __init__(self, plants: list):
        self.plants = plants
        # Keep track of all plants added this year
        self.new_ids = []

    def to_dataframe(self):
        return pd.DataFrame(
            [
                {
                    "uuid": plant.uuid,
                    "chemical": plant.chemical,
                    "technology": plant.technology,
                    "region": plant.region,
                    "start_year": plant.start_year,
                    "byproduct": plant.byproducts,
                    "capacities": plant.capacities,
                    "capacity_factor": plant.capacity_factor,
                }
                for plant in self.plants
            ]
        )

    def remove(self, remove_plant):
        self.plants.remove(remove_plant)

    def append(self, new_plant):
        self.plants.append(new_plant)
        self.new_ids.append(new_plant.uuid)

    def empty(self):
        """Return True if no plants in stack"""
        return not self.plants

    def filter_plants(
        self, region=None, technology=None, chemical=None, methanol_type=None
    ):
        """Filter plants based on one or more criteria"""
        plants = self.plants
        if region is not None:
            plants = filter(lambda plant: plant.region == region, plants)
        if technology is not None:
            plants = filter(lambda plant: plant.technology == technology, plants)
        if chemical is not None:
            plants = filter(
                lambda plant: (plant.chemical == chemical)
                or (chemical in plant.byproducts),
                plants,
            )
        if methanol_type is not None:
            plants = filter(
                lambda plant: plant.technology in METHANOL_SUPPLY_TECH[methanol_type],
                plants,
            )

        return list(plants)

    def get_fossil_plants(self, chemical):
        return [
            plant
            for plant in self.plants
            if (
                (plant.technology in UNABATED_FOSSIL_TECH)
                and (plant.chemical == chemical)
            )
        ]

    def get_capacity(self, chemical, methanol_type=None, **kwargs):
        """Get the plant capacity, optionally filtered by region, technology, chemical"""
        if methanol_type is not None:
            kwargs["methanol_type"] = methanol_type

        plants = self.filter_plants(chemical=chemical, **kwargs)
        return sum(plant.get_capacity(chemical) for plant in plants)

    def get_yearly_volume(self, chemical, methanol_type=None, **kwargs):
        """Get the yearly volume, optionally filtered by region, technology, chemical"""
        if methanol_type is not None:
            kwargs["methanol_type"] = methanol_type

        plants = self.filter_plants(chemical=chemical, **kwargs)
        return sum(plant.get_yearly_volume(chemical=chemical) for plant in plants)

    def get_tech(self, id_vars, chemical=None):
        """
        Get technologies of this stack

        Args:
            id_vars: aggregate by these variables

        Returns:
            Dataframe with technologies
        """

        df = pd.DataFrame(
            [
                {
                    "chemical": plant.chemical,
                    "technology": plant.technology,
                    "region": plant.region,
                    "retrofit": plant.retrofit,
                    "capacity": plant.get_capacity_all(),
                }
                for plant in self.plants
            ]
        )
        #drop chem and rename byproduct
        df_byproduct = pd.DataFrame([*df.capacity], df.index).stack().rename_axis([None, 'chemicals']).reset_index(1,name='capacity')
        df = df.explode("capacity")
        df = df.rename(columns={"capacity":"byproduct"})
        df = pd.concat([df,df_byproduct], axis=1)
        df.drop(columns=["chemical", "byproduct"], inplace=True)
        df.rename(columns={"chemicals":"chemical"}, inplace=True)

        try:
            return df.groupby(id_vars).agg(
                capacity=("capacity", "sum"), number_of_plants=("capacity", "count")
            )
        except KeyError:
            # There are no plants
            return pd.DataFrame()

    def get_new_plant_stack(self):
        return PlantStack(
            plants=[plant for plant in self.plants if plant.plant_status == "new"]
        )

    def get_old_plant_stack(self):
        return PlantStack(
            plants=[plant for plant in self.plants if plant.plant_status == "old"]
        )

    def get_unique_tech(self, chemical=None):
        if chemical is not None:
            plants = self.filter_plants(chemical=chemical)
        else:
            plants = self.plants

        valid_combos = {(plant.technology, plant.region) for plant in plants}
        return pd.DataFrame(valid_combos, columns=["technology", "region"])

    def get_regional_contribution(self):
        df_agg = (
            pd.DataFrame(
                [
                    {
                        "region": plant.region,
                        "capacity": plant.get_capacity(),
                    }
                    for plant in self.plants
                ]
            )
            .groupby("region", as_index=False)
            .sum()
        )
        df_agg["proportion"] = df_agg["capacity"] / df_agg["capacity"].sum()
        return df_agg

    def aggregate_stack(self, chemical=None, year=None, this_year=False):

        # Filter for chemical
        if chemical is not None:
            plants = self.filter_plants(chemical=chemical)
        else:
            plants = self.plants

        # Keep only plants that were built in a year
        if this_year:
            plants = [plant for plant in plants if plant.uuid in self.new_ids]

        # Calculate capacity and number of plants for new and retrofit
        try:
            df_agg = (
                pd.DataFrame(
                    [
                        {
                            "capacity": plant.get_capacity(chemical),
                            "yearly_volume": plant.get_yearly_volume(chemical=chemical),
                            "technology": plant.technology,
                            "origin": plant.origin,
                            "region": plant.region,
                            "retrofit": plant.retrofit,
                        }
                        for plant in plants
                    ]
                )
                .groupby(["origin", "technology", "region", "retrofit"], as_index=False)
                .agg(
                    capacity=("capacity", "sum"),
                    number_of_plants=("capacity", "count"),
                    yearly_volume=("yearly_volume", "sum"),
                )
            ).fillna(0)

            # Helper column to avoid having True and False as column names
            df_agg["build_type"] = "new_build"
            df_agg.loc[df_agg.retrofit, "build_type"] = "retrofit"

            df = df_agg.pivot_table(
                values=["capacity", "number_of_plants", "yearly_volume"],
                index=["region", "origin", "technology"],
                columns="build_type",
                dropna=False,
                fill_value=0,
            )

            # Make sure all columns are present
            for col in [
                ("capacity", "retrofit"),
                ("capacity", "new_build"),
                ("number_of_plants", "retrofit"),
                ("number_of_plants", "new_build"),
                ("yearly_volume", "retrofit"),
                ("yearly_volume", "new_build"),
            ]:
                if col not in df.columns:
                    df[col] = 0

            # Add totals
            df[("capacity", "total")] = (
                df[("capacity", "new_build")] + df[("capacity", "retrofit")]
            )
            df[("number_of_plants", "total")] = (
                df[("number_of_plants", "new_build")]
                + df[("number_of_plants", "retrofit")]
            )
            df[("yearly_volume", "total")] = (
                df[("yearly_volume", "new_build")] + df[("yearly_volume", "retrofit")]
            )

        # No plants exist
        except KeyError:
            return pd.DataFrame()

        df.columns.names = ["quantity", "build_type"]

        # Add year to index if passed (to help identify chunks)
        if year is not None:
            df["year"] = year
            df = df.set_index("year", append=True)

        return df

    def get_tech_plant_stack(self, technology: str):
        return PlantStack(
            plants=[plant for plant in self.plants if plant.technology == technology]
        )


def make_new_plant(
    best_transition, df_process_data, year, retrofit, chemical, df_plant_capacities
):
    """
    Make a new plant, based on a transition entry from the ranking dataframe

    Args:
        best_transition: The best transition (destination is the plant to build)
        df_process_data: The inputs dataframe (needed for plant specs)
        year: Build the plant in this year
        retrofit: Plant is retrofitted from an old plant

    Returns:
        The new plant
    """
    df_process_data = df_process_data.reset_index()
    spec = df_process_data[
        (df_process_data.technology == best_transition["destination"])
        & (df_process_data.year == best_transition["year"])
        & (df_process_data.region == best_transition["region"])
    ]

    # Map tech type back from ints
    types_of_tech = {1: "Initial", 2: "Transition", 3: "End-state"}
    type_of_tech = types_of_tech[best_transition["type_of_tech_destination"]]

    return Plant(
        technology=first(spec["technology"]),
        origin=first(spec["origin"]),
        region=first(spec["region"]),
        start_year=year,
        retrofit=retrofit,
        biomass_yearly=first(spec["inputs", "Raw material", "biomass_yearly"]),
        bio_oils_yearly=first(spec["inputs", "Raw material", "bio_oils_yearly"]),
        pyrolysis_oil_yearly=first(
            spec["inputs", "Raw material", "pyrolysis_oil_yearly"]
        ),
        waste_water_yearly=first(spec["inputs", "Raw material", "waste_water_yearly"]),
        municipal_solid_waste_rdf_yearly=first(
            spec["inputs", "Raw material", "municipal_solid_waste_rdf_yearly"]
        ),
        methanol_black_yearly=first(
            spec["inputs", "Raw material", "methanol_black_yearly"]
        ),
        methanol_green_yearly=first(
            spec["inputs", "Raw material", "methanol_green_yearly"]
        ),
        ccs_total=first(spec["emissions", "", "ccs_total"]),
        ccs_yearly=first(spec["emissions", "", "ccs_yearly"]),
        plant_lifetime=first(spec["spec", "", "plant_lifetime"]),
        chemical=chemical,
        capacity_factor=first(spec["spec", "", "capacity_factor"]),
        type_of_tech=type_of_tech,
        df_plant_capacities=df_plant_capacities,
    )
