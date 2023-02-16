from functools import partial

import pandas as pd

from config import ECONOMIC_LIFETIME_YEARS, PLANT_SPEC_OVERRIDE
from flow.import_data.generic_data import GenericDataImporter
from flow.import_data.util import convert_df_to_regional


class ChemicalDataImporter(GenericDataImporter):
    """Imports data specific to a chemical"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def import_all(self):
        """Import all chemicals data"""
        functions = {
            partial(self._import_inputs): "inputs.csv",
            partial(self._import_ccs_rate): "ccs_rate.csv",
            partial(self._import_current_production): "current_production.csv",
            partial(self._import_demand): "demand.csv",
            partial(self._import_process_economics): "process_economics.csv",
            partial(self._import_plant_specs): "plant_specs.csv",
        }

        # For every data type...
        for func, filename in functions.items():
            dfs = []

            # ... export the data for every chemical and merge into one file
            for chemical in self.chemicals:
                df = func(chemical=chemical)

                if "sensitivity" in df.columns:
                    df.drop(columns="sensitivity", inplace=True)

                dfs.append(df)

            self.export_data(
                pd.concat(dfs),
                filename=filename,
                export_dir="intermediate",
            )

    def _get_chemicals_values(self, id_vars, chemical, **kwargs):
        """Base function to get values specific to a chemical"""
        id_vars.append("chemical")
        df = self.get_time_vars(
            sheet_name=f"Values - {chemical}",
            data_type="chemical",
            id_vars=id_vars,
            **kwargs,
        )
        return df.query(f"chemical == '{chemical}'")

    def _import_inputs(self, chemical):
        """Get process inputs for a chemicals"""
        df = self._get_chemicals_values(
            id_vars=["technology", "component", "category", "name", "region"],
            value_name="input",
            # unit="GJ/t NH3",
            categories=["Energy", "Raw material"],
            chemical=chemical,
        )
        return convert_df_to_regional(df).set_index(
            ["technology", "region", "chemical", "year"]
        )

    def _import_ccs_rate(self, chemical):
        """Get CCS rate per process"""
        df = self._get_chemicals_values(
            id_vars=["technology"],
            name="Capture rate",
            value_name="ccs_rate",
            chemical=chemical,
        )

        df["ccs_rate"] /= 100
        df["emissions_rate"] = 1 - df["ccs_rate"]
        return df.set_index(["technology", "chemical", "year"])

    def _import_current_production(self, chemical):
        """Get current production rates of a chemical"""
        df_prod = self._get_chemicals_values(
            id_vars=["region", "technology"],
            name="Current-day production",
            value_name="current_day_production",
            year=2020,
            chemical=chemical,
        )

        # Get the share made by old plants
        df_share = self._get_chemicals_values(
            id_vars=["region", "technology"],
            name="Current-day production % old",
            value_name="old_share",
            year=2020,
            chemical=chemical,
        )

        if df_share.empty:
            # Assume all plants are old
            df_prod["old_share"] = 1
        else:
            df_prod = df_prod.merge(
                df_share, on=["region", "technology", "year", "chemical"]
            )

        return df_prod

    def _import_demand(self, chemical):
        """Get demand for a chemical"""
        if self.sensitivity == "bdem":
            demand = "BAU demand"
        elif self.sensitivity == "ldem":
            demand = "Low demand"
        else:
            demand = "High demand"

        return self._get_chemicals_values(
            id_vars=["region", "technology"],
            name=demand,
            value_name="demand",
            chemical=chemical,
        ).set_index(["region", "chemical", "year", "technology"])

    # def _import_multi_product_ratios(self):
    #     """Get data on emissions shares of different technologies"""
    #     return super()._import_multi_product_ratios()

    def _import_process_economics(self, chemical):
        """Get process economics for different processes for a chemical"""
        opex_value_vars = [
            "operations_and_maintenance",
        ]

        capex_value_vars = [
            "capex_new_build_brownfield",
            "capex_retrofit",
        ]

        df = self._get_chemicals_values(
            id_vars=["technology", "component", "name", "region"],
            categories=["Process economics"],
            chemical=chemical,
        )

        df_tech_trans = self._import_tech_transitions(
            transformation_true=False
        ).reset_index()
        df_tech_trans = df_tech_trans.query(f"chemical == '{chemical}'")
        df_tech_trans = df_tech_trans.rename({"destination": "technology"}, axis=1)
        df_tech_trans = df_tech_trans[~df_tech_trans.name.isin(["Decommission cost"])]

        df = df[~df.component.isin(["Business-case specific parameter"])]
        df_capex = df[(df.name.map(self.rename_cols)).isin(capex_value_vars)]
        df_opex = df[(df.name.map(self.rename_cols)).isin(opex_value_vars)]

        df_capex = pd.merge(
            df_capex,
            df_tech_trans,
            how="right",
            on=["technology", "component", "name", "chemical"],
        )

        df_capex = (
            df_capex.pivot_table(
                index=["origin", "technology", "component", "year", "region"],
                values=["value"],
                columns="name",
            )["value"]
            .reset_index()
            .rename(columns=self.rename_cols)[
                ["year", "region", "origin", "technology", "component"]
                + capex_value_vars
            ]
        )

        # Replace nan with zero
        df_capex = df_capex.fillna(0.0)

        # Sub-setting dataframe
        df_general = df_capex[df_capex.component.isin(["General"])]
        df_non_general = df_capex[~df_capex.component.isin(["General"])]

        # Aggregate the sub_process costs
        df_general = df_general.groupby(
            ["year", "region", "origin", "technology"]
        ).sum()
        df_non_general = df_non_general.groupby(
            ["year", "region", "origin", "technology"]
        ).sum()

        df_general = df_general.reset_index()
        df_non_general = df_non_general.reset_index()
        df_capex = df_general.append(df_non_general)

        # Add decommission costs to pathways that needs to be retrofitted
        origin_decommission_tech = df_general.loc[
            (~df_general.origin.isin(["Non-existent"]))
            & (df_general.capex_new_build_brownfield > 0)
        ]["origin"].unique()

        destination_tech = df_general.loc[
            (~df_general.origin.isin(["Non-existent"]))
            & (df_general.capex_new_build_brownfield > 0)
        ]["technology"].unique()

        for org_tech in origin_decommission_tech:
            if not df_capex.loc[
                (df_capex.technology.isin([org_tech]))
                & (df_capex.origin == "Non-existent")
            ]["capex_new_build_brownfield"].unique():
                decommission_cost = 0
            else:
                decommission_cost = (
                    df_capex.loc[
                        (df_capex.technology.isin([org_tech]))
                        & (df_capex.origin == "Non-existent")
                    ]["capex_new_build_brownfield"].unique()[0]
                    * 0.05
                )

            df_capex.loc[
                (df_capex.origin.isin([org_tech]))
                & (df_capex.technology.isin(destination_tech)),
                ["capex_new_build_brownfield"],
            ] += decommission_cost

        # Move everything that is non-existent origin from new_build_brownfield_to retrofit
        df_capex.loc[
            (
                ~df_capex.origin.isin(["Non-existent"])
                & (df_capex.capex_new_build_brownfield > 0)
            ),
            ["capex_retrofit"],
        ] = (
            df_capex.loc[
                (
                    ~df_capex.origin.isin(["Non-existent"])
                    & (df_capex.capex_new_build_brownfield > 0)
                ),
                ["capex_new_build_brownfield"],
            ]
        ).values

        df_capex.loc[
            (
                ~df_capex.origin.isin(["Non-existent"])
                & (df_capex.capex_new_build_brownfield > 0)
            ),
            ["capex_new_build_brownfield"],
        ] = 0.0

        # Add decommission cost
        df_capex["decommission_cost"] = 0.05 * df_capex["capex_new_build_brownfield"]

        # Add O&M costs based on destination
        df = pd.merge(
            df_capex,
            df_opex[["technology", "year", "region", "value"]],
            how="left",
            on=["technology", "year", "region"],
        ).rename(columns={"value": opex_value_vars[0]})

        df["chemical"] = chemical

        # Keep only regional values
        return convert_df_to_regional(df).set_index(
            ["chemical", "origin", "technology", "year", "region"]
        )

    def _import_plant_specs(self, chemical):
        """Get plant specifications for different processes for a chemical"""
        df = self._get_chemicals_values(
            id_vars=["technology", "component", "name", "region"],
            categories=["Process economics"],
            chemical=chemical,
        )

        # Keep only plant specs
        df = df[
            df.name.isin(
                ["Plant lifetime", "Assumed plant capacity", "Capacity factor"]
            )
        ]

        # For some chemicals we don't have capacity factors
        if "Capacity factor" in df.name.unique():
            value_vars = ["plant_lifetime", "assumed_plant_capacity", "capacity_factor"]
        else:
            value_vars = ["plant_lifetime", "assumed_plant_capacity"]

        # Pivot data to wide format so we can more easily do calculations later on
        df_pivot = (
            df.pivot_table(
                index=["chemical", "technology", "year", "region"],
                values="value",
                columns="name",
            )
            .reset_index()
            .rename(columns=self.rename_cols)[
                ["chemical", "year", "technology", "region"] + value_vars
            ]
        )

        df_pivot["plant_lifetime"] = df_pivot["plant_lifetime"].fillna(30)

        for prop, value in PLANT_SPEC_OVERRIDE.items():
            df_pivot[prop] = value

        # Convert plant capacity from t/day to Mton/year
        df_pivot["assumed_plant_capacity"] *= 365 / 1e6

        # Change capacity factor from percentage to fraction, ad fill with default (8000 hours)
        if "Capacity factor" in df.name.unique():
            df_pivot["capacity_factor"] /= 100
        else:
            df_pivot["capacity_factor"] = 0.913

        return _calculate_yearly_volume(
            df_pivot=df_pivot,
            chemical=chemical,
            df_multi_product_ratio=super()._import_multi_product_ratios(),
        )


def _calculate_yearly_volume(df_pivot, chemical, df_multi_product_ratio):
    """Calculate yearly volume based on plant capacity and multi product ratios"""

    df_pivot = convert_df_to_regional(df_pivot)
    df_multi_product_ratio = df_multi_product_ratio.reset_index()

    # Technologies that make multiple chemicals
    multi_chemical_tech = set(df_pivot.technology) & set(
        df_multi_product_ratio.technology
    )

    if multi_chemical_tech:
        df_pivot = df_pivot.merge(
            df_multi_product_ratio, on=["chemical", "region", "technology"], how="left"
        ).fillna(1)
        df_pivot["assumed_plant_capacity"] *= df_pivot["ratio"]
        df_pivot.drop(columns="ratio", inplace=True)

    df_pivot["total_yearly_volume"] = (
        df_pivot["assumed_plant_capacity"] * df_pivot["capacity_factor"]
    )
    df_pivot["total_volume"] = (
        df_pivot["total_yearly_volume"] * df_pivot["plant_lifetime"]
    )

    # Total volume to use in TCO calculations
    df_pivot["total_volume_economic"] = (
        df_pivot["total_yearly_volume"] * ECONOMIC_LIFETIME_YEARS
    )

    if "primary_chemical" not in df_pivot.columns:
        df_pivot["primary_chemical"] = df_pivot["chemical"]

    return df_pivot.set_index(["chemical", "technology", "year", "region"])
