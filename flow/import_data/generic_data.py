from flow.import_data.base import BaseImporter
from flow.import_data.util import (convert_df_to_regional,
                                   convert_df_to_steam_crackers)

FOSSIL_FUELS = ["Naphtha", "Natural gas", "Ethane", "Propane", "Coal"]


class GenericDataImporter(BaseImporter):
    """Imports generic data not specific to a chemical"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def import_data(self):
        functions = {
            self._import_decommission_rates: "decommission_rates.csv",
            self._import_tech: "technologies.csv",
            self._import_tech_transitions: "technology_transitions.csv",
            self._import_carbon_price: "carbon_prices.csv",
            self._import_availability: "availabilities.csv",
            self._import_input_price: "input_prices.csv",
            self._import_ccs_price: "ccs_prices.csv",
            self._import_emissions_factors: "emissions_factors.csv",
            self._import_emissions_share: "emissions_share.csv",
            self._import_multi_product_ratios: "multi_product_ratio.csv",
            self._import_input_conversion: "input_conversion.csv",
        }

        for func, filename in functions.items():
            df = func()

            if "sensitivity" in df.columns:
                df.drop(columns="sensitivity", inplace=True)

            self.export_data(df, filename=filename, export_dir="intermediate")

    def _import_decommission_rates(self):
        """Get decommission rates of different technologies"""
        df = self.get_time_vars(
            sheet_name="Decommission",
            id_vars=["technology"],
            data_type="decommission",
            value_name="decommission_rate",
        )

        df.decommission_rate /= 100

        return df.set_index("technology")

    def _import_emissions_factors(self):
        """Get emissions factors of different fuels"""
        df = self.get_time_vars(
            sheet_name="Emissions",
            id_vars=["scope", "name", "region", "pathway"],
            data_type="emissions",
            value_name="emission_factor",
        )

        # For BAU, we have different emission factors
        if self.pathway == "bau":
            df = df[
                ((df.pathway == "BAU") & (
                    df.name.isin(["Electricity - Grid", "Coal", "Naphtha", "Natural gas", "Ethane", "Propane"])))
                | ((df.pathway != "BAU") & (
                    ~df.name.isin(["Electricity - Grid", "Coal", "Naphtha", "Natural gas", "Ethane", "Propane"])))
                ]
        else:
            df = df[df.pathway != "BAU"]

        df.drop(columns="pathway", inplace=True)

        # Convert data to only have regional values
        return convert_df_to_regional(df).set_index(["scope", "name", "region", "year"])

    def _import_availability(self):
        """Get availability data for CCS / resources"""
        df = self.get_time_vars(
            sheet_name="Prices and Availability",
            categories=["Availability"],
            id_vars=["name", "region", "unit", "sensitivity"],
            data_type="prices",
        )

        # covert CO2 from Gt to t
        df.loc[df.name == "CO2 storage", "value"] *= 1e9

        default_ccs_availability = (df.name == "CO2 storage") & (
            df.sensitivity == "default"
        )
        constrained_ccs_availability = (df.name == "CO2 storage") & (
            df.sensitivity == "sensitivity"
        )
        non_ccs_availability = ~(df.name == "CO2 storage")

        if self.sensitivity == "ccs":
            df = df[constrained_ccs_availability | non_ccs_availability]
        else:
            df = df[default_ccs_availability | non_ccs_availability]

        return df.set_index(["name", "region", "value"])

    def _get_price(self):
        """Get data from the prices tab"""
        return self.get_time_vars(
            sheet_name="Prices and Availability",
            categories=["Price"],
            id_vars=["name", "region", "unit", "sensitivity"],
            data_type="prices",
        )

    def _import_emissions_share(self):
        """Get data on emissions shares of different technologies"""
        df = self.get_time_vars(
            sheet_name="Emissions share",
            id_vars=["name", "chemical", "technology"],
            data_type="emissions_share",
            value_name="emissions_share",
        )

        # Go from percentage to share
        df["emissions_share"] /= 100
        return df

    def _import_carbon_price(self):
        """Get carbon data"""
        df_price = self._get_price()
        return (
            df_price[df_price.name == "Carbon"]
            .rename(columns={"value": "carbon_price"})
            .drop(columns=["region", "name", "unit"])
            .set_index("year")
        )

    def _import_ccs_price(self):
        """Get CCS prices"""
        df_price = self._get_price()
        return (
            df_price[df_price.name == "CCS T&S"]
            .rename(columns={"value": "ccs_price"})
            .drop(columns=["name", "unit"])
            .set_index(["year", "region"])
        )

    def _import_input_price(self):
        """Get prices of process inputs, such as coal or gas"""
        df_price = self._get_price()
        df_price = (
            df_price[~df_price.name.isin(["CCS T&S", "Carbon"])]
            .rename(columns={"value": "input_price"})
            .drop(columns="unit")
        )

        default_fossil_prices = df_price.name.isin(FOSSIL_FUELS) & (
            df_price.sensitivity == "default"
        )
        low_fossil_prices = df_price.name.isin(FOSSIL_FUELS) & (
            df_price.sensitivity == "sensitivity"
        )
        non_fossil_prices = ~df_price.name.isin(FOSSIL_FUELS)

        if self.sensitivity == "lfos":
            df_price = df_price[low_fossil_prices | non_fossil_prices]
        else:
            df_price = df_price[default_fossil_prices | non_fossil_prices]

        return convert_df_to_regional(df_price).set_index(["name", "region", "year"])

    def _import_tech(self):
        """Get the different technologies and their start/end dates"""
        return self._get_excel(
            sheet_name="Tech Desc and Dates - All", usecols="A:K"
        ).set_index(["chemical", "technology"])

    def _import_tech_transitions(self, transformation_true=True):
        """Get the possible technology transitions"""
        df = self._get_excel(sheet_name="Tech Origin-Destination - All", usecols="A:F")

        # Push tech transition that is not non-existent in origin to retrofit
        if transformation_true:
            df.loc[
                ~df.origin.isin(["Non-existent"])
                & df.name.isin(["CAPEX - new build brownfield"]),
                "name",
            ] = "CAPEX - retrofit"
        return df.set_index(["chemical", "origin", "destination", "component"])

    def _import_multi_product_ratios(self):
        """Get data on emissions shares of different technologies"""
        df = self._get_excel(sheet_name="Multi chemicals ratios", usecols="A:I")

        # Convert to regional for non-steam cracker tech
        df = convert_df_to_regional(df)

        # Convert to different steam cracker technologies
        df = convert_df_to_steam_crackers(df)
        return df.melt(
            id_vars=["region", "technology", "primary_chemical"],
            var_name="chemical",
            value_name="ratio",
        ).set_index(["chemical", "technology", "region"])

    def _import_input_conversion(self):
        return self._get_excel(sheet_name="Unit conversion", usecols="A:E").set_index(
            ["category", "name"]
        )
