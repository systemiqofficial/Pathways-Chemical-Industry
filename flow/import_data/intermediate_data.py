import pandas as pd
from pandas.errors import ParserError

from config import MODEL_SCOPE
from flow.import_data.base import BaseImporter
from util.util import make_multi_df


class IntermediateDataImporter(BaseImporter):
    """Imports data that is output by the model at some point in time"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.intermediate_path = self.export_dir.joinpath("intermediate")
        self.final_path = self.export_dir.joinpath("final")

    def get_plant_sizes(self):
        """Get plant sizes for each different chemical/process"""
        df_spec = self.get_plant_specs()
        return df_spec.reset_index()[
            ["chemical", "technology", "assumed_plant_capacity", "capacity_factor"]
        ].drop_duplicates(["chemical", "technology"])

    def get_all_plants(self):
        return pd.read_csv(self.final_path.joinpath("All", "all_plants.csv"))

    def get_availabilities(self):
        return pd.read_csv(self.intermediate_path.joinpath("availabilities.csv"))

    def get_decommission_rates(self):
        return pd.read_csv(self.intermediate_path.joinpath("decommission_rates.csv"))

    def get_emissions_shares(self):
        return pd.read_csv(self.intermediate_path.joinpath("emissions_share.csv"))

    def get_inputs(self):
        return pd.read_csv(self.intermediate_path.joinpath("inputs.csv"))

    def get_input_conversion(self):
        return pd.read_csv(self.intermediate_path.joinpath("input_conversion.csv"))

    def get_plant_specs(self):
        return pd.read_csv(
            self.intermediate_path.joinpath("plant_specs.csv"),
            index_col=["technology", "year", "region", "chemical"],
        )

    def get_plant_capacities(self):
        df_spec = self.get_plant_specs().reset_index()
        return df_spec.drop_duplicates(["chemical", "region", "technology"])[
            ["chemical", "technology", "region", "assumed_plant_capacity"]
        ]

    def get_emissions_factors(self):
        return pd.read_csv(self.intermediate_path.joinpath("emissions_factors.csv"))

    def get_demand(self):
        return pd.read_csv(self.intermediate_path.joinpath("demand.csv")).query(
            f"region =='{MODEL_SCOPE}'"
        )

    def get_ccs_rate(self):
        return pd.read_csv(self.intermediate_path.joinpath("ccs_rate.csv"))

    def get_ccs_price(self):
        return pd.read_csv(
            self.intermediate_path.joinpath("ccs_prices.csv"),
            index_col=["year", "region"],
        )

    def get_input_price(self):
        return pd.read_csv(self.intermediate_path.joinpath("input_prices.csv"))

    def get_carbon_price(self):
        return pd.read_csv(
            self.intermediate_path.joinpath("carbon_prices.csv"), index_col="year"
        )

    def get_multi_product_ratio(self):
        return pd.read_csv(self.intermediate_path.joinpath("multi_product_ratio.csv"))

    def get_current_production(self, japan_only=False):
        df = pd.read_csv(self.intermediate_path.joinpath("current_production.csv"))

        if japan_only:
            return df.query("region == 'Japan'")

        # Set Japan methanol production to 0 for global model run
        # (the value is only there for the local model to stimulate MTO)
        df.loc[
            (df.region == "Japan") & (df.chemical == "Methanol"),
            "current_day_production",
        ] = 0
        return df

    def get_process_economics(self):
        return pd.read_csv(
            self.intermediate_path.joinpath("process_economics.csv"),
            index_col=["chemical", "technology", "origin", "year", "region"],
        )

    def get_tech_transitions(self):
        return pd.read_csv(
            self.intermediate_path.joinpath("technology_transitions.csv")
        )

    def get_tech(self):
        return pd.read_csv(self.intermediate_path.joinpath("technologies.csv"))

    def get_process_data(self, data_type):
        """Get data outputted by the model on process level: cost/inputs/emissions"""
        file_path = self.intermediate_path.joinpath(f"{data_type}.csv")

        # Read multi-index
        header = [0, 1] if data_type in ["cost", "inputs_pivot"] else 0

        # Costs
        index_cols = [0, 1, 2, 3, 4] if data_type == "cost" else [0, 1, 2, 3]

        return pd.read_csv(file_path, header=header, index_col=index_cols)

    def get_all_process_data(self, chemical=None):
        """Get combined data outputted by the model on process level"""
        df_inputs_pivot = self.get_process_data("inputs_pivot")
        df_emissions = self.get_process_data("emissions")
        df_cost = self.get_process_data("cost")
        df_spec = self.get_plant_specs()

        # Add multi index layers to join
        # 2 levels for emissions/spec to get it on the right level
        df_emissions = make_multi_df(
            make_multi_df(df=df_emissions, name=""), name="emissions"
        )
        df_spec = make_multi_df(make_multi_df(df=df_spec, name=""), name="spec")
        df_cost = make_multi_df(df=df_cost, name="cost")
        df_inputs_pivot = make_multi_df(df=df_inputs_pivot, name="inputs")

        df_all = df_spec.join(df_inputs_pivot).join(df_emissions).join(df_cost)
        df_all.columns.names = ["group", "category", "name"]

        if chemical is not None:
            df_all = df_all.query(f"chemical == '{chemical}'").droplevel("chemical")
        return df_all.query("year <= 2050")

    def get_variable_per_year(self, chemical, variable):
        file_path = self.export_dir.joinpath(
            "final", chemical, f"{variable}_per_year.csv"
        )
        index_col = 0 if variable == "outputs" else [0, 1]
        return pd.read_csv(file_path, header=[0, 1], index_col=index_col)

    def get_ranking(self, rank_type, chemical, japan_only=False):
        file_path = self.export_dir.joinpath(
            "ranking", chemical, f"{rank_type}_rank.csv"
        )
        df = pd.read_csv(file_path)

        if japan_only:
            return df.query("region == 'Japan'")
        return df

    def get_post_ranking(self, rank_type, chemical, japan_only=False):
        file_path = self.export_dir.joinpath(
            "ranking", chemical, f"{rank_type}_post_rank.csv"
        )
        df = pd.read_csv(file_path)

        if japan_only:
            return df.query("region == 'Japan'")
        return df

    def get_technology_distribution(self, chemical, new=False):
        suffix = "_new" if new else ""
        file_path = self.export_dir.joinpath(
            "final", chemical, f"technologies_over_time_region{suffix}.csv"
        )
        try:
            df = pd.read_csv(file_path, index_col=[0, 1, 2, 3], header=[0, 1]).fillna(0)
        except ParserError:
            # No plants, return empty df with right columns and index
            parameters = ["capacity", "number_of_plants", "yearly_volume"]
            build_types = ["new_build", "retrofit", "total"]
            columns = pd.MultiIndex.from_product([parameters, build_types])
            index = pd.MultiIndex.from_arrays(
                [[], [], []], names=("region", "origin", "technology", "year")
            )
            return pd.DataFrame(columns=columns, index=index)

        # Only keep rows which have plants
        return df[df[("number_of_plants", "total")] != 0]

    def get_availability_used(self):
        path = self.final_path.joinpath("All", "availability_output.csv")
        return pd.read_csv(path)

    def get_transition(self, chemical):
        path = self.final_path.joinpath("All", "transitions.csv")
        df_tech_transition = pd.read_csv(path)
        df_tech_transition = df_tech_transition[df_tech_transition.chemical == chemical]
        return df_tech_transition
