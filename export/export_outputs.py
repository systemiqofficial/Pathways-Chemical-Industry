import pandas as pd
import plotly.express as px
from plotly.offline import plot

from flow.import_data.intermediate_data import IntermediateDataImporter
from models.decarbonization import DecarbonizationPathway


def load_technologies_over_time_region(
    chemical: str, dataloader: IntermediateDataImporter
):
    df_region_tech = dataloader.get_technology_distribution(chemical=chemical)
    df_tech = df_region_tech.groupby(["year", "technology"]).sum()
    return df_region_tech, df_tech


def load_variable_per_year(
    chemical: str,
    dataloader: IntermediateDataImporter,
    variable: str,
    index: list[str],
    transpose: bool = True,
) -> pd.DataFrame:
    df = dataloader.get_variable_per_year(chemical=chemical, variable=variable)
    if transpose:
        df = df.transpose()
    df["chemical"] = chemical
    df.set_index(index, append=True, inplace=True)
    return df


def append_emissions(
    df_emissions: pd.DataFrame, df_yearly_outputs_trans: pd.DataFrame, chemical: str
) -> pd.DataFrame:
    df_emissions = df_emissions.append(
        df_yearly_outputs_trans.loc[
            [
                (chemical, "emissions", "yearly_emissions_total"),
                (chemical, "emissions", "yearly_emissions_scope_1"),
                (chemical, "emissions", "yearly_emissions_scope_2"),
                (chemical, "emissions", "yearly_emissions_scope_3_upstream"),
                (chemical, "emissions", "yearly_emissions_scope_3_downstream"),
                (chemical, "ccs", "co2_captured"),
                (chemical, "cumulative", "yearly_emissions_total"),
                (chemical, "cumulative", "co2_captured"),
            ],
            :,
        ]
    )
    return df_emissions


def append_costs(
    df_costs: pd.DataFrame, df_yearly_outputs_trans: pd.DataFrame, chemical: str
) -> pd.DataFrame:
    df_costs = df_costs.append(
        df_yearly_outputs_trans.loc[
            [
                (chemical, "lcox", "new_build_brownfield"),
                (chemical, "capex", "capex_new_build"),
                (chemical, "capex", "capex_retrofit"),
                (chemical, "capex", "capex_yearly"),
                (chemical, "opex", "opex_new_build"),
                (chemical, "opex", "opex_retrofit"),
                (chemical, "opex", "opex_yearly"),
                (chemical, "cumulative", "capex_yearly"),
            ],
            :,
        ]
    )
    return df_costs


def append_inputs(
    df_inputs: pd.DataFrame,
    df_input_conversion: pd.DataFrame,
    df_yearly_outputs_trans: pd.DataFrame,
    chemical: str,
) -> pd.DataFrame:
    df_append = df_yearly_outputs_trans.loc[(chemical, "inputs")]

    df_append["category"] = "inputs"
    df_append["chemical"] = chemical
    df_append.set_index(["chemical", "category"], append=True, inplace=True)
    df_append = df_append.reorder_levels(["chemical", "category", "quantity"], axis=0)

    # Only consider source with conversion factor unequal to 1
    df_input_conversion.reset_index(inplace=True)
    df_input_conversion.set_index(["category", "name"], inplace=True)

    df_input_conversion = df_input_conversion[
        df_input_conversion["Conversion factor"] != 1
    ]

    for cat, mat in df_input_conversion.index.unique():
        category_material_name = mat + " " + cat
        conversion_factor = df_input_conversion.query(
            "category == @cat & name == @mat"
        )["Conversion factor"].values[0]

        # Multiply the conversion factor on the selected fields
        df_append[
            df_append.index.get_level_values("quantity").str.contains(
                category_material_name
            )
        ] = (
            df_append.query("quantity.str.contains(@category_material_name)")
            * conversion_factor
        )

    return df_inputs.append(df_append)


def set_chemical_index(
    df: pd.DataFrame, chemical: str, index: list[str]
) -> pd.DataFrame:
    df["chemical"] = chemical
    df.set_index("chemical", append=True, inplace=True)
    index.insert(0, "chemical")
    return df.reorder_levels(index, axis=0)


def pivot_data_tech(df: pd.DataFrame) -> pd.DataFrame:
    # Pivot
    df_pivot = df.pivot_table(
        values=[("yearly_volume", "total")], index="technology", columns="year"
    )
    df_pivot.columns = df_pivot.columns.droplevel().droplevel()
    return df_pivot


def pivot_data_region(df: pd.DataFrame) -> pd.DataFrame:
    # Aggregation and pivot
    df_pivot = df.pivot_table(
        values=[("yearly_volume", "total")], index="region", columns="year", aggfunc="sum"
    )
    df_pivot.columns = df_pivot.columns.droplevel().droplevel()
    return df_pivot


def export_outputs(
    pathway: DecarbonizationPathway, sensitivity: str, chemicals: list, model_scope: str
):
    """
    Export outputs made in calculate_outputs to a format more suited for analysis (pivot and aggregate)

    Args:
        pathway: the pathway object
        sensitivity:
        chemicals:
        model_scope:
    """
    importer = IntermediateDataImporter(
        pathway=pathway,
        sensitivity=sensitivity,
        chemicals=chemicals,
        model_scope=model_scope,
    )

    plot_availabilities(importer)

    df_all_emission_output = pd.DataFrame()
    df_all_cost_output = pd.DataFrame()
    df_all_capex_output = pd.DataFrame()
    df_all_opex_output = pd.DataFrame()
    df_all_inputs_output = pd.DataFrame()
    df_all_region = pd.DataFrame()
    df_all_tech_output = pd.DataFrame()

    df_input_conversion = importer.get_input_conversion()

    for chemical in chemicals:
        # Load result data from model from a CSV
        (
            df_tech_over_time_region,
            df_tech_over_time,
        ) = load_technologies_over_time_region(chemical=chemical, dataloader=importer)

        df_yearly_outputs_trans = load_variable_per_year(
            chemical=chemical,
            dataloader=importer,
            variable="outputs",
            index=["chemical"],
        )
        df_yearly_outputs_trans = df_yearly_outputs_trans.reorder_levels(
            ["chemical", "category", "quantity"], axis=0
        )

        df_yearly_opex = load_variable_per_year(
            chemical=chemical,
            dataloader=importer,
            variable="opex",
            index=["chemical"],
            transpose=False,
        )

        df_yearly_capex = load_variable_per_year(
            chemical=chemical,
            dataloader=importer,
            variable="capex",
            index=["chemical"],
            transpose=False,
        )

        # Append every chemical to consolidated emissions dataframe
        df_all_emission_output = append_emissions(
            df_all_emission_output, df_yearly_outputs_trans, chemical
        )
        df_all_cost_output = append_costs(
            df_all_cost_output, df_yearly_outputs_trans, chemical
        )
        df_all_inputs_output = append_inputs(
            df_all_inputs_output, df_input_conversion, df_yearly_outputs_trans, chemical
        )

        df_all_capex_output = df_all_capex_output.append(df_yearly_opex)

        df_all_opex_output = df_all_opex_output.append(df_yearly_capex)

        # Aggregate and pivot/transpose the data
        df_pivot_tech = pivot_data_tech(df_tech_over_time)
        df_pivot_tech = set_chemical_index(
            df_pivot_tech, chemical=chemical, index=["technology"]
        )

        df_pivot_region = pivot_data_region(df_tech_over_time_region)
        df_pivot_region = set_chemical_index(
            df_pivot_region, chemical=chemical, index=["region"]
        )

        df_all_region = df_all_region.append(df_pivot_region)
        df_all_tech_output = df_all_tech_output.append(df_pivot_tech)

        # Export data to CSV
        importer.export_data(
            df=df_pivot_tech, filename="pivot_tech.csv", export_dir=f"final/{chemical}"
        )
        importer.export_data(
            df=df_pivot_region,
            filename="pivot_region.csv",
            export_dir=f"final/{chemical}",
        )
        importer.export_data(
            df=df_yearly_outputs_trans,
            filename="trans_outputs_per_year.csv",
            export_dir=f"final/{chemical}",
        )

    importer.export_data(
        df=df_all_emission_output,
        filename="emission_output.csv",
        export_dir="final/All",
    )
    importer.export_data(
        df=df_all_cost_output,
        filename="cost_output.csv",
        export_dir="final/All",
    )
    importer.export_data(
        df=df_all_inputs_output,
        filename="inputs_output.csv",
        export_dir="final/All",
    )
    importer.export_data(
        df=df_all_opex_output,
        filename="opex_output.csv",
        export_dir="final/All",
    )
    importer.export_data(
        df=df_all_capex_output,
        filename="capex_output.csv",
        export_dir="final/All",
    )

    importer.export_data(
        df=df_all_region,
        filename="all_chemical_region.csv",
        export_dir="final/All",
    )
    importer.export_data(
        df=df_all_tech_output,
        filename="all_chemical_tech.csv",
        export_dir="final/All",
    )


def plot_availabilities(importer: IntermediateDataImporter):
    """
    Plot availabilities of resources: the cap and amount used per chemical and total

    Args:
        importer: to import the data
    """
    df_availability = importer.get_availability_used()
    df_long = df_availability.drop(columns=["Unnamed: 0", "unit"]).melt(
        id_vars=["name", "region", "year"]
    )
    for resource in df_long.name.unique():
        for chemical in importer.chemicals + ["All"]:
            df = df_long[
                (df_long.name == resource) & (df_long.variable.str.contains(chemical))
            ]
            fig = px.line(df, x="year", y="value", color="region", line_dash="variable")
            path = importer.export_dir.joinpath("final", chemical, "availability")
            path.mkdir(exist_ok=True, parents=True)

            plot(
                fig,
                filename=f"{path}/{resource}.html",
                auto_open=False,
            )
