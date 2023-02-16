import pandas as pd

from flow.import_data.intermediate_data import IntermediateDataImporter
from util.util import make_multi_df

cost_components = [
    ("cost", "lcox", "new_build_brownfield"),
    ("cost", "lcox_contribution", "capex_new_build_brownfield"),
    ("cost", "lcox_contribution", "operations_and_maintenance"),
    ("cost", "lcox_contribution", "energy_electricity"),
    ("cost", "lcox_contribution", "energy_non_electricity"),
    ("cost", "lcox_contribution", "raw_material_total"),
    ("cost", "lcox_contribution", "carbon"),
    ("cost", "lcox_contribution", "ccs"),
]


def _calculate_ethylene_fossil_share(importer) -> pd.DataFrame:
    """
    Calculate the yearly share of fossil for ethylene.
    This multiplication factor is used as a proxy to calculate scope 3
    downstream emissions for Pyrolysis oil
    """

    df_tech_dist = importer.get_technology_distribution(chemical="Ethylene")
    df_tech_dist = df_tech_dist.groupby(["year", "region", "technology"]).sum()
    df_capacity = df_tech_dist["capacity", "total"].reset_index()
    df_capacity.columns = df_capacity.columns.droplevel(1)

    df_tech = importer.get_tech()
    fossil_ethylene_tech = df_tech[
        (df_tech.chemical == "Ethylene")
        & (df_tech.category_detailed.str.contains("Fossil"))
    ].technology.unique()
    df_capacity["tech_type"] = "green"
    df_capacity.loc[
        df_capacity["technology"].isin(fossil_ethylene_tech), "tech_type"
    ] = "fossil"

    # Calculate yearly share of fossil
    df_agg = df_capacity.pivot_table(
        values="capacity", index="year", columns="tech_type", aggfunc="sum"
    ).fillna(0)

    if "green" not in df_agg.columns:
        df_agg["green"] = 0

    df_agg["total"] = df_agg["fossil"] + df_agg["green"]
    df_agg["share_fossil"] = df_agg["fossil"] / df_agg["total"]

    # Shift forward one year; this year's production is next year's fuel
    df_share = df_agg["share_fossil"]
    return df_share.shift(1).fillna(1)


def calculate_emissions(df, df_ethylene_fossil):
    df_out = pd.DataFrame()

    # Calculate total scope 1,2,3 emissions
    for scope in ["1", "2", "3_upstream", "3_downstream"]:
        df_out["yearly_emissions_scope_" + scope] = (
            df["emissions", "", "scope_" + scope]
            * df["spec", "", "total_yearly_volume"]
            * df["tech_total", "number_of_plants", "total"]
        )

    # Grab total emissions
    df_out["yearly_emissions_total"] = (
        df_out["yearly_emissions_scope_1"]
        + df_out["yearly_emissions_scope_2"]
        + df_out["yearly_emissions_scope_3_upstream"]
        + df_out["yearly_emissions_scope_3_downstream"]
    )

    # Add in fossil share for Pyrolysis oil
    df_out = df_out.reset_index().merge(df_ethylene_fossil, on="year")
    df_out.loc[
        df_out.technology.str.contains("Pyrolysis"),
        "yearly_emissions_scope_3_downstream",
    ] *= df_out.loc[df_out.technology.str.contains("Pyrolysis"), "share_fossil"]
    return df_out


def calculate_co2_captured(df):
    df_out = pd.DataFrame()

    # Calculate CO2 captured
    df_out["co2_captured"] = (
        df["emissions", "", "ccs_capacity"]
        * df["spec", "", "total_yearly_volume"]
        * df["tech_total", "number_of_plants", "total"]
    )

    return df_out


def calculate_inputs(df, chemical):
    df_out = pd.DataFrame()

    # Calculate electricity used
    df_out["electricity_consumption"] = (
        df["inputs", "Energy", "electricity"]
        * df["spec", "", "total_yearly_volume"]
        * df["tech_total", "number_of_plants", "total"]
    )

    # Calculate H2 used (0.177 ton H2 per ton NH3, 120 is heating value of H2)
    df_out["hydrogen_feedstock_ammonia"] = (
        df["spec", "", "total_yearly_volume"]
        * df["tech_total", "number_of_plants", "total"]
        * 0.177
        * 120
    )

    # Calculate all raw materials
    for input_type in ["Raw material", "Energy"]:
        for input in df["inputs", input_type].columns:
            if "yearly" not in input and "total" not in input:
                df_out[f"{input} {input_type} consumption"] = (
                    df["inputs", input_type, input]
                    * df["spec", "", "total_yearly_volume"]
                    * df["tech_total", "number_of_plants", "total"]
                )
                if "Hydrogen" in input and "Ammonia" in chemical:
                    df_out[f"{input} {input_type} consumption"] = (
                        df_out[f"{input} {input_type} consumption"] * 0.177 * 120
                    )

    return df_out.reset_index().drop_duplicates(subset=["region", "technology", "year"])


def calculate_opex(df):
    """Calculate yearly CAPEX spent (new build + retrofit)"""
    df_out = pd.DataFrame()

    # Calculate OPEX
    opex_new = (
        df["cost", "economics", "operations_and_maintenance"]
        * df["spec", "", "total_yearly_volume"]
        * df["tech_total", "number_of_plants", "new_build"]
    )

    opex_retrofit = (
        df["cost", "economics", "operations_and_maintenance"]
        * df["spec", "", "total_yearly_volume"]
        * df["tech_total", "number_of_plants", "retrofit"]
    )

    df_out["opex_new_build"] = opex_new
    df_out["opex_retrofit"] = opex_retrofit
    df_out["opex_yearly"] = opex_new + opex_retrofit

    df_out_tech = (
        df_out.reset_index()
        .groupby(by=["region", "technology", "year"])
        .sum()
        .reset_index()
    )

    df_out_tech = (
        df_out_tech.pivot(
            index=["region", "technology"],
            columns=["year"],
            values=["opex_new_build", "opex_retrofit", "opex_yearly"],
        )
        .fillna(0)
        .swaplevel(j=0, i=1, axis=1)
    )

    return df_out, df_out_tech


def calculate_capex(df):
    """Calculate yearly CAPEX spent (new build + retrofit)"""
    df_out = pd.DataFrame()


    # Calculate CAPEX - note that some plants are listed as new_built while they should actually be listed as retrofit.
    # They are then multiplied with the wrong (often 0) CAPEX and missed. The capex_yearly calculation below is a workaround.
    capex_new = (
        df["cost", "economics", "capex_new_build_brownfield"]
        * df["spec", "", "total_yearly_volume"]
        * df["tech_new", "number_of_plants", "new_build"]
    )

    capex_retrofit = (
        df["cost", "economics", "capex_retrofit"]
        * df["spec", "", "total_yearly_volume"]
        * df["tech_new", "number_of_plants", "retrofit"]
    )
    capex_yearly = (
        (df["cost", "economics", "capex_new_build_brownfield"] + df["cost", "economics", "capex_retrofit"])
        * df["spec", "", "total_yearly_volume"]
        * df["tech_new", "number_of_plants", "total"]
    )

    df_out["capex_new_build"] = capex_new
    df_out["capex_retrofit"] = capex_retrofit
    df_out["capex_yearly"] = capex_yearly

    df_out_tech = (
        df_out.reset_index()
        .groupby(by=["region", "technology", "year"])
        .sum()
        .reset_index()
    )

    df_out_tech = (
        df_out_tech.pivot(
            index=["region", "technology"],
            columns=["year"],
            values=["capex_new_build", "capex_retrofit", "capex_yearly"],
        )
        .fillna(0)
        .swaplevel(j=0, i=1, axis=1)
    )

    return df_out, df_out_tech


def add_weighted_average_capex(df, tech_var: bool):
    """Add weighted average CAPEX over new build and retrofit"""
    df = df.fillna(0)

    if tech_var:
        tech_var = "tech_new"
    else:
        tech_var = "tech_total"
    # Take weighted average over new build and retrofit of CAPEX
    df["cost", "economics", "capex_average"] = (
        df["cost", "economics", "capex_new_build_brownfield"]
        * df[tech_var, "capacity", "new_build"]
        + df["cost", "economics", "capex_retrofit"]
        * df[tech_var, "capacity", "retrofit"]
    ) / (
        df[tech_var, "capacity", "new_build"]
        + df[tech_var, "capacity", "retrofit"]
    )
    return df


def calculate_lcox_contribution_aggregate(df, groupby):
    # Multiply by capacity by segment (region/tech)
    df = (
        df[cost_components]
        .multiply(df["tech_total", "capacity", "total"], axis="index")
        .join(df["tech_total", "capacity", "total"])
    )

    # Aggregate by segment
    df_agg = df.groupby([groupby, "year"]).sum()

    # Divide back by aggregated capacity per segment
    df_agg = df_agg[cost_components].divide(
        df_agg["tech_total", "capacity", "total"], axis="index"
    )

    return df_agg


def calculate_weighted_average(df):
    """
    Calculate the weighted average over all region/tech combos

    Args:
        df:

    Returns:

    """
    df_agg = df.groupby("year").apply(
        lambda df_year: (
            df_year[cost_components]
            .multiply(df_year["tech_total", "capacity", "total"], axis="index")
            .sum()
            / df_year["tech_total", "capacity", "total"].sum()
        )
    )

    # Get rid of the top level index
    df_agg.columns = df_agg.columns.droplevel(0)
    return df_agg


def combine_data(df_emissions, df_ccs, df_inputs, df_capex, df_opex, df_weighted):
    return (
        df_weighted.join(
            pd.concat({"emissions": df_emissions.groupby("year").sum()}, axis=1)
        )
        .join(pd.concat({"ccs": df_ccs.groupby("year").sum()}, axis=1))
        .join(pd.concat({"inputs": df_inputs.groupby("year").sum()}, axis=1))
        .join(pd.concat({"capex": df_capex.groupby("year").sum()}, axis=1))
        .join(pd.concat({"opex": df_opex.groupby("year").sum()}, axis=1))
    )


def calculate_cumulatives(df_export):
    """Calculate cumulative sum of CCS/CAPEX/emissions"""

    cols = [
        ("ccs", "co2_captured"),
        ("capex", "capex_yearly"),
        ("emissions", "yearly_emissions_scope_1"),
        ("emissions", "yearly_emissions_scope_2"),
        ("emissions", "yearly_emissions_scope_3_upstream"),
        ("emissions", "yearly_emissions_scope_3_downstream"),
        ("emissions", "yearly_emissions_total"),
    ]

    for group, col in cols:
        df_export["cumulative", col] = df_export[group, col].cumsum()

    return df_export


def calculate_lcox_start_year(df_all_plants: pd.DataFrame, df_process: pd.DataFrame):
    """Calculate LCOX for each plant, joining on start year"""
    df_lcox = (
        df_process["cost", "lcox", "new_build_brownfield"]
        .reset_index()
        .droplevel([1, 2], axis=1)
        .rename(columns={"year": "start_year"})
        .drop(columns="origin")
        .drop_duplicates(["start_year", "region", "technology"])
    )

    # We don't have cost data prior to 2020
    df_all_plants.loc[df_all_plants.start_year < 2020, "start_year"] = 2020

    return df_all_plants.merge(df_lcox, on=["technology", "region", "start_year"])


def calculate_outputs(pathway, sensitivity, chemicals, model_scope):
    """
    Calculate derived outputs from the model run
    """
    importer = IntermediateDataImporter(
        pathway=pathway,
        sensitivity=sensitivity,
        chemicals=chemicals,
        model_scope=model_scope,
    )

    df_ethylene_fossil = _calculate_ethylene_fossil_share(importer)

    for chemical in chemicals:
        df_process = importer.get_all_process_data(chemical=chemical)
        df_tech_total = importer.get_technology_distribution(chemical=chemical)
        df_tech_new = importer.get_technology_distribution(chemical=chemical, new=True)

        df_tech_total = make_multi_df(df_tech_total, name="tech_total")
        df_tech_new = make_multi_df(df_tech_new, name="tech_new")

        df_new = pd.merge(df_tech_new, df_process, left_index=True, right_index=True)
        df_total = pd.merge(df_tech_total, df_process, left_index=True, right_index=True)

        # Convert volume back from Mt to t
        df_new["spec", "", "total_volume"] *= 1e6
        df_new["spec", "", "total_yearly_volume"] *= 1e6

        df_total["spec", "", "total_volume"] *= 1e6
        df_total["spec", "", "total_yearly_volume"] *= 1e6

        df_new = add_weighted_average_capex(df_new, tech_var=True)
        # df_total = add_weighted_average_capex(df_total, tech_var=False)

        # Calculate aggregated emissions / CCS / inputs / CAPEX
        df_emissions = calculate_emissions(df_total, df_ethylene_fossil)
        df_ccs = calculate_co2_captured(df_total)
        df_inputs = calculate_inputs(df_total, chemical)
        df_capex, df_capex_tech = calculate_capex(df_new)
        df_opex, df_opex_tech = calculate_opex(df_total)

        # Calculate LCOX contributions per region / tech
        for groupby in ["region", "technology"]:
            df_breakdown = calculate_lcox_contribution_aggregate(df=df_total, groupby=groupby)
            importer.export_data(
                df=df_breakdown,
                filename=f"lcox_breakdown_{groupby}.csv",
                export_dir=f"final/{chemical}",
            )

        # Export the non weighted LCOX breakdown per region / tech / year
        importer.export_data(
            df=df_process[cost_components],
            filename="lcox_breakdown.csv",
            export_dir=f"final/{chemical}",
        )

        # Calculate weighted averages
        df_weighted = calculate_weighted_average(df_total)

        # Combine and export
        df_export = combine_data(
            df_emissions, df_ccs, df_inputs, df_capex, df_opex, df_weighted
        )
        df_export.columns.names = ["category", "quantity"]

        # Calculate cumulative sums
        df_export = calculate_cumulatives(df_export)

        importer.export_data(
            df=df_export,
            filename="outputs_per_year.csv",
            export_dir=f"final/{chemical}",
        )

        importer.export_data(
            df=df_capex_tech,
            filename="capex_per_year.csv",
            export_dir=f"final/{chemical}",
        )

        importer.export_data(
            df=df_opex_tech,
            filename="opex_per_year.csv",
            export_dir=f"final/{chemical}",
        )

    df_all_plants = importer.get_all_plants()
    df_lcox_start_year = calculate_lcox_start_year(df_all_plants=df_all_plants, df_process=df_process)

    importer.export_data(
        df=df_lcox_start_year,
        filename="lcox_start_year.csv",
        export_dir="final/All",
    )
