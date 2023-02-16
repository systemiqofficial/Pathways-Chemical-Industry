import datetime
import logging
import os
import shutil

import pandas as pd

from config import END_YEAR, LOG_LEVEL, START_YEAR
from flow.import_data.intermediate_data import IntermediateDataImporter

logger = logging.getLogger(name=__name__)
logger.setLevel(LOG_LEVEL)

category_naming_dict = {
    "demand": "vol_dem_global",
    "emission": "emissions",
    "tech": "vol_prod_tech",
    "technology": "vol_prod_technology",
    "category_technology": "vol_prod_cattech",
    "category_abatement": "vol_prod_catabat",
    "category_feedstock": "vol_prod_catfeed",
    "category_energy": "vol_prod_catener",
    "category_ccs": "vol_prod_catccs",
    "category_detailed": "vol_prod_catdet",
    "type_of_tech": "vol_prod_techtype",
}

agg_naming_dict = {
    "cumulative": "cum",
    "emissions": "inc",
    "capex": "inc",
    "opex": "inc",
    "ccs": "inc",
}

quant_naming_dict = {
    "yearly_emissions_total": "yearly",
    "yearly_emissions_scope_1": "yearly1",
    "yearly_emissions_scope_2": "yearly2",
    "yearly_emissions_scope_3_upstream": "yearly3u",
    "yearly_emissions_scope_3_downstream": "yearly3d",
    "new_build_brownfield": "lcoxavg",
    "co2_captured": "co2cap",
}

general_dict = {
    "capex_new_build": "capex_nb",
    "capex_retrofit": "capex_ret",
    "opex_new_build": "opex_nb",
    "opex_retrofit": "opex_ret",
}

agg_naming_dict.update(general_dict)
quant_naming_dict.update(general_dict)
category_naming_dict.update(general_dict)

CUSTOM_REGION_SORT = {
    "North America": 0,
    "Latin America": 1,
    "Europe": 2,
    "Russia": 3,
    "Africa": 4,
    "Middle East": 5,
    "China": 6,
    "India": 7,
    "Japan": 8,
    "Rest of Asia and Pacific": 9,
}

CUSTOM_CHEMICAL_SORT = {
    "Ammonia": 0,
    "Ammonium Nitrate": 1,
    "Urea": 2,
    "Methanol": 3,
    "Ethylene": 4,
    "Propylene": 5,
    "Butadiene": 6,
    "Benzene": 7,
    "Toluene": 8,
    "Xylene": 9,
    "BTX": 10,
}


ALL_PATHWAYS = {"me", "fa", "nf", "nfs", "bau"}

ALL_SENSITIVITIES = {"def", "bdem", "ldem", "lfos", "ccs"}

ALL_CHEMICALS = {
    "Ammonia",
    "Urea",
    "Ammonium Nitrate",
    "Ethylene",
    "Propylene",
    "Butadiene",
    "Benzene",
    "Toluene",
    "Xylene",
    "Methanol",
}


def create_dataframe(
    chemical: bool = True, region: bool = False, tech: bool = False, **kwargs
):
    """
    Create an empty dataframe with the right index and the order.

    Args:
        chemical: boolean to add as an index
        region: boolean to add as an index
        tech: boolean to add as an index
        **kwargs: pathway and sensitivity variable to import the tech dataframe

    Returns: Empty dataframe with the right index

    """
    iterables = []
    index_names = []

    if chemical:
        chemical_list = [
            "Ammonia",
            "Ammonium Nitrate",
            "Urea",
            "Methanol",
            "Ethylene",
            "Propylene",
            "Butadiene",
            "Benzene",
            "Toluene",
            "Xylene",
            "BTX",
        ]
        iterables.append(chemical_list)
        index_names.append("chemical")

    if region:
        region_list = [
            "North America",
            "Latin America",
            "Europe",
            "Russia",
            "Africa",
            "Middle East",
            "China",
            "India",
            "Japan",
            "Rest of Asia and Pacific",
        ]
        iterables.append(region_list)
        index_names.append("region")

    if kwargs:
        iterables += list(kwargs.values())
        index_names += list(kwargs)

    if tech:
        # simplify this
        for key, value in kwargs.items():
            if key == "pathway":
                pathway = value
            elif key == "sensitivity":
                sensitivity = value
        dl = IntermediateDataImporter(pathway=pathway, sensitivity=sensitivity)

        df_tech = dl.get_tech()
        df_tech["empty"] = ""
        tech_columns = df_tech.columns[
            ~df_tech.columns.str.contains("available")
            & ~df_tech.columns.str.contains("chemical")
        ]

        df_tech_final = pd.DataFrame()
        for col in tech_columns:
            if col == "technology":
                tech_col = ["chemical", col, "empty"]
            else:
                tech_col = ["chemical", col, "technology"]

            df_tech_set = (
                df_tech[tech_col]
                .set_index(tech_col)
                .sort_values(by=["chemical"], key=sort_func)
            )
            df_tech_set["tech_category"] = col

            df_tech_final = df_tech_final.append(df_tech_set)

            if region:
                iterables.remove(pathway)
                iterables.remove(sensitivity)
                df_reg_tech = (
                    df_tech_final[df_tech_final.tech_category == "technology"]
                    .reset_index()
                    .drop(columns=["empty", "tech_category"])
                )
                df_reg_chem = pd.DataFrame(
                    index=pd.MultiIndex.from_product(
                        iterables, names=["chemical", "region"]
                    )
                ).reset_index()
                df_chem_reg_tech = df_reg_chem.merge(df_reg_tech, on="chemical")
                df_chem_reg_tech = df_chem_reg_tech.set_index(
                    ["chemical", "region", "technology"]
                ).sort_values(by=["chemical", "region"], key=sort_func)
                return df_chem_reg_tech

        return df_tech_final

    return pd.DataFrame(index=pd.MultiIndex.from_product(iterables, names=index_names))


def append_outputs(
    df_empty: pd.DataFrame,
    file_path: str,
    pathway: str = None,
    sensitivity: str = None,
    index_col=[0, 1, 2],
):
    df_file_path = pd.read_csv(file_path, index_col=index_col, header=[0]).fillna(0)
    if pathway is not None:
        df_file_path["pathway"] = pathway
        df_file_path.set_index("pathway", append=True, inplace=True)
    if sensitivity is not None:
        df_file_path["sensitivty"] = sensitivity
        df_file_path.set_index("sensitivity", append=True, inplace=True)
    return df_empty.append(df_file_path)


def sort_func(x):
    if x.name == "region":
        return x.map(CUSTOM_REGION_SORT)
    elif x.name == "chemical":
        return x.map(CUSTOM_CHEMICAL_SORT)
    else:
        return x


def create_sheet_name(pathway, sensitivity, category, aggregation=None, quantity=None):
    """
    Creating specific sheet names for the xlxs document

    Args:
        pathway: str - name
        sensitivity: str - name
        category: str - name
        quantity: str - name

    Returns:
        sheetname: str

    """
    category = (
        category_naming_dict[category]
        if category in category_naming_dict.keys()
        else category
    )
    if aggregation is not None:
        aggregation = (
            agg_naming_dict[aggregation]
            if aggregation in agg_naming_dict.keys()
            else aggregation
        )
    if quantity is not None:
        quantity = (
            quant_naming_dict[quantity]
            if quantity in quant_naming_dict.keys()
            else quantity
        )

    sheet_name = "_".join(
        [
            name
            for name in [pathway, sensitivity, category, aggregation, quantity]
            if name is not None
        ]
    )

    return sheet_name.replace(" ", "").replace("__", "_").lower()


def save_outputs_xlsx(
    file_path: str,
    category: str,
    pathway: str,
    sensitivity: str,
    writer,
    index_col,
    empty_join=False,
):
    """
    Save specifics outputs in on xlsx file

    Args:
        file_path:
        category:
        pathway:
        sensitivity:
        writer:
        index_col:
        empty_join:

    Returns:

    """
    header = [0, 1] if category in {"capex", "opex"} else [0]
    df_file_path = pd.read_csv(file_path, index_col=index_col, header=header).fillna(0)

    if category in {"emission", "cost"}:
        keys = list(
            set(
                zip(
                    df_file_path.index.get_level_values(1).to_list(),
                    df_file_path.index.get_level_values(2).to_list(),
                )
            )
        )
        for aggregation, quantity in keys:
            sheet_name = create_sheet_name(
                pathway=pathway,
                sensitivity=sensitivity,
                category=category,
                aggregation=aggregation,
                quantity=quantity,
            )
            # sheet_name = "_".join([NAME_DICT.get(string, string) for string in sheet_name.split("_")])
            df = df_file_path.query(
                f"quantity == {[quantity]} & category == {[aggregation]}"
            )
            df_chem = create_dataframe(
                category=[aggregation], quantity=[quantity]
            ).join(df)
            df_chem.to_excel(writer, sheet_name=sheet_name)

    elif category in {"capex", "opex"}:
        for category in df_file_path.columns.get_level_values(None).unique():
            df = df_file_path.loc[
                :, df_file_path.columns.get_level_values(None) == category
            ].droplevel(None, axis=1)

            df_pex = create_dataframe(
                chemical=True,
                region=True,
                tech=True,
                pathway=pathway,
                sensitivity=sensitivity,
            )
            df_pex = df_pex.join(df).fillna(0)

            sheet_name = create_sheet_name(
                pathway=pathway,
                sensitivity=sensitivity,
                category=category,
                quantity="tech_reg",
            )
            df_pex.to_excel(writer, sheet_name=sheet_name)

            df_pex = (
                df_pex.groupby(by=["chemical", "technology"])
                .sum()
                .sort_values(by=["chemical", "technology"], key=sort_func)
            )

            sheet_name = create_sheet_name(
                pathway=pathway,
                sensitivity=sensitivity,
                category=category,
                quantity="tech",
            )
            df_pex.to_excel(writer, sheet_name=sheet_name)

    elif category == "inputs":
        for aggregation in ["Raw material", "Energy"]:
            sheet_name = create_sheet_name(
                pathway=pathway,
                sensitivity=sensitivity,
                category=category,
                aggregation=aggregation,
            )
            df = df_file_path.query("quantity.str.contains(@aggregation)")
            df = create_dataframe(
                category=[category],
                quantity=df.index.get_level_values(2).unique().to_list(),
            ).join(df)
            df.to_excel(writer, sheet_name=sheet_name)

            df_swap = df.swaplevel(
                0,
                -1,
            ).sort_values(by=["quantity", "chemical"], key=sort_func)
            df_swap.to_excel(
                writer,
                sheet_name=f"{pathway}_{sensitivity}_{aggregation.replace(' ','').lower()}_chem",
            )

    elif category == "region":
        df = create_dataframe(region=True).join(df_file_path)

        df.to_excel(writer, sheet_name=f"{pathway}_{sensitivity}_vol_prod_chem_reg")
        # swap region and chemical
        df_swap = (
            create_dataframe(region=True)
            .join(df_file_path)
            .reset_index()
            .sort_values(by=["region", "chemical"], key=sort_func)
            .set_index(["region", "chemical"])
        )
        df_swap.to_excel(
            writer, sheet_name=f"{pathway}_{sensitivity}_vol_prod_reg_chem"
        )

        # create production output
        df_production = df_file_path.groupby(by="chemical").sum()
        df_production = (
            create_dataframe().reset_index().set_index("chemical").join(df_production)
        )
        df_production.to_excel(
            writer, sheet_name=f"{pathway}_{sensitivity}_vol_prod_global"
        )

    elif category == "tech":
        # create for various technology functions
        df_tech = create_dataframe(tech=True, pathway=pathway, sensitivity=sensitivity)
        tech_cat_list = list(df_tech.tech_category.unique())
        tech_cat_list.remove("empty")
        for tech_category in tech_cat_list:
            df_tech_sub = df_tech[df_tech.tech_category == tech_category]
            df_tech_sub = df_tech_sub.drop(columns=["tech_category"])
            if tech_category == "technology":
                df_tech_sub = df_tech_sub.reset_index(level=2)
                df_tech_sub = df_tech_sub.join(df_file_path)
                df_tech_sub = df_tech_sub.iloc[:, 1:]
            else:
                df_tech_sub.index.names = ["chemical", tech_category, "technology"]
                df_tech_sub = df_tech_sub.join(df_file_path).fillna(0)
                df_tech_sub = (
                    df_tech_sub.groupby(["chemical", tech_category])
                    .sum()
                    .sort_values(by=["chemical", tech_category], key=sort_func)
                )
                # df_tech_sub = df_tech_sub.set_index("technology", append=True)

            df_tech_sub.to_excel(
                writer,
                sheet_name=create_sheet_name(
                    pathway=pathway,
                    sensitivity=sensitivity,
                    category=tech_category,
                ),
            )

    elif len(index_col) == 1:
        df = create_dataframe().reset_index().set_index("chemical").join(df_file_path)
        df.to_excel(
            writer,
            sheet_name=create_sheet_name(
                pathway=pathway, sensitivity=sensitivity, category=category
            ),
        )
    else:
        df = create_dataframe().join(df_file_path)
        df.to_excel(
            writer,
            sheet_name=create_sheet_name(
                pathway=pathway, sensitivity=sensitivity, category=category
            ),
        )


def _merge_wedge_charts(sensitivity_list: dict, cwd: str, model_scope: str):
    """
    Merge HTML wedge charts to be in the Aggregated folder

    Args:
        pathway_list:
        sensitivity_list:
        cwd:

    Returns:

    """

    for pathway, sensitivities in sensitivity_list.items():
        for sensitivity in sensitivities:
            final_path = os.path.join(
                cwd, "output", model_scope, pathway, sensitivity, "final"
            )
            chemicals = os.listdir(final_path)
            chemicals.remove("All")
            for chemical in chemicals:
                for extension in ["html", "png"]:
                    for wedge_type in "technology", "region":
                        old_wedge_path = os.path.join(
                            final_path, chemical, f"{wedge_type}_over_time.{extension}"
                        )
                        new_wedge_dir = os.path.join(
                            cwd,
                            "output",
                            model_scope,
                            "Aggregated",
                            "Wedges",
                            extension,
                        )
                        os.makedirs(new_wedge_dir, exist_ok=True)

                        new_wedge_name = f"{wedge_type}_over_time_{chemical}_{pathway}_{sensitivity}.{extension}"
                        new_wedge_path = os.path.join(new_wedge_dir, new_wedge_name)
                        try:
                            shutil.copy(old_wedge_path, new_wedge_path)
                        except FileNotFoundError:
                            logger.warning(f"Did not find {new_wedge_name}, skipping")


def merge_outputs(model_scope, chemicals):
    """
    Merge outputs for different pathway/sensitivity runs

    """

    # Setting directories
    agg_output_dir = f"output/{model_scope}/Aggregated"
    if not os.path.exists(os.path.join(os.getcwd(), agg_output_dir)):
        os.makedirs(os.path.join(os.getcwd(), agg_output_dir))

    pathway_list = list(
        ALL_PATHWAYS
        & set(list(set([x for x in os.walk(f"output/{model_scope}")][0][1])))
    )
    sensitivity_list = {
        pathway: list(
            set([x for x in os.walk(f"output/{model_scope}/{pathway}")][0][1])
        )
        for pathway in pathway_list
    }
    chemical_list = {}
    for pathway, sensitivities in sensitivity_list.items():
        chemical_list[pathway] = {}
        for sensitivity in sensitivities:
            chemical_list[pathway][sensitivity] = list(
                set(
                    [
                        x
                        for x in os.walk(
                            f"output/{model_scope}/{pathway}/{sensitivity}/ranking"
                        )
                    ][0][1]
                )
            )

    # Initialize excel writer
    cwd = os.path.abspath(os.getcwd())

    _merge_wedge_charts(
        sensitivity_list=sensitivity_list, cwd=cwd, model_scope=model_scope
    )

    # Initialize excel writer

    today_date = datetime.date.today().strftime("%Y%m%d")
    writer = pd.ExcelWriter(
        cwd + f"/output/{model_scope}/Aggregated/{today_date}_dashboard_output.xlsx"
    )

    # Loop across all calculated outputs emissions, costs, inputs and demand
    for category in ["emission", "cost", "inputs", "demand", "capex", "opex"]:
        df_output = pd.DataFrame()
        for pathway in pathway_list:
            for sensitivity in sensitivity_list[pathway]:
                dl = IntermediateDataImporter(
                    pathway=pathway,
                    sensitivity=sensitivity,
                    model_scope=model_scope,
                    chemicals=chemicals,
                )
                file_path = dl.export_dir.joinpath(
                    "final", "All", f"{category}_output.csv"
                )
                index_col = [0] if category == "demand" else [0, 1, 2]
                df_output = append_outputs(
                    df_empty=df_output,
                    file_path=file_path,
                    pathway=pathway,
                    index_col=index_col,
                )
                # Merge all the aggregated outputs for each pathway into one file
                save_outputs_xlsx(
                    file_path=file_path,
                    category=category,
                    pathway=pathway,
                    sensitivity=sensitivity,
                    writer=writer,
                    index_col=index_col,
                )

        if category in ["emission", "cost", "inputs"]:
            df_output = df_output.reorder_levels(
                ["pathway", "chemical", "category", "quantity"], axis=0
            )
            df_output = df_output.sort_index(
                level=["chemical", "pathway", "category"],
                ascending=[True, True, False],
                axis=0,
            )

        dl.export_data(
            df=df_output,
            filename=f"{category}_output.csv",
            export_dir=f"{model_scope}/Aggregated",
            aggregate=True,
        )

    # Loop across all calculated region and tech
    for category in ["region", "tech"]:
        for pathway in pathway_list:
            for sensitivity in sensitivity_list[pathway]:
                dl = IntermediateDataImporter(pathway=pathway, sensitivity=sensitivity)
                file_path = dl.export_dir.joinpath(
                    "final", "All", f"all_chemical_{category}.csv"
                )
                # Merge all the aggregated outputs for each pathway into one file
                save_outputs_xlsx(
                    file_path=file_path,
                    category=category,
                    pathway=pathway,
                    sensitivity=sensitivity,
                    writer=writer,
                    index_col=[0, 1],
                    empty_join=True,
                )

    # Loop across the ranking file to output
    for pathway in pathway_list:
        for sensitivity in sensitivity_list[pathway]:
            dl = IntermediateDataImporter(pathway=pathway, sensitivity=sensitivity)
            for variable, folder, file_name in [
                ("lcox", "ranking", "new_build_post_rank"),
                ("scope_1", "intermediate", "emissions"),
            ]:
                df = pd.DataFrame()
                if folder == "ranking":
                    for chemical in chemical_list[pathway][sensitivity]:
                        file_path = dl.export_dir.joinpath(
                            folder, chemical, f"{file_name}.csv"
                        )
                        df = append_outputs(
                            df_empty=df, file_path=file_path, index_col=[0, 1]
                        )
                    df = df.reset_index()
                    tech_column = "destination"

                else:
                    file_path = dl.export_dir.joinpath(folder, f"{file_name}.csv")
                    df = append_outputs(
                        df_empty=df, file_path=file_path, index_col=[0, 1]
                    )
                    df = df.reset_index()
                    tech_column = "technology"

                df = df.pivot(
                    index=["chemical", tech_column, "region"],
                    columns="year",
                    values=variable,
                )
                df = df.sort_values(
                    by=["chemical", tech_column, "region"], key=sort_func
                )
                df_tech = (
                    df.groupby(by=["chemical", tech_column])
                    .mean()
                    .sort_values(by=["chemical", tech_column], key=sort_func)
                )

                df_reg = (
                    df.groupby(by=["chemical", "region"])
                    .mean()
                    .sort_values(by=["chemical", "region"], key=sort_func)
                )

                df_chem = (
                    df.groupby(by=["chemical"])
                    .mean()
                    .sort_values(by=["chemical"], key=sort_func)
                )

                df.to_excel(
                    writer,
                    sheet_name=create_sheet_name(
                        pathway=pathway,
                        sensitivity=sensitivity,
                        category=f"{variable}_tech_reg",
                    ),
                )
                df_tech.to_excel(
                    writer,
                    sheet_name=create_sheet_name(
                        pathway=pathway,
                        sensitivity=sensitivity,
                        category=f"{variable}_tech",
                    ),
                )
                df_reg.to_excel(
                    writer,
                    sheet_name=create_sheet_name(
                        pathway=pathway,
                        sensitivity=sensitivity,
                        category=f"{variable}_reg",
                    ),
                )
                df_chem.to_excel(
                    writer,
                    sheet_name=create_sheet_name(
                        pathway=pathway,
                        sensitivity=sensitivity,
                        category=f"{variable}_chem",
                    ),
                )

    writer.save()

    df_empty = pd.DataFrame()
    for pathway in pathway_list:
        for sensitivity in sensitivity_list[pathway]:
            dl = IntermediateDataImporter(pathway=pathway, sensitivity=sensitivity)
            file_path = dl.export_dir.joinpath(
                "final", "All", "all_chemical_region.csv"
            )
            df_region = append_outputs(
                df_empty=df_empty,
                file_path=file_path,
                pathway=pathway,
                index_col=[0, 1],
            )
    df_region = df_region.reorder_levels(["pathway", "chemical", "region"], axis=0)

    # Transpose dataframe into the right format
    df_region = df_region.stack().unstack([0, -1])

    # Drop not needed columns
    nondrop_list = [x for x in df_region.columns if x[1] == str(END_YEAR)] + [
        (pathway_list[0], str(START_YEAR))
    ]
    df_region.drop(df_region.columns.difference(nondrop_list), axis=1, inplace=True)

    # Rename column index
    df_region.columns = df_region.columns.to_flat_index()
    df_region = df_region.rename(
        columns={(pathway_list[0], str(START_YEAR)): ("Baseline", str(START_YEAR))}
    )

    dl.export_data(
        df=df_region,
        filename="region.csv",
        export_dir=f"{model_scope}/Aggregated",
        aggregate=True,
    )
