from pathlib import Path

import pandas as pd

from config import CHEMICALS, MODEL_SCOPE

RENAME_COLS = {
    "Scope": "scope",
    "Name": "name",
    "Region": "region",
    "Value": "emissions",
    "Chemical": "chemical",
    "Process": "technology",
    "Sub-process": "component",
    "Category": "category",
    "Category technology": "category_technology",
    "Category abatement": "category_abatement",
    "Category feedstock": "category_feedstock",
    "Category Energy": "category_energy",
    "Category CCS": "category_ccs",
    "Category detailed": "category_detailed",
    "Note": "note",
    "Source": "source",
    "Non-time-bound variable": "non_time_bound_variable",
    "Unit": "unit",
    "CAPEX - new build brownfield": "capex_new_build_brownfield",
    "CAPEX - retrofit": "capex_retrofit",
    "O&M": "operations_and_maintenance",
    "Plant lifetime": "plant_lifetime",
    "Assumed plant capacity": "assumed_plant_capacity",
    "Technology": "technology",
    "Available from": "available_from",
    "Available until": "available_until",
    "Origin": "origin",
    "Destination": "destination",
    "Capacity factor": "capacity_factor",
    "Python Import": "python_import",
    "Type of tech": "type_of_tech",
    "Sensitivity": "sensitivity",
    "Primary chemical": "primary_chemical",
    "Pathway": "pathway",
}

# Use these columns to import from Excel
USECOLS = {
    "emissions_share": "A:BS",
    "emissions": "A:BR",
    "prices": "A:BR",
    "chemical": "A:BT",
    "decommission": "A:AF",
}


class BaseImporter:
    """Base class to import data from the Master Template Excel"""

    def __init__(
        self,
        pathway,
        sensitivity,
        model_scope=MODEL_SCOPE,
        chemicals=CHEMICALS,
        rename_cols=RENAME_COLS,
    ):
        parent_path = Path(__file__).resolve().parents[2]
        if model_scope=="Japan":
            self.input_path = parent_path.joinpath(
                "data/Master template Japan - python copy.xlsx"
            )
        if model_scope=="World":
            self.input_path = parent_path.joinpath(
                "data/Master template - python copy.xlsx"
            )
        self.pathway = pathway
        self.sensitivity = sensitivity
        self.chemicals = chemicals
        self.export_dir = parent_path.joinpath(
            "output", model_scope, pathway, sensitivity
        )
        self.aggregate_export_dir = parent_path.joinpath("output/")
        self.rename_cols = rename_cols

    def _get_excel(
        self,
        sheet_name: str,
        usecols: str,
        unit: str = None,
        categories: list = None,
        name: str = None,
    ):
        """
        Get an excel sheet from the Master Template

        Args:
            sheet_name: which sheet to get the data from
            usecols: use these columns in the excel
            unit: filter for this unit
            categories: filter for these categories
            name: filter for this name

        Returns:
            Dataframe with results
        """
        df = pd.read_excel(
            self.input_path, sheet_name=sheet_name, usecols=usecols
        ).rename(columns=self.rename_cols)

        if "python_import" in df.columns:
            df = df[df["python_import"].astype(bool)].drop(columns="python_import")

        if unit is not None:
            df = df[df.unit == unit]

        if categories is not None:
            df = df[df.category.isin(categories)]

        if name is not None:
            df = df[df.name == name]

        return df

    def get_time_vars(
        self,
        sheet_name: str,
        id_vars: list,
        data_type: str,
        value_name: str = "value",
        year: int = None,
        **kwargs
    ):
        """
        Get time based variables from the master template, pivot from wide to long format

        Args:
            sheet_name: which sheet to get data from
            id_vars: identifier variables (that uniquely identify a row)
            data_type: which data type: e.g. emissions/prices
            value_name: name of the value in the resulting dataframe
            year: get only data fot this year

        Returns:
            The requested data, in long format
        """
        years = range(2020, 2051) if data_type == "decommission" else range(2020, 2081)
        usecols = USECOLS[data_type]

        if data_type == "decommission":
            keep_cols = id_vars + list(years)
        else:
            keep_cols = id_vars + [str(year) for year in years]

        df = (
            self._get_excel(sheet_name=sheet_name, usecols=usecols, **kwargs)[keep_cols]
            .melt(id_vars=id_vars, var_name="year", value_name=value_name)
            .astype({"year": int})
        )

        if year is not None:
            df = df[df.year == year]

        return df

    def export_data(
        self, df: pd.DataFrame, filename: str, export_dir: str, aggregate=False
    ):
        """
        Export output data into the output directory

        Args:
            aggregate:
            df: Data to export
            filename: Filename to export to
            export_dir: Additional directory to create
        """
        output_dir = self.export_dir if not aggregate else self.aggregate_export_dir
        if export_dir is not None:
            output_dir = output_dir.joinpath(export_dir)
        else:
            output_dir = output_dir

        # Make export directory if it doesn't exist yet
        output_dir.mkdir(exist_ok=True, parents=True)

        export_path = output_dir.joinpath(filename)
        df.to_csv(export_path)
