import pandas as pd

# Raw materials
# The categorisation (e.g. "Biomass") is to capture types that should be lumped in together for availability caps
# Refer to Master Template tab "Input Categories" for an up-to-date list
RAW_MATERIALS = {
    "Biomass": ["Wet biomass", "Dry biomass", "Bioethanol", "Bio-oils"],
    "Municipal solid waste RdF": ["Municipal solid waste RdF"],
    "Waste water": ["Waste water"],
    "Pyrolysis oil": ["Pyrolysis oil"],
    "Bio-oils": ["Bio-oils"],
    "Methanol - Green": ["Methanol - Green"],
    "Methanol - Black": ["Methanol - Black"],
}


def sum_raw_material_columns(df: pd.DataFrame) -> pd.DataFrame:
    for raw_material_category in RAW_MATERIALS:
        cols = [
            col
            for col in df["Raw material"].columns
            if col in RAW_MATERIALS[raw_material_category]
        ]

        raw_material_name = f"{raw_material_category.replace(' - ','_').replace(' ', '_').replace('-', '_').lower()}_yearly"

        df["Raw material", raw_material_name] = (
            df["Raw material"][cols].sum(axis="columns")
            * df["spec", "total_yearly_volume"]
            * 1e6
        )

    return df


def calculate_input_totals(
    df_inputs: pd.DataFrame, df_spec: pd.DataFrame
) -> pd.DataFrame:
    """
    Calculate totals for inputs

    Args:
        df_inputs: Inputs df
        df_spec: df with plant specs

    Returns:
        df with inputs totals
    """
    df = df_inputs.join(pd.concat({"spec": df_spec}, axis=1))

    df = sum_raw_material_columns(df)

    return df


def calculate_ccs(df_emissions: pd.DataFrame, df_spec: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate total CCS for a tech
    Args:
        df_emissions: df with emissions data
        df_spec: df with specification data per plant

    Returns:
        df with total CCS per tech
    """
    df = df_emissions.join(df_spec[["total_volume", "total_yearly_volume"]])

    df["ccs_total"] = df["ccs_capacity"] * df["total_volume"] * 1e6
    df["ccs_yearly"] = df["ccs_capacity"] * df["total_yearly_volume"] * 1e6

    return df
