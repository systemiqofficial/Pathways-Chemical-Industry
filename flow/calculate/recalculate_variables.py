import logging
import math

import pandas as pd

from config import METHANOL_DEMAND_TECH, METHANOL_TYPES
from flow.calculate.calculate_additional_variables import calculate_ccs
from flow.calculate.calculate_cost import calculate_cost
from flow.calculate.calculate_emissions import calculate_emissions_aggregate
from flow.calculate.calculate_tco import calculate_tco
from flow.import_data.intermediate_data import IntermediateDataImporter

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


def _add_all_scopes_for_methanol(df_emission_factors):
    """
    Add all emissions scopes for methanol black; pre-fill with 0.
    To be overwritten based on methanol tech emissivity
    """

    df_template = df_emission_factors.query(
        "name == 'Methanol - Black' and scope == '3_upstream'"
    )

    # Only use scope 1 for now; possibility to add '2', '3_downstream'
    other_scopes = {"1"}

    for scope in other_scopes:
        df_copy = df_template.copy()
        df_copy["scope"] = scope
        df_copy["emission_factor"] = 0
        df_emission_factors = df_emission_factors.append(df_copy)
    return df_emission_factors


def recalculate_emissions(
    df_emission_factors: pd.DataFrame,
    df_emissions_shares: pd.DataFrame,
    df_ccs_rate: pd.DataFrame,
    df_inputs: pd.DataFrame,
    df_spec: pd.DataFrame,
    dict_emission: dict,
):
    """Update or add emission factors for Methanol as a fuel for other chemicals"""

    df_emission_factors = _add_all_scopes_for_methanol(df_emission_factors)

    for methanol_type in METHANOL_TYPES:
        for scope in dict_emission[methanol_type].keys():
            if not math.isnan(dict_emission[methanol_type][scope]):
                df_emission_factors.loc[
                    (df_emission_factors.name == methanol_type)
                    & ("scope_" + df_emission_factors.scope == scope),
                    "emission_factor",
                ] = dict_emission[methanol_type][scope]

    df_emissions = calculate_emissions_aggregate(
        df_inputs=df_inputs,
        df_emission_factors=df_emission_factors,
        df_ccs_rate=df_ccs_rate,
        df_emissions_shares=df_emissions_shares,
    )
    df_emissions = calculate_ccs(df_emissions=df_emissions, df_spec=df_spec)

    return df_emissions


def recalculate_costs(
    df_emissions: pd.DataFrame,
    df_inputs: pd.DataFrame,
    df_ccs_price: pd.DataFrame,
    df_carbon_price: pd.DataFrame,
    df_input_price: pd.DataFrame,
    df_economics: pd.DataFrame,
    dict_lcox: dict,
):
    dict_lcox = {
        key: value for (key, value) in dict_lcox.items() if not math.isnan(value)
    }
    # Calculate input costs
    for methanol_type, value_ in dict_lcox.items():
        df_input_price.loc[
            (df_input_price.name == methanol_type), "input_price"
        ] = value_

    return calculate_cost(
        df_emissions=df_emissions,
        df_inputs=df_inputs,
        df_ccs_price=df_ccs_price,
        df_carbon_price=df_carbon_price,
        df_input_price=df_input_price,
        df_economics=df_economics,
    )


def recalculate_variables(
    year: int, dict_lcox: dict, dict_emission: dict, importer: IntermediateDataImporter
):
    """Recalculate cost and emissions variables for Methanol demand tech"""

    df_inputs = importer.get_inputs()
    df_inputs = df_inputs[(df_inputs.technology.isin(METHANOL_DEMAND_TECH))]

    df_spec = importer.get_plant_specs()
    df_spec = df_spec.query(f"technology.isin({METHANOL_DEMAND_TECH})")

    df_emission_factors = importer.get_emissions_factors()

    df_emissions_shares = importer.get_emissions_shares()

    df_ccs_rate = importer.get_ccs_rate()
    df_ccs_rate = df_ccs_rate[df_ccs_rate.technology.isin(METHANOL_DEMAND_TECH)]

    df_ccs_price = importer.get_ccs_price()

    df_carbon_price = importer.get_carbon_price()

    df_input_price = importer.get_input_price()

    df_economics = importer.get_process_economics()
    df_economics = df_economics.query(f"technology.isin({METHANOL_DEMAND_TECH})")

    df_emissions = recalculate_emissions(
        df_emission_factors=df_emission_factors,
        df_emissions_shares=df_emissions_shares,
        df_ccs_rate=df_ccs_rate,
        df_inputs=df_inputs,
        df_spec=df_spec,
        dict_emission=dict_emission,
    )

    df_cost = recalculate_costs(
        df_emissions=df_emissions,
        df_inputs=df_inputs,
        df_ccs_price=df_ccs_price,
        df_carbon_price=df_carbon_price,
        df_input_price=df_input_price,
        df_economics=df_economics,
        dict_lcox=dict_lcox,
    )

    df_tco = calculate_tco(df_cost=df_cost, df_spec=df_spec, year=year)

    return df_emissions, df_tco
