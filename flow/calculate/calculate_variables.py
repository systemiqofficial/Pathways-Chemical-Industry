import logging

from flow.calculate.calculate_additional_variables import (
    calculate_ccs, calculate_input_totals)
from flow.calculate.calculate_cost import calculate_cost
from flow.calculate.calculate_emissions import calculate_emissions_aggregate
from flow.calculate.calculate_tco import calculate_tco
from flow.calculate.pivot_inputs import pivot_inputs
from flow.import_data.intermediate_data import IntermediateDataImporter
from util.util import timing

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


def validate_notnull(df):
    assert not df.isnull().values.any()


@timing
def calculate_variables(**kwargs):
    """
    Calculate variables per process/region/year based on Excel input file:
    - inputs
    - emissions (scope 1,2,3)
    - cost (+TCO/LCOX)
    """
    importer = IntermediateDataImporter(**kwargs)

    # Get inputs per process and export
    df_inputs = importer.get_inputs()
    df_spec = importer.get_plant_specs()
    df_inputs_pivot = pivot_inputs(df_inputs, values="input")
    df_inputs_pivot = calculate_input_totals(df_inputs=df_inputs_pivot, df_spec=df_spec)
    validate_notnull(df_inputs_pivot)
    importer.export_data(
        df=df_inputs_pivot, filename="inputs_pivot.csv", export_dir="intermediate"
    )

    df_emission_factors = importer.get_emissions_factors()

    df_emissions_shares = importer.get_emissions_shares().iloc[:, 1:]
    df_emissions_shares.replace({"Dry Biomass": "Dry biomass"}, inplace=True)

    df_ccs_rate = importer.get_ccs_rate()
    df_ccs_price = importer.get_ccs_price()
    df_carbon_price = importer.get_carbon_price()
    df_input_price = importer.get_input_price()
    df_economics = importer.get_process_economics()

    # Calculate emissions per process
    df_emissions = calculate_emissions_aggregate(
        df_inputs=df_inputs,
        df_emission_factors=df_emission_factors,
        df_ccs_rate=df_ccs_rate,
        df_emissions_shares=df_emissions_shares,
    )
    df_emissions = calculate_ccs(df_emissions=df_emissions, df_spec=df_spec)
    # validate_notnull(df_emissions)
    importer.export_data(
        df=df_emissions, filename="emissions.csv", export_dir="intermediate"
    )

    # Calculate input costs
    df_cost = calculate_cost(
        df_emissions=df_emissions,
        df_inputs=df_inputs,
        df_ccs_price=df_ccs_price,
        df_carbon_price=df_carbon_price,
        df_input_price=df_input_price,
        df_economics=df_economics,
    )

    # Calculate TCO per process and year
    df_tco = calculate_tco(df_cost=df_cost, df_spec=df_spec)

    importer.export_data(df=df_tco, filename="cost.csv", export_dir="intermediate")
