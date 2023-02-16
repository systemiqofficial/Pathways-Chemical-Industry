import logging

from config import END_YEAR, LOG_LEVEL, START_YEAR
from flow.import_data.intermediate_data import IntermediateDataImporter
from flow.optimize.build_new import build_new
from flow.optimize.decommission import decommission
from flow.optimize.retrofit import retrofit
from models.decarbonization import DecarbonizationPathway
from util.util import timing

logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)


def optimize(pathway: DecarbonizationPathway):
    """
    Run the pathway simulation over the years:
        - First, decommission a fixed % of plants
        - Then, retrofit a fixed %
        - Then, build new if increasing demand
    Args:
        pathway: The decarb pathway

    Returns:
        The updated pathway
    """

    for year in range(START_YEAR, END_YEAR):
        logger.info("Optimizing for %s", year)
        pathway.update_plant_status(year=year)

        pathway = pathway.update_methanol_availability_from_stack(year=year)

        # Copy over last year's stack to this year
        pathway = pathway.copy_stack(year=year)
        # Run model for all chemicals (Methanol last as it needs MTO/A/P demand)

        for chemical in pathway.get_shuffled_chemicals():
            logger.info(chemical)

            # Decommission plants
            pathway = decommission(pathway=pathway, year=year, chemical=chemical)

            # Retrofit plants, except for business as usual scenario
            if pathway.pathway_name != "bau":
                pathway = retrofit(pathway=pathway, year=year, chemical=chemical)

            # Build new plants
            pathway = build_new(pathway=pathway, year=year, chemical=chemical)

        # Finally, re-rank MTO tech for next year, based on this year's Methanol stack
        if "Methanol" in pathway.chemicals:
            pathway = pathway.re_rank_mtx_tech(year=year)

        # Copy availability to next year
        pathway.copy_availability(year=year)

    # Update one last time to make sure end year availability/demand is right
    pathway.update_methanol_availability_from_stack(year=END_YEAR)
    pathway.get_demand(chemical="Methanol", year=END_YEAR, build_new=True)

    return pathway


@timing
def optimize_pathway(pathway, sensitivity, chemicals, model_scope):
    """
    Get data per technology, ranking data and then run the pathway simulation
    """
    importer = IntermediateDataImporter(
        pathway=pathway,
        sensitivity=sensitivity,
        chemicals=chemicals,
        model_scope=model_scope,
    )

    # Make pathway
    pathway = DecarbonizationPathway(
        pathway_name=pathway,
        chemicals=chemicals,
        start_year=START_YEAR,
        end_year=END_YEAR,
        sensitivity=sensitivity,
        model_scope=model_scope,
    )

    # Optimize plant stack on a yearly basis
    pathway = optimize(
        pathway=pathway,
    )

    # Save rankings after they have been adjusted due to MTO
    pathway.save_rankings()
    pathway.save_availability()
    pathway.save_demand()
    pathway.save_stacks()

    for chemical in chemicals:
        df_stack_total = pathway.aggregate_stacks(this_year=False, chemical=chemical)
        df_stack_new = pathway.aggregate_stacks(this_year=True, chemical=chemical)

        importer.export_data(
            df=df_stack_total,
            filename="technologies_over_time_region.csv",
            export_dir=f"final/{chemical}",
        )

        importer.export_data(
            df=df_stack_new,
            filename="technologies_over_time_region_new.csv",
            export_dir=f"final/{chemical}",
        )

        pathway.plot_stacks(df_stack_total, groupby="technology", chemical=chemical)
        pathway.plot_stacks(df_stack_total, groupby="region", chemical=chemical)

    pathway.plot_methanol_availability(df_availability=pathway.availability)
    pathway.save_transitions()

    logger.info("Pathway optimization complete")
