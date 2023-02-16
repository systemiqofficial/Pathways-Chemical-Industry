import itertools
import logging
import multiprocessing as mp
import random

import numpy as np

from config import (CHEMICALS, LOG_LEVEL, MODEL_SCOPE, PATHWAYS, RUN_PARALLEL,
                    SENSITIVITIES, run_config)
from export.export_outputs import export_outputs
from export.merge_outputs import merge_outputs
from flow.calculate.calculate_outputs import calculate_outputs
from flow.calculate.calculate_variables import calculate_variables
from flow.import_data.all import import_data
from flow.optimize.optimize import optimize_pathway
from flow.rank.rank_technologies import make_rankings

logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)

np.random.seed(100)
random.seed(100)


funcs = {
    "IMPORT_DATA": import_data,
    "CALCULATE_VARIABLES": calculate_variables,
    "MAKE_RANKINGS": make_rankings,
    "OPTIMIZE_PATHWAY": optimize_pathway,
    "CALCULATE_OUTPUTS": calculate_outputs,
    "EXPORT_OUTPUTS": export_outputs,
}


def _run_model(pathway, sensitivity):
    for name, func in funcs.items():
        if name in run_config:
            logger.info(
                f"Running pathway {pathway} sensitivity {sensitivity} section {name}"
            )
            japan_chemicals = [
                chemical
                for chemical in CHEMICALS
                if chemical
                not in [
                    "Ammonia",
                    "Urea",
                    "Ammonium Nitrate",
                ]
            ]
            func(
                pathway=pathway,
                sensitivity=sensitivity,
                chemicals=CHEMICALS if MODEL_SCOPE == "World" else japan_chemicals,
                model_scope=MODEL_SCOPE,
            )


def run_model_sequential(runs):
    """Run model sequentially, slower but better for debugging"""
    for pathway, sensitivity in runs:
        _run_model(pathway=pathway, sensitivity=sensitivity)


def run_model_parallel(runs):
    """Run model in parallel, faster but harder to debug"""
    n_cores = mp.cpu_count()
    logger.info(f"{n_cores} cores detected")
    pool = mp.Pool(processes=n_cores)

    logger.info(f"Running model for scenario/sensitivity {runs}")
    for pathway, sensitivity in runs:
        pool.apply_async(_run_model, args=(pathway, sensitivity))
    pool.close()
    pool.join()


def main():
    runs = list(itertools.product(PATHWAYS, SENSITIVITIES))
    if RUN_PARALLEL:
        run_model_parallel(runs)
    else:
        run_model_sequential(runs)

    if "MERGE_OUTPUTS" in run_config:
        logger.info("Merge outputs")
        merge_outputs(model_scope=MODEL_SCOPE, chemicals=CHEMICALS)


if __name__ == "__main__":
    main()
