import warnings

from flow.import_data.chemical_data import ChemicalDataImporter
from flow.import_data.generic_data import GenericDataImporter
from util.util import timing


@timing
def import_data(**kwargs):
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Data Validation extension is not supported and will be removed",
        )
        importer = GenericDataImporter(**kwargs)
        importer.import_data()

        importer = ChemicalDataImporter(**kwargs)
        importer.import_all()
