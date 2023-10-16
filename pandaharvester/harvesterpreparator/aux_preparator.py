from . import analysis_aux_preparator
from .analysis_aux_preparator import AnalysisAuxPreparator
from pandaharvester.harvestercore import core_utils

# logger
baseLogger = core_utils.setup_logger("aux_preparator")

analysis_aux_preparator.baseLogger = baseLogger


# preparator plugin for auxiliary inputs
class AuxPreparator(AnalysisAuxPreparator):
    pass
