from pandaharvester.harvestercore import core_utils
from .base_stager import BaseStager

# logger
baseLogger = core_utils.setup_logger("rse_direct_stager")


# stager plugin with RSE + no data motion
class RseDirectStager(BaseStager):
    """In the workflow for RseDirectStager, workers directly upload output files to RSE
    and thus there is no data motion in Harvester."""

    # constructor

    def __init__(self, **kwarg):
        BaseStager.__init__(self, **kwarg)

    # check status
    def check_stage_out_status(self, jobspec):
        for fileSpec in jobspec.get_output_file_specs(skip_done=True):
            fileSpec.status = "finished"
        return True, ""

    # trigger stage out
    def trigger_stage_out(self, jobspec):
        return True, ""

    # zip output files
    def zip_output(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, "PandaID={0}".format(jobspec.PandaID), method_name="zip_output")
        return self.simple_zip_output(jobspec, tmpLog)
