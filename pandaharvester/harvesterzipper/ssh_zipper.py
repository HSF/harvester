from pandaharvester.harvestercore import core_utils
from .base_zipper import BaseZipper

# logger
_logger = core_utils.setup_logger("ssh_zipper")


# ssh plugin for zipper
class SshZipper(BaseZipper):
    # constructor
    def __init__(self, **kwarg):
        BaseZipper.__init__(self, **kwarg)

    # zip output files
    def zip_output(self, jobspec):
        tmpLog = self.make_logger(_logger, "PandaID={0}".format(jobspec.PandaID), method_name="zip_output")
        return self.ssh_zip_output(jobspec, tmpLog)

    # asynchronous zip output
    def async_zip_output(self, jobspec):
        tmpLog = self.make_logger(_logger, "PandaID={0}".format(jobspec.PandaID), method_name="zip_output")
        # not really asynchronous as two staged zipping is not implemented in this plugin
        return self.ssh_zip_output(jobspec, tmpLog)

    # post zipping
    def post_zip_output(self, jobspec):
        return True, ""
