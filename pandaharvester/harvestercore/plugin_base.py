from future.utils import iteritems
from pandaharvester.harvestercore import core_utils


class PluginBase(object):
    def __init__(self, **kwarg):
        for tmpKey, tmpVal in iteritems(kwarg):
            setattr(self, tmpKey, tmpVal)

    # make logger
    def make_logger(self, base_log, token=None, method_name=None, send_dialog=True):
        if send_dialog and hasattr(self, "dbInterface"):
            hook = self.dbInterface
        else:
            hook = None
        return core_utils.make_logger(base_log, token=token, method_name=method_name, hook=hook)
