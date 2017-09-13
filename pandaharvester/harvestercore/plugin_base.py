from future.utils import iteritems


class PluginBase:
    def __init__(self, **kwarg):
        for tmpKey, tmpVal in iteritems(kwarg):
            setattr(self, tmpKey, tmpVal)
