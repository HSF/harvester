class PluginBase:
    def __init__(self, **kwarg):
        for tmpKey, tmpVal in kwarg.iteritems():
            setattr(self, tmpKey, tmpVal)
