import re
import os
import sys
from future.utils import iteritems

from liveconfigparser.LiveConfigParser import LiveConfigParser

# get ConfigParser
tmpConf = LiveConfigParser()

# URL for config file if any
configEnv = 'HARVESTER_INSTANCE_CONFIG_URL'
if configEnv in os.environ:
    configURL = os.environ[configEnv]
else:
    configURL = None

# read
tmpConf.read('panda_harvester.cfg', configURL)


# dummy section class
class _SectionClass:
    def __init__(self):
        pass


# loop over all sections
for tmpSection in tmpConf.sections():
    # read section
    tmpDict = getattr(tmpConf, tmpSection)
    # make section class
    tmpSelf = _SectionClass()
    # update module dict
    sys.modules[__name__].__dict__[tmpSection] = tmpSelf
    # expand all values
    for tmpKey, tmpVal in iteritems(tmpDict):
        # use env vars
        if tmpVal.startswith('$'):
            tmpMatch = re.search('\$\{*([^\}]+)\}*', tmpVal)
            envName = tmpMatch.group(1)
            if envName not in os.environ:
                raise KeyError('{0} in the cfg is an undefined environment variable.'.format(envName))
            tmpVal = os.environ[envName]
        # convert string to bool/int
        if tmpVal == 'True':
            tmpVal = True
        elif tmpVal == 'False':
            tmpVal = False
        elif tmpVal == 'None':
            tmpVal = None
        elif re.match('^\d+$', tmpVal):
            tmpVal = int(tmpVal)
        elif '\n' in tmpVal:
            tmpVal = tmpVal.split('\n')
            # remove empty
            tmpVal = [x.strip() for x in tmpVal if x.strip()]
        # update dict
        setattr(tmpSelf, tmpKey, tmpVal)
