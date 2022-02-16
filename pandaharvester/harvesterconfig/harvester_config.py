import re
import os
import sys
import six
import json
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


# load configmap
config_map_data = {}
if 'PANDA_HOME' in os.environ:
    config_map_name = 'panda_harvester_configmap.json'
    config_map_path = os.path.join(os.environ['PANDA_HOME'], 'etc/configmap', config_map_name)
    if os.path.exists(config_map_path):
        with open(config_map_path) as f:
            config_map_data = json.load(f)

# loop over all sections
for tmpSection in tmpConf.sections():
    # read section
    tmpDict = getattr(tmpConf, tmpSection)
    # load configmap
    if tmpSection in config_map_data:
        tmpDict.update(config_map_data[tmpSection])
    # make section class
    tmpSelf = _SectionClass()
    # update module dict
    sys.modules[__name__].__dict__[tmpSection] = tmpSelf
    # expand all values
    for tmpKey, tmpVal in iteritems(tmpDict):
        # use env vars
        if isinstance(tmpVal, str) and tmpVal.startswith('$'):
            tmpMatch = re.search('\$\{*([^\}]+)\}*', tmpVal)
            envName = tmpMatch.group(1)
            if envName not in os.environ:
                raise KeyError('{0} in the cfg is an undefined environment variable.'.format(envName))
            tmpVal = os.environ[envName]
        # convert string to bool/int
        if not isinstance(tmpVal, six.string_types):
            pass
        elif tmpVal == 'True':
            tmpVal = True
        elif tmpVal == 'False':
            tmpVal = False
        elif tmpVal == 'None':
            tmpVal = None
        elif re.match('^\d+$', tmpVal):
            tmpVal = int(tmpVal)
        elif '\n' in tmpVal and (
                re.match(r'^\W*\[.*\]\W*$', tmpVal.replace('\n', ''))
                or re.match(r'^\W*\{.*\}\W*$', tmpVal.replace('\n', ''))):
            tmpVal = json.loads(tmpVal)
        elif '\n' in tmpVal:
            tmpVal = tmpVal.split('\n')
            # remove empty
            tmpVal = [x.strip() for x in tmpVal if x.strip()]
        # update dict
        setattr(tmpSelf, tmpKey, tmpVal)
