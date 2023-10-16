import re
import os
import sys
import six
import json
import socket
from future.utils import iteritems

from liveconfigparser.LiveConfigParser import LiveConfigParser

# get ConfigParser
tmpConf = LiveConfigParser()

# URL for config file if any
configEnv = "HARVESTER_INSTANCE_CONFIG_URL"
if configEnv in os.environ:
    configURL = os.environ[configEnv]
else:
    configURL = None

# read
tmpConf.read("panda_harvester.cfg", configURL)


# get the value of env var in the config
def env_var_parse(val):
    match = re.search("\$\{*([^\}]+)\}*", val)
    if match is None:
        return val
    var_name = match.group(1)
    if var_name.upper() == "HOSTNAME":
        return socket.gethostname().split(".")[0]
    if var_name not in os.environ:
        raise KeyError("{0} in the cfg is an undefined environment variable.".format(var_name))
    else:
        return os.environ[var_name]


# env var substitution in all values in nested list + dict which parsed from json object
def nested_obj_env_var_sub(obj):
    if isinstance(obj, list):
        for i, v in enumerate(obj):
            if isinstance(v, str):
                obj[i] = env_var_parse(v)
            else:
                nested_obj_env_var_sub(v)
    elif isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, str):
                obj[k] = env_var_parse(v)
            else:
                nested_obj_env_var_sub(v)


# dummy section class
class _SectionClass:
    def __init__(self):
        pass


# load configmap
config_map_data = {}
if "PANDA_HOME" in os.environ:
    config_map_name = "panda_harvester_configmap.json"
    config_map_path = os.path.join(os.environ["PANDA_HOME"], "etc/configmap", config_map_name)
    if os.path.exists(config_map_path):
        with open(config_map_path) as f:
            config_map_data = json.load(f)


# config format in dict for print only
config_dict = {}

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
    # initialize config dict
    config_dict[tmpSection] = {}
    # expand all values
    for tmpKey, tmpVal in iteritems(tmpDict):
        # use env vars
        if isinstance(tmpVal, str) and tmpVal.startswith("$"):
            tmpVal = env_var_parse(tmpVal)
        # convert string to bool/int
        if not isinstance(tmpVal, six.string_types):
            pass
        elif tmpVal == "True":
            tmpVal = True
        elif tmpVal == "False":
            tmpVal = False
        elif tmpVal == "None":
            tmpVal = None
        elif re.match("^\d+$", tmpVal):
            tmpVal = int(tmpVal)
        elif "\n" in tmpVal and (re.match(r"^\W*\[.*\]\W*$", tmpVal.replace("\n", "")) or re.match(r"^\W*\{.*\}\W*$", tmpVal.replace("\n", ""))):
            tmpVal = json.loads(tmpVal)
            nested_obj_env_var_sub(tmpVal)
        elif "\n" in tmpVal:
            tmpVal = tmpVal.split("\n")
            # remove empty
            tmpVal = [x.strip() for x in tmpVal if x.strip()]
        # update dict
        setattr(tmpSelf, tmpKey, tmpVal)
        # update config dict
        if not any(ss in tmpKey.lower() for ss in ["password", "passphrase", "secret"]):
            config_dict[tmpSection][tmpKey] = tmpVal
