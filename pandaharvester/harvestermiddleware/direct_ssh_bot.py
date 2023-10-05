import os
import json
import sys

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_factory import PluginFactory

# logger
_logger = core_utils.setup_logger("direct_ssh_bot")


# SSH bot runs a function and exits immediately
class DirectSshBot(object):
    # execution
    def run(self):
        tmpLog = _logger
        try:
            # get parameters
            param_dict = json.load(sys.stdin)
            plugin_config = param_dict["plugin_config"]
            function_name = param_dict["function_name"]
            tmpLog = core_utils.make_logger(_logger, "pid={0}".format(os.getpid()), method_name=function_name)
            tmpLog.debug("start")
            args = core_utils.unpickle_from_text(str(param_dict["args"]))
            kwargs = core_utils.unpickle_from_text(str(param_dict["kwargs"]))
            # get plugin
            pluginFactory = PluginFactory(no_db=True)
            core = pluginFactory.get_plugin(plugin_config)
            # execute
            ret = getattr(core, function_name)(*args, **kwargs)
            # make return
            return_dict = {"return": core_utils.pickle_to_text(ret), "args": core_utils.pickle_to_text(args), "kwargs": core_utils.pickle_to_text(kwargs)}
            tmpLog.debug("done")
        except Exception as e:
            errMsg = core_utils.dump_error_message(tmpLog)
            return_dict = {"exception": core_utils.pickle_to_text(e), "dialog": core_utils.pickle_to_text(errMsg)}
        return json.dumps(return_dict)


# main body


def main():
    # run bot
    bot = DirectSshBot()
    ret = bot.run()
    # propagate results via stdout
    print(ret)


if __name__ == "__main__":
    main()
