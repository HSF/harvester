import re

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils

# placeholder in queue-based plugin parameters, such as ${harvesterID}, ${queueName} or ${common.<key>}
_placeholder_pattern = re.compile(r"\$\{([a-zA-Z\d_.]+)\}")


# make a list from a single value or a list
def as_list(data):
    return data if isinstance(data, list) else [data]


# make plugins from pluginConfigs in harvester config, a list of {"module": ..., "name": ..., "configs": {setup_name: params}}
def make_plugins_from_plugin_configs(plugin_factory, plugin_configs, tmp_log):
    plugins = []
    for plugin_config in plugin_configs:
        try:
            base_params = {"module": plugin_config["module"], "name": plugin_config["name"]}
            setup_maps = plugin_config["configs"].items()
        except Exception:
            tmp_log.error(f"failed to parse pluginConfigs {plugin_config}")
            core_utils.dump_error_message(tmp_log)
            continue
        for setup_name, setup_map in setup_maps:
            plugin_params = {**base_params, "setup_name": setup_name}
            try:
                plugin_params.update(setup_map)
                plugin = plugin_factory.get_plugin(plugin_params)
                if plugin is not None:
                    plugins.append(plugin)
            except Exception:
                tmp_log.error(f"failed to launch plugin in pluginConfigs for {plugin_params}")
                core_utils.dump_error_message(tmp_log)
    return plugins


# resolve placeholders in a queue-based plugin parameter; unknown placeholders are left as they are
def resolve_placeholders(value, queue_name, queue_config):
    if "$" not in value:
        return value

    def _replace(match):
        key = match.group(1)
        if key == "harvesterID":
            return str(harvester_config.master.harvester_id)
        if key == "queueName":
            return queue_name
        if key.startswith("common."):
            # values from the common block of the queue
            common = getattr(queue_config, "common", None) or {}
            common_value = common.get(key[len("common.") :])
            if common_value is not None:
                return str(common_value)
        return match.group(0)

    return _placeholder_pattern.sub(_replace, value)


# make plugins from a list of plugin parameters under attr_name in queue configs, skipping offline queues
def make_plugins_from_queue_configs(plugin_factory, queue_config_mapper, attr_name, tmp_log):
    plugins = []
    for queue_name, queue_config in queue_config_mapper.get_all_queues().items():
        setup_list = getattr(queue_config, attr_name, None)
        if queue_config.queueStatus == "offline" or not isinstance(setup_list, list):
            continue
        for setup in setup_list:
            plugin_params = {"setup_name": queue_name, "queueName": queue_name}
            try:
                for key, value in setup.items():
                    plugin_params[key] = resolve_placeholders(value, queue_name, queue_config) if isinstance(value, str) else value
                plugin = plugin_factory.get_plugin(plugin_params)
                if plugin is not None:
                    plugins.append(plugin)
            except Exception:
                tmp_log.error(f"failed to launch plugin for queue={queue_name} and {plugin_params}")
                core_utils.dump_error_message(tmp_log)
    return plugins
