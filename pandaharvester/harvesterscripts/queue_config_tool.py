import types
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper


qcm = QueueConfigMapper(update_db=False)


def list_active_queues():
    """list all active queue names"""
    qs = qcm.get_active_queues()
    ks = sorted(qs.keys())
    for k in ks:
        print(k)


def list_config_ids():
    """list all configIDs and queue names"""
    qs = qcm.get_all_queues_with_config_ids()
    ks = sorted(qs.keys())
    print("configID : queue name")
    print("--------- ------------")
    for k in ks:
        print("{0:8} : {1}".format(k, qs[k].queueName))


def dump_active_queue(name, to_print=True):
    """dump configuration of an active queue with name"""
    if not qcm.has_queue(name):
        print("ERROR : {0} is not available".format(name))
        return
    q = qcm.get_queue(name)
    if to_print:
        print(q)
    else:
        return q


def dump_all_active_queues(to_print=True):
    """dump configuration of all active queues"""
    qs = qcm.get_active_queues()
    if to_print:
        ks = sorted(qs.keys())
        for k in ks:
            print(qs[k])
    else:
        return list(qs.values())


def dump_queue_with_config_id(config_id, to_print=True):
    """dump configuration of a queue with configID"""
    if not qcm.has_queue(None, config_id):
        print("ERROR : configID={0} is not available".format(config_id))
        return
    q = qcm.get_queue(None, config_id)
    if to_print:
        print(q)
    else:
        return q


def help(o=None):
    """help() to list all functions. help(func_name) for the function"""
    if o is not None:
        __builtins__.help(o)
    else:
        maxLen = len(max(globals(), key=len))
        print(("{0:" + str(maxLen) + "} : {1}").format("function name", "description"))
        print("-" * maxLen + "- -" + "-" * maxLen)
        for i in sorted(globals()):
            v = globals()[i]
            if isinstance(v, types.FunctionType):
                print(("{0:" + str(maxLen) + "} : {1}").format(i, v.__doc__))
