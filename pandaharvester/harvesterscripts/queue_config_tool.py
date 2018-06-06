import types
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper

qcm = QueueConfigMapper()
qcm.load_data()


def list_active_queues():
    """list all active queue names"""
    qs = qcm.get_active_queues()
    ks = qs.keys()
    ks.sort()
    for k in ks:
        print (k)


def list_config_ids():
    """list all configIDs and queue names"""
    qs = qcm.get_all_queues_with_config_ids()
    ks = qs.keys()
    ks.sort()
    print ('configID : queue name')
    print ('--------- ------------')
    for k in ks:
        print ('{0:8} : {1}'.format(k, qs[k].queueName))


def dump_active_queue(name):
    """dump configuration of an active queue with name"""
    if not qcm.has_queue(name):
        print ("ERROR : {0} is not available".format(name))
        return
    q = qcm.get_queue(name)
    print (q)


def dump_all_active_queues():
    """dump configuration of all active queues"""
    qs = qcm.get_active_queues()
    ks = qs.keys()
    ks.sort()
    for k in ks:
        print (qs[k])


def dump_queue_with_config_id(config_id):
    """dump configuration of a queue with configID"""
    if not qcm.has_queue(None, config_id):
        print ("ERROR : configID={0} is not available".format(config_id))
        return
    q = qcm.get_queue(None, config_id)
    print (q)


def help(o=None):
    """help() to list all functions. help(func_name) for the function"""
    if o is not None:
        __builtins__.help(o)
    else:
        maxLen = len(max(globals(), key=len))
        print (('{0:' + str(maxLen) + '} : {1}').format('function name', 'description'))
        print ('-' * maxLen + '- -' + '-' * maxLen)
        for i in sorted(globals()):
            v = globals()[i]
            if isinstance(v, types.FunctionType):
                print (('{0:' + str(maxLen) + '} : {1}').format(i, v.__doc__))
