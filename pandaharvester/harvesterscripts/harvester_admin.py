from pandaharvester.harvestercore import fifos


def repopulate_fifos():
    agent_fifo_class_name_tuple = ('MonitorFIFO',)
    for agent_fifo_class_name in agent_fifo_class_name_tuple:
        fifo = getattr(fifos, agent_fifo_class_name)()
        if not fifo.enabled:
            continue
        fifo.populate(clear_fifo=True)
        print('Repopulated {0} fifo'.format(fifo.agentName))

# TODO
