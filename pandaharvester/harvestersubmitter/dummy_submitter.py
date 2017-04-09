import uuid

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase

# setup base logger
baseLogger = core_utils.setup_logger()


# dummy submitter
class DummySubmitter(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # submit workers
    def submit_workers(self, workspec_list):
        """Submit workers to a scheduling system like batch systems and computing elements.
        This method takes a list of WorkSpecs as input argument, and returns a list of tuples.
        Each tuple is composed of a return code and a dialog message.
        Nth tuple in the returned list corresponds to submission status and dialog message for Nth worker
        in the given WorkSpec list.
        A unique identifier is set to WorkSpec.batchID when submission is successful,
        so that they can be identified in the scheduling system.


        :param workspec_list: a list of work specs instances
        :return: A list of tuples. Each tuple is composed of submission status (True for success, False otherwise)
        and dialog message
        :rtype: [(bool, string),]
        """
        tmpLog = core_utils.make_logger(baseLogger)
        tmpLog.debug('start nWorkers={0}'.format(len(workspec_list)))
        retList = []
        for workSpec in workspec_list:
            if workSpec.get_jobspec_list() is not None:
                tmpLog.debug('aggregated nCore={0} minRamCount={1} maxDiskCount={2}'.format(workSpec.nCore,
                                                                                            workSpec.minRamCount,
                                                                                            workSpec.maxDiskCount))
                tmpLog.debug('max maxWalltime={0}'.format(workSpec.maxWalltime))
                for jobSpec in workSpec.get_jobspec_list():
                    tmpLog.debug('PandaID={0} nCore={1} RAM={2}'.format(jobSpec.PandaID,
                                                                        jobSpec.jobParams['coreCount'],
                                                                        jobSpec.jobParams['minRamCount']))
            workSpec.batchID = uuid.uuid4().hex
            retList.append((True, ''))
        tmpLog.debug('done')
        return retList
