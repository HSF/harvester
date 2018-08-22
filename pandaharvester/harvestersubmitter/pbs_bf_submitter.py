import datetime
import tempfile
try:
    import subprocess32 as subprocess
except:
    import subprocess

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase

# logger
baseLogger = core_utils.setup_logger('pbs_bf_submitter')


# submitter for PBS batch system
class PBSSubmitter(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.uploadLog = False
        self.logBaseURL = None
        self.nodesToDecrease = 0
        self.durationSecondsToDecrease = 0
        PluginBase.__init__(self, **kwarg)
        # template for batch script
        tmpFile = open(self.templateFile)
        self.template = tmpFile.read()
        tmpFile.close()
        if not hasattr(self, 'maxDurationSeconds'):
            self.maxDurationSeconds = self.defaultDurationSeconds

    # adjust cores based on available events
    def adjust_needed_resource(self, workspec):
        # TODO
        return workspec.nCore
    
    # submit workers
    def submit_workers(self, workspec_list):
        retList = []
        for workSpec in workspec_list:
            # make logger
            tmpLog = self.make_logger(baseLogger, 'workerID={0}'.format(workSpec.workerID),
                                      method_name='submit_workers')
            # make batch script
            batchFile = self.make_batch_script(workSpec)
            # command
            comStr = "qsub {0}".format(batchFile)
            # submit
            tmpLog.debug('submit with {0}'.format(comStr))
            p = subprocess.Popen(comStr.split(),
                                 shell=False,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            # check return code
            stdOut, stdErr = p.communicate()
            retCode = p.returncode
            tmpLog.debug('retCode={0}'.format(retCode))
            if retCode == 0:
                # extract batchID
                workSpec.batchID = stdOut.split()[-1]
                tmpLog.debug('batchID={0}'.format(workSpec.batchID))
                # set log files
                if self.uploadLog:
                    if self.logBaseURL is None:
                        baseDir = workSpec.get_access_point()
                    else:
                        baseDir = self.logBaseURL
                    stdOut, stdErr = self.get_log_file_names(batchFile, workSpec.batchID)
                    if stdOut is not None:
                        workSpec.set_log_file('stdout', '{0}/{1}'.format(baseDir, stdOut))
                    if stdErr is not None:
                        workSpec.set_log_file('stderr', '{0}/{1}'.format(baseDir, stdErr))
                tmpRetVal = (True, '')
            else:
                # failed
                errStr = stdOut + ' ' + stdErr
                tmpLog.error(errStr)
                tmpRetVal = (False, errStr)
            retList.append(tmpRetVal)
        return retList

    # make batch script
    def make_batch_script(self, workspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, 'workerID={0}'.format(workspec.workerID),
                                  method_name='make_batch_script')

        resource = self.get_max_bf_resources(workspec)
        if resource:
            tmpLog.debug("Selected resources: %s" % resource)
            workspec.nCore = (resource['nodes'] - self.nodesToDecrease) * self.nCorePerNode
            duration = resource['duration'] - self.durationSecondsToDecrease
        else:
            workspec.nCore = self.nCore
            duration = self.defaultDurationSeconds

        workspec.nCore = self.adjust_needed_resource(workspec)
        duration = str(datetime.timedelta(seconds=duration))
        tmpFile = tempfile.NamedTemporaryFile(delete=False, suffix='_submit.sh', dir=workspec.get_access_point())
        tmpFile.write(self.template.format(nCorePerNode=self.nCorePerNode,
                                           localQueue=self.localQueue,
                                           projectName=self.projectName,
                                           nNode=workspec.nCore / self.nCorePerNode,
                                           accessPoint=workspec.accessPoint,
                                           duration=duration,
                                           workerID=workspec.workerID)
                      )
        tmpFile.close()
        return tmpFile.name

    # get log file names
    def get_log_file_names(self, batch_script, batch_id):
        stdOut = None
        stdErr = None
        with open(batch_script) as f:
            for line in f:
                if not line.startswith('#PBS'):
                    continue
                items = line.split()
                if '-o' in items:
                    stdOut = items[-1].replace('$SPBS_JOBID', batch_id)
                elif '-e' in items:
                    stdErr = items[-1].replace('$PBS_JOBID', batch_id)
        return stdOut, stdErr

    # get backfill resources
    def get_bf_resources(self, workspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, 'workerID={0}'.format(workspec.workerID),
                                  method_name='get_bf_resources')

        resources = {}
        # command
        comStr = "showbf -p {0} --blocking".format(self.partition)
        # get backfill resources
        tmpLog.debug('Get backfill resources with {0}'.format(comStr))
        p = subprocess.Popen(comStr.split(),
                             shell=False,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        # check return code
        stdOut, stdErr = p.communicate()
        retCode = p.returncode
        tmpLog.debug('retCode={0}'.format(retCode))
        if retCode == 0:
            # extract batchID
            tmpLog.debug("Available backfill resources for partition(%s):\n%s" % (self.partition, stdOut))
            lines = stdOut.splitlines()
            for line in lines:
                line = line.strip()
                if line.startswith(self.partition):
                    try:
                        items = line.split()
                        nodes = int(items[2])
                        if nodes < self.minNodes:
                            continue
                        duration = items[3]
                        if duration == 'INFINITY':
                            duration = self.maxDurationSeconds
                        else:
                            h, m, s = duration.split(':')
                            duration = int(h) * 3600 + int(m) * 60 + int(s)
                            if duration < self.minDurationSeconds:
                                continue
                            if duration > self.maxDurationSeconds:
                                duration = self.maxDurationSeconds
                        key = nodes * duration
                        if key not in resources:
                            resources[key] = []
                        resources[key].append({'nodes': nodes, 'duration': duration})
                    except:
                        tmpLog.error("Failed to parse line: %s" % line)

        else:
            # failed
            errStr = stdOut + ' ' + stdErr
            tmpLog.error(errStr)
        tmpLog.info("Available backfill resources: %s" % resources)
        return resources

    def get_max_bf_resources(self, workspec):
        resources = self.get_bf_resources(workspec)
        if resources:
            max_capacity = max(resources.keys())
            return resources[max_capacity][0]
        return None
