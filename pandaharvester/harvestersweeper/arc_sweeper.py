import arc

from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestermisc import arc_utils

# logger
baselogger = core_utils.setup_logger()

class ARCSweeper(PluginBase):
    '''Sweeper for killing and cleaning ARC jobs'''
    
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        
        # Get credential file from config
        # TODO Handle multiple credentials for prod/analy
        self.cert = harvester_config.credmanager.certFile
        cred_type = arc.initializeCredentialsType(arc.initializeCredentialsType.SkipCredentials)
        self.userconfig = arc.UserConfig(cred_type)
        self.userconfig.ProxyPath(str(self.cert))


    def kill_worker(self, workspec):
        """Cancel the ARC job.

        :param workspec: worker specification
        :type workspec: WorkSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        
        # make logger
        arclog = arc_utils.ARCLogger(baselogger, workspec.workerID)
        tmplog = arclog.log

        job = arc_utils.workspec2arcjob(workspec)
        if not job.JobID:
            # Job not submitted
            tmplog.info("Job was not submitted so cannot be cancelled")
            return True, ''

        job_supervisor = arc.JobSupervisor(self.userconfig, [job])
        job_supervisor.Update()
        job_supervisor.Cancel()

        notcancelled = job_supervisor.GetIDsNotProcessed()

        if job.JobID in notcancelled:
            if job.State == arc.JobState.UNDEFINED:
                # If longer than one hour since submission assume job never made it
                if job.SubmissionTime + arc.Period(3600) < arc.Time():
                    tmplog.warning("Assuming job is lost and marking as cancelled")
                    return True, ''

                # Job has not yet reached info system
                tmplog.warning("Job is not yet in info system so cannot be cancelled")
                return False, "Job is not yet in info system so could not be cancelled"

            tmplog.warning("Job could not be cancelled")
            return False, "Job could not be cancelled"

        tmplog.info("Job cancelled successfully")
        return True, ''


    def sweep_worker(self, workspec):
        """Clean the ARC job

        :param workspec: worker specification
        :type workspec: WorkSpec
        :return: A tuple of return code (True for success, False otherwise) and error dialog
        :rtype: (bool, string)
        """
        
         # make logger
        arclog = arc_utils.ARCLogger(baselogger, workspec.workerID)
        tmplog = arclog.log

        job = arc_utils.workspec2arcjob(workspec)
        if not job.JobID:
            # Job not submitted
            tmplog.info("Job was not submitted so cannot be cleaned")
            return True, ''

        job_supervisor = arc.JobSupervisor(self.userconfig, [job])
        job_supervisor.Update()
        job_supervisor.Clean()
        
        notcleaned = job_supervisor.GetIDsNotProcessed()

        if job.JobID in notcleaned:
            tmplog.warning("Job could not be cleaned")
            return False, "Job could not be cleaned"

        tmplog.info("Job cleaned successfully")
        return True, ''



def test(jobid):
    '''Kill a job'''
    from pandaharvester.harvestercore.work_spec import WorkSpec
    import json
    wspec = WorkSpec()
    wspec.batchID = jobid
    workAttributes = {"arcjob": {}}
    workAttributes["arcjob"]["JobID"] = wspec.batchID
    workAttributes["arcjob"]["JobStatusURL"] = "ldap://{0}:2135/mds-vo-name=local,o=grid??sub?(nordugrid-job-globalid={1})".format(urlparse.urlparse(jobid).netloc, wspec.batchID)
    workAttributes["arcjob"]["JobStatusInterfaceName"] = "org.nordugrid.ldapng"
    jobmanagementurl = arc.URL(wspec.batchID)
    jobmanagementurl.ChangePath("/jobs")
    workAttributes["arcjob"]["JobManagementURL"] = jobmanagementurl.str()
    workAttributes["arcjob"]["JobManagementInterfaceName"] = "org.nordugrid.gridftpjob"
    
    wspec.workAttributes = workAttributes
    print wspec.workAttributes

    sweeper = ARCSweeper()
    print sweeper.kill_worker(wspec)

if __name__ == "__main__":
    import time, sys, urlparse
    if len(sys.argv) != 2:
        print "Please give ARC job id"
        sys.exit(1)
    test(sys.argv[1])
