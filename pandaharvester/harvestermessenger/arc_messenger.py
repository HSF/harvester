import os
import arc

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestermisc import arc_utils


# json for outputs
jsonOutputsFileName = harvester_config.payload_interaction.eventStatusDumpJsonFile

# xml for outputs
xmlOutputsBaseFileName = harvester_config.payload_interaction.eventStatusDumpXmlFile

# logger
baselogger = core_utils.setup_logger()

class ARCMessenger(PluginBase):
    '''Mechanism for passing information about completed jobs back to harvester.'''

    def __init__(self, **kwarg):
        self.jobSpecFileFormat = 'json'
        PluginBase.__init__(self, **kwarg)

        # Get credential file from config
        # TODO Handle multiple credentials for prod/analy
        self.cert = harvester_config.credmanager.certFile
        cred_type = arc.initializeCredentialsType(arc.initializeCredentialsType.SkipCredentials)
        self.userconfig = arc.UserConfig(cred_type)
        self.userconfig.ProxyPath(self.cert)

    def _list_url_recursive(self, url, log, fname='', filelist=[]):
        '''List ARC job directory recursively to find all files'''

        dp = arc_utils.DataPoint(url+'/'+fname, self.userconfig)
        files = dp.h.List(arc.DataPoint.INFO_TYPE_NAME | arc.DataPoint.INFO_TYPE_TYPE)
        if not files[1]:
            log.warning("Failed listing %s/%s" % (url, fname))
            return filelist
        for f in files[0]:
            if f.GetType() == f.file_type_file:
                filelist.append((fname+'/'+f.GetName()).strip('/'))
            elif f.GetType() == f.file_type_dir:
                filelist = self.listUrlRecursive(url, log, (fname+'/'+str(f.GetName())).strip('/'), filelist)
        return filelist
  

    def _download_outputs(self, files, jobid, log):
        '''Download the output files specified in downloadfiles'''

        # construct datapoint object, initialising connection. Use the same
        # object until base URL changes. TODO group by base URL.
        datapoint = arc_utils.DataPoint(jobid, self.userconfig)
        dp = datapoint.h
        dm = arc.DataMover()
        dm.retry(False)
        dm.passive(True)
        dm.secure(False)
        fetched = []
        notfetched = []
        notfetchedretry = []
        
        # TODO: configurable dir
        localdir = os.path.join(os.path.getcwd(), 'tmp', jobid[jobid.rfind('/'):])

        if re.search(r'[\*\[\]\?]', files):
            # found wildcard, need to get sessiondir list
            remotefiles = self.listUrlRecursive(jobid, log)
            expandedfiles = []
            for wcf in files:
                if re.search(r'[\*\[\]\?]', wcf):
                    # only match wildcards in matching dirs
                    expandedfiles += [rf for rf in remotefiles if fnmatch.fnmatch(rf, wcf) and os.path.dirname(rf) == os.path.dirname(wcf)]
                else:
                    expandedfiles.append(wcf)
            # remove duplicates from wildcard matching through set
            files = list(set(expandedfiles))

        for f in files:
            localfile = os.path.join(localdir, f)
            # create required local dirs
            try:
                os.makedirs(localdir, 0755)
            except OSError as e:
                if e.errno != errno.EEXIST or not os.path.isdir(localdir):
                    log.warning('Failed to create directory %s: %s', localdir, os.strerror(e.errno))
                    notfetched.append(jobid)
                    break
            remotefile = arc.URL(str(jobid + '/' + f))
            dp.SetURL(remotefile)
            localdp = arc_ttils.DataPoint(localfile, self.userconfig)
            # do the copy
            status = dm.Transfer(dp, localdp.h, arc.FileCache(), arc.URLMap())
            if not status and str(status).find('File unavailable') == -1: # tmp fix for globus error which is always retried
                if status.Retryable():
                    log.warning('Failed to download but will retry %s: %s', dp.GetURL().str(), str(status))
                    notfetchedretry.append(jobid)
                else:
                    log.error('Failed to download with permanent failure %s: %s', dp.GetURL().str(), str(status))
                    notfetched.append(jobid)
            else:
                log.info('Downloaded %s', dp.GetURL().str())
        if jobid not in notfetched and jobid not in notfetchedretry:
            fetched.append(jobid)

        return (fetched, notfetched, notfetchedretry)


    def post_processing(self, workspec, jobspec_list, map_type):
        '''Fetch job output and process (fix pilot pickle, timings etc if needed)'''
        # Get jobReport, xml etc from jobSmallFiles.tgz
        tmplog, _ = arc_utils.setup_logging(baselogger, workspec.workerID)
        job = arc_utils.workspec2arcjob(workspec)
        
        # post_processing is only called once, so no retries are done. But keeping
        # the possibility here in case it changes.
        (fetched, notfetched, notfetchedretry) = self._download_outputs(workspec.workAttributes['downloadfiles'], job.JobID, tmplog)
        if job.JobID not in fetched:
            tmplog.warning("Could not get outputs of {0}", job.JobID)
            

    def get_work_attributes(self, workspec):
        '''Get info from the job to pass back to harvester'''
        # Just return existing attributes. Attributes are added to workspec for
        # finished jobs in post_processing
        return workspec.workAttributes

    def events_requested(self, workspec):
        '''Used to tell harvester that the worker requests events'''
        
        # TODO for ARC + ES where getEventRanges is called before submitting job
        return {}

    def feed_events(self, workspec, events_dict):
        '''Havester has an event range to pass to job'''
        
        # TODO for ARC + ES pass event ranges in job desc
        return True

    def events_to_update(self, workspec):
        '''Report events processed for harvester to update'''
        
        # TODO implement for ARC + ES where job does not update event ranges itself
        return {}

    # The remaining methods do not apply to ARC
    def feed_jobs(self, workspec, jobspec_list):
        '''Pass job to worker. No-op for Grid'''
        return True

    def get_files_to_stage_out(self, workspec):
        '''Not required in Grid case'''
        return {}

    def job_requested(self, workspec):
        '''Used in pull model to say that worker is ready for a job'''
        return False

    def setup_access_points(self, workspec_list):
        '''Access is through CE so nothing to set up here'''
        pass

    def get_panda_ids(self, workspec):
        '''For pull model, get panda IDs assigned to jobs'''
        return []

    def acknowledge_events_files(self, workSpec):
        '''Tell workers that harvester received events/files. No-op here'''
        pass
    

def test():
    pass

if __name__ == '__main__':
    test()
