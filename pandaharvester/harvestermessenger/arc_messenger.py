import errno
import os
import json
import re
import shutil
import tarfile
from urlparse import urlparse
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
        self.schedulerid = harvester_config.master.harvester_id
        self.logdir = harvester_config.joblog.logdir
        self.logurl = harvester_config.joblog.logurl
        self.tmpdir = '/tmp' # TODO configurable or common function

        # Credential dictionary role: proxy file
        self.certs = dict(zip([r.split('=')[1] for r in list(harvester_config.credmanager.voms)],
                              list(harvester_config.credmanager.outCertFile)))
        self.cred_type = arc.initializeCredentialsType(arc.initializeCredentialsType.SkipCredentials)


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
  

    def _download_outputs(self, files, logsubdir, jobid, pandaid, userconfig, log):
        '''Download the output files specified in downloadfiles'''

        # construct datapoint object, initialising connection. Use the same
        # object until base URL changes. TODO group by base URL.
        
        datapoint = arc_utils.DataPoint(str(jobid), userconfig)
        dp = datapoint.h
        dm = arc.DataMover()
        dm.retry(False)
        dm.passive(True)
        dm.secure(False)
        fetched = []
        notfetched = []
        notfetchedretry = []
        
        # create required local log dirs
        logdir = os.path.join(self.logdir, logsubdir)
        try:
            os.makedirs(logdir, 0755)
        except OSError as e:
            if e.errno != errno.EEXIST or not os.path.isdir(logdir):
                log.warning('Failed to create directory {0}: {1}'.format(logdir, os.strerror(e.errno)))
                notfetched.append(jobid)
                return (fetched, notfetched, notfetchedretry)

        tmpdldir = os.path.join(self.tmpdir, str(pandaid))
        try:
            os.makedirs(tmpdldir, 0755)
        except OSError as e:
            if e.errno != errno.EEXIST or not os.path.isdir(tmpdldir):
                log.warning('Failed to create directory {0}: {1}'.format(tmpdldir, os.strerror(e.errno)))
                notfetched.append(jobid)
                return (fetched, notfetched, notfetchedretry)

        filelist = files.split(';')
        if re.search(r'[\*\[\]\?]', files):
            # found wildcard, need to get sessiondir list
            remotefiles = self.listUrlRecursive(jobid, log)
            expandedfiles = []
            for wcf in filelist:
                if re.search(r'[\*\[\]\?]', wcf):
                    # only match wildcards in matching dirs
                    expandedfiles += [rf for rf in remotefiles if fnmatch.fnmatch(rf, wcf) and os.path.dirname(rf) == os.path.dirname(wcf)]
                else:
                    expandedfiles.append(wcf)
            # remove duplicates from wildcard matching through set
            filelist = list(set(expandedfiles))

        for f in filelist:
            if f == 'gmlog/errors':
                localfile = os.path.join(logdir, '%d.log' % pandaid)
            elif f.find('.log') != -1:
                localfile = os.path.join(logdir, '%d.out' % pandaid)
            else:
                localfile = os.path.join(tmpdldir, f)

            remotefile = arc.URL(str(jobid + '/' + f))
            dp.SetURL(remotefile)
            localdp = arc_utils.DataPoint(str(localfile), userconfig)
            # do the copy
            status = dm.Transfer(dp, localdp.h, arc.FileCache(), arc.URLMap())
            if not status and str(status).find('File unavailable') == -1: # tmp fix for globus error which is always retried
                if status.Retryable():
                    log.warning('Failed to download but will retry {0}: {1}'.format(dp.GetURL().str(), str(status)))
                    notfetchedretry.append(jobid)
                else:
                    log.error('Failed to download with permanent failure {0}: {1}'.format(dp.GetURL().str(), str(status)))
                    notfetched.append(jobid)
            else:
                os.chmod(localfile, 0644)
                log.info('Downloaded {0}'.format(dp.GetURL().str()))

        if jobid not in notfetched and jobid not in notfetchedretry:
            fetched.append(jobid)

        return (fetched, notfetched, notfetchedretry)

    def _extractAndFixPilotPickle(self, arcjob, pandaid, haveoutput, logsubdir, log):
        '''
        Extract the pilot pickle from jobSmallFiles.tgz, and fix attributes
        '''

        arcid = arcjob['JobID']
        pandapickle = None
        tmpjobdir = os.path.join(self.tmpdir, str(pandaid))
        if haveoutput:
            log.debug('os.cwd(): {0}'.format(os.getcwd()))
            try:
                smallfiles = tarfile.open(os.path.join(tmpjobdir, 'jobSmallFiles.tgz'))
                pandapickle = smallfiles.extractfile("panda_node_struct.pickle")
            except Exception as e:
                log.error("{0}: failed to extract pickle for arcjob {1}: {2}".format(pandaid, arcid, str(e)))

        if pandapickle:
            jobinfo = arc_utils.ARCPandaJob(filehandle=pandapickle)
            # de-serialise the metadata to json
            try:
                jobinfo.metaData = json.loads(jobinfo.metaData)
            except:
                log.warning("{0}: no metaData in pilot pickle".format(pandaid))
            shutil.rmtree(tmpjobdir, ignore_errors=True)
        else:
            jobinfo = arc_utils.ARCPandaJob(jobinfo={'jobId': pandaid, 'state': 'finished'})
            jobinfo.schedulerID = self.schedulerid
            jobinfo.pilotID = "%s.out|Unknown|Unknown|Unknown|Unknown" % '/'.join([self.logurl, logsubdir, str(pandaid)])
                
            # TODO: set error code based on batch error message (memory kill etc)
            jobinfo.pilotErrorCode = 1008
            if arcjob['Error']:
                jobinfo.pilotErrorDiag = arcjob['Error']
            else:
                # Probably failure getting outputs
                jobinfo.pilotErrorDiag = "Failed to get outputs from CE"            

        jobinfo.computingElement = urlparse(arcid).netloc
        
        return jobinfo.dictionary()

    def post_processing(self, workspec, jobspec_list, map_type):
        '''
        Fetch job output and process pilot info for sending in final heartbeat.
        The pilot pickle is loaded and some attributes corrected (schedulerid,
        pilotlog etc), then converted to dictionary and stored in
        workspec.workAttributes[pandaid]. If pilot pickle cannot be used,
        report ARC error in pilotErrorDiag and fill all possible attributes
        using ARC information.
        '''

        arclog = arc_utils.ARCLogger(baselogger, workspec.workerID)
        tmplog = arclog.log
        tmplog.info('Post processing ARC job {0}'.format(workspec.batchID))
        job = workspec.workAttributes['arcjob']
        proxyrole = workspec.workAttributes['proxyrole']
        arcid = job['JobID']
        tmplog.info('Job id {0}'.format(arcid))

        if 'arcdownloadfiles' not in workspec.workAttributes:
            tmplog.error('No files to download')
            return

        # Assume one-to-one mapping of workers to jobs. If jobspec_list is empty
        # it means the job was cancelled by panda or otherwise forgotten
        if not jobspec_list:
            return

        # Set certificate
        userconfig = arc.UserConfig(self.cred_type)
        try:
            userconfig.ProxyPath(str(self.certs[proxyrole]))
        except:
            tmplog.error("Job {0}: no proxy found with role {1}".format(job.JobID, proxyrole))
            return

        pandaid = long(jobspec_list[0].PandaID)
        # post_processing is only called once, so no retries are done. But keep
        # the possibility here in case it changes
        (fetched, notfetched, notfetchedretry) = self._download_outputs(workspec.workAttributes['arcdownloadfiles'],
                                                                        workspec.workAttributes['logsubdir'],
                                                                        arcid, pandaid, userconfig, tmplog)
        if arcid not in fetched:
            tmplog.warning("Could not get outputs of {0}".format(arcid))

        workspec.workAttributes[pandaid] = {}

        workspec.workAttributes[pandaid] = self._extractAndFixPilotPickle(job, pandaid, (arcid in fetched), workspec.workAttributes['logsubdir'], tmplog)
        
        tmplog.debug("pilot info for {0}: {1}".format(pandaid, workspec.workAttributes[pandaid]))

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
    
    def kill_requested(self, workspec):
        '''Worker wants to kill itself (?)'''
        return False
    
    def is_alive(self, workspec, time_limit):
        '''Check if worker is alive, not for Grid'''
        return True
    

def test():
    pass

if __name__ == '__main__':
    test()
