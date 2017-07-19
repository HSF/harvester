import json
import re
from threading import Thread
import traceback
import arc

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestersubmitter.arc_parser import ARCParser
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvestermisc import arc_utils

# logger
baselogger = core_utils.setup_logger()

class SubmitThr(Thread):
    '''Class to submit jobs in separate thread'''

    def __init__(self, queuelist, jobdescs, userconfig, log):
        Thread.__init__(self)
        self.queuelist = queuelist
        self.jobdescs = jobdescs
        self.userconfig = userconfig
        self.log = log
        self.job = None

    def run(self):
        '''Do brokering and submit'''

        # Do brokering among the available queues
        jobdesc = self.jobdescs[0]
        broker = arc.Broker(self.userconfig, jobdesc, "Random")
        targetsorter = arc.ExecutionTargetSorter(broker)
        for target in self.queuelist:
            self.log.debug("considering target {0}:{1}".format(target.ComputingService.Name,
                                                               target.ComputingShare.Name))

            # Adding an entity performs matchmaking and brokering
            targetsorter.addEntity(target)

        if len(targetsorter.getMatchingTargets()) == 0:
            self.log.error("no clusters satisfied job description requirements")
            return

        targetsorter.reset() # required to reset iterator, otherwise we get a seg fault
        selectedtarget = targetsorter.getCurrentTarget()
        # Job object will contain the submitted job
        job = arc.Job()
        submitter = arc.Submitter(self.userconfig)
        if submitter.Submit(selectedtarget, jobdesc, job) != arc.SubmissionStatus.NONE:
            self.log.error("Submission failed")
            return

        self.job = job


class ARCSubmitter(PluginBase):
    '''Submitter for ARC CE'''

    def __init__(self, **kwarg):
        '''Set up DB connection and credentials'''
        PluginBase.__init__(self, **kwarg)

        self.dbproxy = DBProxy()

        # Get credential file from config
        # TODO Handle multiple credentials for prod/analy
        self.cert = harvester_config.credmanager.certFile
        cred_type = arc.initializeCredentialsType(arc.initializeCredentialsType.SkipCredentials)
        self.userconfig = arc.UserConfig(cred_type)
        self.userconfig.ProxyPath(self.cert)


    def _run_submit(self, thr):
        '''Run a thread to do the submission'''

        try:
            thr.start()
        except:
            pass

        # Be careful to wait longer than submission timeout
        thr.join(thr.userconfig.Timeout() + 60.0)
        if thr.isAlive():
            # abort due to timeout and try again
            raise Exception("submission timeout")
        if thr.job is None:
            raise Exception("submission failed")

        thr.log.info("job id {0}".format(thr.job.JobID))
        return thr.job


    def _arc_submit(self, xrsl, arcces, log):
        '''Check the available CEs and submit'''

        queuelist = []

        for arcce in arcces:
            (ce_endpoint, ce_queue) = arcce
            aris = arc.URL(str(ce_endpoint))
            ce_host = aris.Host()
            if aris.Protocol() == 'https':
                aris.ChangePath('/arex')
                infoendpoints = [arc.Endpoint(aris.str(),
                                              arc.Endpoint.COMPUTINGINFO,
                                              'org.ogf.glue.emies.resourceinfo')]
            else:
                aris = 'ldap://'+aris.Host()+'/mds-vo-name=local,o=grid'
                infoendpoints = [arc.Endpoint(aris,
                                              arc.Endpoint.COMPUTINGINFO,
                                              'org.nordugrid.ldapng')]

            # retriever contains a list of CE endpoints
            retriever = arc.ComputingServiceRetriever(self.userconfig, infoendpoints)
            retriever.wait()
            # targets is the list of queues
            # parse target.ComputingService.ID for the CE hostname
            # target.ComputingShare.Name is the queue name
            targets = retriever.GetExecutionTargets()

            # Filter only sites for this process
            for target in targets:
                if not target.ComputingService.ID:
                    log.info("Target {0} does not have ComputingService ID defined, skipping".format(target.ComputingService.Name))
                    continue
                # If EMI-ES infoendpoint, force EMI-ES submission
                if infoendpoints[0].InterfaceName == 'org.ogf.glue.emies.resourceinfo' \
                  and target.ComputingEndpoint.InterfaceName != 'org.ogf.glue.emies.activitycreation':
                    log.debug("Rejecting target interface {0} because not EMI-ES".format(target.ComputingEndpoint.InterfaceName))
                    continue
                # Check for matching host and queue
                targethost = re.sub(':arex$', '', re.sub('urn:ogf:ComputingService:', '', target.ComputingService.ID))
                targetqueue = target.ComputingShare.Name
                if targethost != ce_host:
                    log.debug('Rejecting target host {0} as it does not match {1}'.format(targethost, ce_host))
                    continue
                if targetqueue != ce_queue:
                    log.debug('Rejecting target queue {0} as it does not match {1}'.format(targetqueue, ce_queue))
                    continue

                queuelist.append(target)
                log.debug("Adding target {0}:{1}".format(targethost, targetqueue))

        # check if any queues are available, if not leave and try again next time
        if not queuelist:
            raise Exception("No free queues available")

        log.debug("preparing submission")
        jobdescs = arc.JobDescriptionList()
        if not arc.JobDescription_Parse(str(xrsl), jobdescs):
            raise Exception("Failed to prepare job description")

        # Run the submission in a separate thread
        thr = SubmitThr(queuelist, jobdescs, self.userconfig, log)
        return self._run_submit(thr)


    # submit workers
    def submit_workers(self, workspec_list):
        retlist = []

        # Get queue info from DB
        pandaqueues = self.dbproxy.get_cache("panda_queues.json", None)
        if pandaqueues is None:
            raise Exception("Failed to get panda queue info from database")
        pandaqueues = pandaqueues.data

        osmap = self.dbproxy.get_cache("ddmendpoints_objectstores.json", None)
        if osmap is None:
            raise Exception("Failed to get Object Store info from database")
        osmap = osmap.data

        for workspec in workspec_list:

            tmplog, _ = arc_utils.setup_logging(baselogger, workspec.workerID)

            # Assume for aCT that jobs are always pre-fetched (no late-binding)
            for jobspec in workspec.get_jobspec_list():

                tmplog.debug("JobSpec: {0}".format(jobspec.values_map()))

                if jobspec.computingSite not in pandaqueues:
                    retlist.append((False, "No queue information for {0}".format(jobspec.computingSite)))
                    continue

                # Get CEs from panda queue info
                # List of (endpoint, queue) tuples
                arcces = []
                for endpoint in pandaqueues[jobspec.computingSite]['queues']:
                    ce_endpoint = endpoint['ce_endpoint']
                    if not re.search('://', ce_endpoint):
                        ce_endpoint = 'gsiftp://%s' % ce_endpoint
                    ce_queue = endpoint['ce_queue_name']
                    arcces.append((ce_endpoint, ce_queue))

                if not arcces:
                    retlist.append((False, "No CEs defined for %{0}".format(jobspec.computingSite)))
                    continue

                # Set true pilot or not
                queueconfigmapper = QueueConfigMapper()
                queueconfig = queueconfigmapper.get_queue(jobspec.computingSite)
                pandaqueues[jobspec.computingSite]['truepilot'] = 'running' in queueconfig.noHeartbeat

                tmplog.debug("Converting to ARC XRSL format")
                arcxrsl = ARCParser(jobspec.jobParams,
                                    jobspec.computingSite,
                                    pandaqueues[jobspec.computingSite],
                                    osmap,
                                    '/tmp', # tmpdir, TODO common tmp dir
                                    None, #jobSpec.eventranges, # TODO event ranges
                                    tmplog)
                arcxrsl.parse()
                xrsl = arcxrsl.getXrsl()
                tmplog.debug("ARC xrsl: {0}".format(xrsl))
                
                # Set the files to be downloaded at the end of the job
                downloadfiles = 'gmlog/errors'
                if 'logFile' in jobspec.jobParams:
                    downloadfiles += ';%s' %jobspec.jobParams['logFile'].replace('.tgz', '')
                if not pandaqueues[jobspec.computingSite]['truepilot']:
                    downloadfiles += ';jobSmallFiles.tgz'

                try:
                    tmplog.debug("Submission targets: {0}".format(arcces))
                    arcjob = self._arc_submit(xrsl, arcces, tmplog)
                    tmplog.info("ARC CE job id {0}".format(arcjob.JobID))
                    arc_utils.arcjob2workspec(arcjob, workspec)
                    workspec.workAttributes['arcdownloadfiles'] = downloadfiles
                    workspec.batchID = arcjob.JobID
                    tmplog.debug(workspec.workAttributes)
                    result = (True, '')
                except Exception as exc:
                    tmplog.error(traceback.format_exc())
                    result = (False, "Failed to submit ARC job: {0}".format(str(exc)))

                retlist.append(result)

        return retlist


def test():
    '''test submission'''
    from pandaharvester.harvestercore.job_spec import JobSpec
    from pandaharvester.harvestercore.plugin_factory import PluginFactory

    import json

    queuename = 'ARC-TEST'
    queueconfmapper = QueueConfigMapper()
    queueconf = queueconfmapper.get_queue(queuename)
    pluginfactory = PluginFactory()

    pandajob = '{"jobsetID": 11881, "logGUID": "88ee8a52-5c70-490c-a585-5eb6f48e4152", "cmtConfig": "x86_64-slc6-gcc49-opt", "prodDBlocks": "mc16_13TeV:mc16_13TeV.364168.Sherpa_221_NNPDF30NNLO_Wmunu_MAXHTPTV500_1000.merge.EVNT.e5340_e5984_tid11329621_00", "dispatchDBlockTokenForOut": "NULL,NULL", "destinationDBlockToken": "dst:CERN-PROD_DATADISK,dst:NDGF-T1_DATADISK", "destinationSE": "CERN-PROD_PRESERVATION", "realDatasets": "mc16_13TeV.364168.Sherpa_221_NNPDF30NNLO_Wmunu_MAXHTPTV500_1000.simul.HITS.e5340_e5984_s3126_tid11364822_00,mc16_13TeV.364168.Sherpa_221_NNPDF30NNLO_Wmunu_MAXHTPTV500_1000.simul.log.e5340_e5984_s3126_tid11364822_00", "prodUserID": "gingrich", "GUID": "A407D965-B139-A543-8851-A8E134A678D7", "realDatasetsIn": "mc16_13TeV:mc16_13TeV.364168.Sherpa_221_NNPDF30NNLO_Wmunu_MAXHTPTV500_1000.merge.EVNT.e5340_e5984_tid11329621_00", "nSent": 2, "cloud": "WORLD", "StatusCode": 0, "homepackage": "AtlasOffline/21.0.15", "inFiles": "EVNT.11329621._001079.pool.root.1", "processingType": "simul", "currentPriority": 900, "fsize": "129263662", "fileDestinationSE": "CERN-PROD_PRESERVATION,BOINC_MCORE", "scopeOut": "mc16_13TeV", "minRamCount": 1573, "jobDefinitionID": 0, "maxWalltime": 40638, "scopeLog": "mc16_13TeV", "transformation": "Sim_tf.py", "maxDiskCount": 485, "coreCount": 1, "prodDBlockToken": "NULL", "transferType": "NULL", "destinationDblock": "mc16_13TeV.364168.Sherpa_221_NNPDF30NNLO_Wmunu_MAXHTPTV500_1000.simul.HITS.e5340_e5984_s3126_tid11364822_00_sub0418634273,mc16_13TeV.364168.Sherpa_221_NNPDF30NNLO_Wmunu_MAXHTPTV500_1000.simul.log.e5340_e5984_s3126_tid11364822_00_sub0418634276", "dispatchDBlockToken": "NULL", "jobPars": "--inputEVNTFile=EVNT.11329621._001079.pool.root.1 --maxEvents=50 --postInclude \\"default:RecJobTransforms/UseFrontier.py\\" --preExec \\"EVNTtoHITS:simFlags.SimBarcodeOffset.set_Value_and_Lock(200000)\\" \\"EVNTtoHITS:simFlags.TRTRangeCut=30.0;simFlags.TightMuonStepping=True\\" --preInclude \\"EVNTtoHITS:SimulationJobOptions/preInclude.BeamPipeKill.py,SimulationJobOptions/preInclude.FrozenShowersFCalOnly.py\\" --skipEvents=4550 --firstEvent=5334551 --outputHITSFile=HITS.11364822._128373.pool.root.1 --physicsList=FTFP_BERT_ATL_VALIDATION --randomSeed=106692 --DBRelease=\\"all:current\\" --conditionsTag \\"default:OFLCOND-MC16-SDR-14\\" --geometryVersion=\\"default:ATLAS-R2-2016-01-00-01_VALIDATION\\" --runNumber=364168 --AMITag=s3126 --DataRunNumber=284500 --simulator=FullG4 --truthStrategy=MC15aPlus", "attemptNr": 2, "swRelease": "Atlas-21.0.15", "nucleus": "CERN-PROD", "maxCpuCount": 40638, "outFiles": "HITS.11364822._128373.pool.root.11,log.11364822._128373.job.log.tgz.11", "ddmEndPointOut": "CERN-PROD_DATADISK,NDGF-T1_DATADISK", "scopeIn": "mc16_13TeV", "PandaID": 3487584273, "sourceSite": "NULL", "dispatchDblock": "panda.11364822.07.05.GEN.0c9b1d3b-feec-411a-89e4-1cbf7347d70c_dis003487584270", "prodSourceLabel": "managed", "checksum": "ad:cd0bf10b", "jobName": "mc16_13TeV.364168.Sherpa_221_NNPDF30NNLO_Wmunu_MAXHTPTV500_1000.simul.e5340_e5984_s3126.3433643361", "ddmEndPointIn": "NDGF-T1_DATADISK", "taskID": 11364822, "logFile": "log.11364822._128373.job.log.tgz.1"}'
    pandajob = json.loads(pandajob)
    jspec = JobSpec()
    jspec.convert_job_json(pandajob)
    jspec.computingSite = queuename
    jspeclist = [jspec]

    maker = pluginfactory.get_plugin(queueconf.workerMaker)
    wspec = maker.make_worker(jspeclist, queueconf)

    wspec.hasJob = 1
    wspec.set_jobspec_list(jspeclist)

    sub = ARCSubmitter()
    print sub.submit_workers([wspec])
    print wspec.batchID

if __name__ == '__main__':
    test()
