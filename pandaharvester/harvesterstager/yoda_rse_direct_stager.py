import time
import datetime
import uuid
import os
import sys
import stat
import os.path
import threading
import tarfile
import hashlib
import string
import shutil
import errno

from future.utils import iteritems

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestermover import mover_utils
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
from pandaharvester.harvesterstager.base_stager import BaseStager

from .base_stager import BaseStager

# logger
baseLogger = core_utils.setup_logger("yoda_rse_direct_stager")


class Error(EnvironmentError):
    pass


class SpecialFileError(EnvironmentError):
    """Raised when trying to do a kind of operation (e.g. copying) which is
    not supported on a special file (e.g. a named pipe)"""


class ExecError(EnvironmentError):
    """Raised when a command could not be executed"""


# stager plugin with RSE + local site move behaviour for Yoda zip files
class YodaRseDirectStager(BaseStager):
    """In the workflow for RseDirectStager, workers directly upload output files to RSE
    and thus there is no data motion in Harvester."""

    # constructor

    def __init__(self, **kwarg):
        BaseStager.__init__(self, **kwarg)
        tmpLog = self.make_logger(baseLogger, "ThreadID={0}".format(threading.current_thread().ident), method_name="YodaRseDirectStager __init__ ")
        tmpLog.debug("start")
        self.Yodajob = False
        self.pathConvention = None
        self.objstoreID = None
        self.changeFileStatusOnSuccess = True
        tmpLog.debug("stop")

    # check status
    def check_stage_out_status(self, jobspec):
        # make logger
        tmpLog = self.make_logger(
            baseLogger, "PandaID={0} ThreadID={1}".format(jobspec.PandaID, threading.current_thread().ident), method_name="check_stage_out_status"
        )
        tmpLog.debug("start")
        for fileSpec in jobspec.get_output_file_specs(skip_done=True):
            fileSpec.objstoreID = self.objstoreID
            fileSpec.pathConvention = self.pathConvention
            fileSpec.status = "finished"
        tmpLog.debug("stop")
        return True, ""

    # trigger stage out
    def trigger_stage_out(self, jobspec):
        # make logger
        tmpLog = self.make_logger(
            baseLogger, "PandaID={0} ThreadID={1}".format(jobspec.PandaID, threading.current_thread().ident), method_name="trigger_stage_out"
        )
        tmpLog.debug("start")
        # check that jobspec.computingSite is defined
        if jobspec.computingSite is None:
            # not found
            tmpLog.error("jobspec.computingSite is not defined")
            return False, "jobspec.computingSite is not defined"
        else:
            tmpLog.debug("jobspec.computingSite : {0}".format(jobspec.computingSite))
        # get the queueConfig and corresponding objStoreID_ES
        queueConfigMapper = QueueConfigMapper()
        queueConfig = queueConfigMapper.get_queue(jobspec.computingSite)
        # write to debug log queueConfig.stager
        tmpLog.debug("jobspec.computingSite - {0} queueConfig.stager {1}".format(jobspec.computingSite, queueConfig.stager))
        # check queueConfig stager section to see if jobtype is set
        if "jobtype" in queueConfig.stager:
            if queueConfig.stager["jobtype"] == "Yoda":
                self.Yodajob = True
        # set the location of the files in fileSpec.objstoreID
        # see file /cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_ddmendpoints.json
        self.objstoreID = int(queueConfig.stager["objStoreID_ES"])
        if self.Yodajob:
            self.pathConvention = int(queueConfig.stager["pathConvention"])
            tmpLog.debug("Yoda Job - PandaID = {0} objstoreID = {1} pathConvention ={2}".format(jobspec.PandaID, self.objstoreID, self.pathConvention))
        else:
            self.pathConvention = None
            tmpLog.debug("PandaID = {0} objstoreID = {1}".format(jobspec.PandaID, self.objstoreID))
        self.RSE_dstpath = queueConfig.stager["RSE_dstPath"]
        # loop over the output files and copy the files
        ifile = 0
        errors = []
        for fileSpec in jobspec.get_output_file_specs(skip_done=True):
            scope = "panda"
            if fileSpec.scope is not None:
                scope = fileSpec.scope
            # for Yoda job set the scope to transient
            if self.Yodajob:
                scope = "transient"
            # only print to log file first 25 files
            if ifile < 25:
                msgStr = "fileSpec.lfn - {0} fileSpec.scope - {1}".format(fileSpec.lfn, fileSpec.scope)
                tmpLog.debug(msgStr)
            if ifile == 25:
                msgStr = "printed first 25 files skipping the rest".format(fileSpec.lfn, fileSpec.scope)
                tmpLog.debug(msgStr)
            hash = hashlib.md5()
            hash.update("%s:%s" % (scope, fileSpec.lfn))
            hash_hex = hash.hexdigest()
            correctedscope = "/".join(scope.split("."))
            srcURL = fileSpec.path
            dstURL = "{endPoint}/{scope}/{hash1}/{hash2}/{lfn}".format(
                endPoint=self.RSE_dstPath, scope=correctedscope, hash1=hash_hex[0:2], hash2=hash_hex[2:4], lfn=fileSpec.lfn
            )
            if ifile < 25:
                tmpLog.debug("src={srcURL} dst={dstURL}".format(srcURL=srcURL, dstURL=dstURL))
            # copy the source file from source to destination skip over if file already exists
            if os.path.exists(dstURL):
                tmpLog.debug("Already copied file {0}".format(dstURL))
                # Set the file spec status
                if self.changeFileStatusOnSuccess:
                    fileSpec.status = "finished"
            else:
                if os.path.exists(srcURL):
                    # check if destination directory exists if not create it
                    dstDIR = os.path.dirname(dstURL)
                    try:
                        if not os.path.exists(dstDIR):
                            os.makedirs(dstDIR)
                            mode = stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP
                            mode = mode | stat.S_IROTH | stat.S_IXOTH | stat.S_ISGID
                            os.chmod(dstDIR, mode)
                        # copy the source file to destination file
                        shutil.copy2(srcURL, dstURL)
                        # Set the file spec status
                        if self.changeFileStatusOnSuccess:
                            self.set_FileSpec_status(jobspec, "finished")
                    except (IOError, os.error) as why:
                        errors.append((srcURL, dstURL, str(why)))
                else:
                    errors.append((srcURL, dstURL, "Source file missing"))
            ifile += 1
        #  Now test for any errors
        if errors:
            for error in errors:
                tmpLog.debug("copy error source {0} destination {1} Reason {2}".format(error[0], error[1], error[2]))
            raise Error(errors)
        # otherwise we are OK
        tmpLog.debug("stop")
        return True, ""

    # zip output files
    def zip_output(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, "PandaID={0}".format(jobspec.PandaID), method_name="zip_output")
        return self.simple_zip_output(jobspec, tmpLog)
