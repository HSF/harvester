import os
import uuid
import time
import multiprocessing
import tempfile
import gc
from concurrent.futures import ThreadPoolExecutor as Pool

try:
    import subprocess32 as subprocess
except ImportError:
    import subprocess

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase


# base class for zipper plugin
class BaseZipper(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        self.zipDir = "${SRCDIR}"
        self.zip_tmp_log = None
        self.zip_jobSpec = None
        PluginBase.__init__(self, **kwarg)

    # zip output files
    def simple_zip_output(self, jobspec, tmp_log):
        tmp_log.debug("start")
        self.zip_tmp_log = tmp_log
        self.zip_jobSpec = jobspec
        argDictList = []
        try:
            for fileSpec in jobspec.outFiles:
                if self.zipDir == "${SRCDIR}":
                    # the same directory as src
                    zipDir = os.path.dirname(next(iter(fileSpec.associatedFiles)).path)
                elif self.zipDir == "${WORKDIR}":
                    # work dir
                    workSpec = jobspec.get_workspec_list()[0]
                    zipDir = workSpec.get_access_point()
                else:
                    zipDir = self.zipDir
                zipPath = os.path.join(zipDir, fileSpec.lfn)
                argDict = dict()
                argDict["zipPath"] = zipPath
                argDict["associatedFiles"] = []
                for assFileSpec in fileSpec.associatedFiles:
                    if os.path.exists(assFileSpec.path):
                        argDict["associatedFiles"].append(assFileSpec.path)
                    else:
                        assFileSpec.status = "failed"
                argDictList.append(argDict)
            # parallel execution
            try:
                if hasattr(harvester_config, "zipper"):
                    nThreadsForZip = harvester_config.zipper.nThreadsForZip
                else:
                    nThreadsForZip = harvester_config.stager.nThreadsForZip
            except Exception:
                nThreadsForZip = multiprocessing.cpu_count()
            with Pool(max_workers=nThreadsForZip) as pool:
                retValList = pool.map(self.make_one_zip, argDictList)
                # check returns
                for fileSpec, retVal in zip(jobspec.outFiles, retValList):
                    tmpRet, errMsg, fileInfo = retVal
                    if tmpRet is True:
                        # set path
                        fileSpec.path = fileInfo["path"]
                        fileSpec.fsize = fileInfo["fsize"]
                        fileSpec.chksum = fileInfo["chksum"]
                        msgStr = "fileSpec.path - {0}, fileSpec.fsize - {1}, fileSpec.chksum(adler32) - {2}".format(
                            fileSpec.path, fileSpec.fsize, fileSpec.chksum
                        )
                        tmp_log.debug(msgStr)
                    else:
                        tmp_log.error("got {0} with {1} when zipping {2}".format(tmpRet, errMsg, fileSpec.lfn))
                        return tmpRet, "failed to zip with {0}".format(errMsg)
        except Exception:
            errMsg = core_utils.dump_error_message(tmp_log)
            return False, "failed to zip with {0}".format(errMsg)
        tmp_log.debug("done")
        return True, ""

    # make one zip file
    def make_one_zip(self, arg_dict):
        try:
            zipPath = arg_dict["zipPath"]
            lfn = os.path.basename(zipPath)
            self.zip_tmp_log.debug("{0} start zipPath={1} with {2} files".format(lfn, zipPath, len(arg_dict["associatedFiles"])))
            # make zip if doesn't exist
            if not os.path.exists(zipPath):
                # tmp file names
                tmpZipPath = zipPath + "." + str(uuid.uuid4())
                tmpZipPathIn = tmpZipPath + ".in"
                with open(tmpZipPathIn, "w") as f:
                    for associatedFile in arg_dict["associatedFiles"]:
                        f.write("{0}\n".format(associatedFile))
                # make command
                com = "tar -c -f {0} -T {1} ".format(tmpZipPath, tmpZipPathIn)
                com += "--transform 's/.*\///' "
                # execute
                p = subprocess.Popen(com, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                stdOut, stdErr = p.communicate()
                retCode = p.returncode
                if retCode != 0:
                    msgStr = "failed to make zip for {0} with {1}:{2}".format(lfn, stdOut, stdErr)
                    self.zip_tmp_log.error(msgStr)
                    return None, msgStr, {}
                # avoid overwriting
                lockName = "zip.lock.{0}".format(lfn)
                lockInterval = 60
                tmpStat = False
                # get lock
                for i in range(lockInterval):
                    tmpStat = self.dbInterface.get_object_lock(lockName, lock_interval=lockInterval)
                    if tmpStat:
                        break
                    time.sleep(1)
                # failed to lock
                if not tmpStat:
                    msgStr = "failed to lock for {0}".format(lfn)
                    self.zip_tmp_log.error(msgStr)
                    return None, msgStr
                if not os.path.exists(zipPath):
                    os.rename(tmpZipPath, zipPath)
                # release lock
                self.dbInterface.release_object_lock(lockName)
            # make return
            fileInfo = dict()
            fileInfo["path"] = zipPath
            # get size
            statInfo = os.stat(zipPath)
            fileInfo["fsize"] = statInfo.st_size
            fileInfo["chksum"] = core_utils.calc_adler32(zipPath)
        except Exception:
            errMsg = core_utils.dump_error_message(self.zip_tmp_log)
            return False, "failed to zip with {0}".format(errMsg)
        self.zip_tmp_log.debug("{0} done".format(lfn))
        return True, "", fileInfo

    # zip output files; file operations are done on remote side with ssh
    def ssh_zip_output(self, jobspec, tmp_log):
        tmp_log.debug("start")
        self.zip_tmp_log = tmp_log
        self.zip_jobSpec = jobspec
        argDictList = []
        outFiles_list = list(jobspec.outFiles)
        try:
            try:
                if hasattr(harvester_config, "zipper"):
                    nThreadsForZip = harvester_config.zipper.nThreadsForZip
                else:
                    nThreadsForZip = harvester_config.stager.nThreadsForZip
            except Exception:
                nThreadsForZip = multiprocessing.cpu_count()
            # check associate file existence

            def _check_assfile_existence(fileSpec):
                in_data = "\\n".join(["{0}".format(assFileSpec.path) for assFileSpec in fileSpec.associatedFiles])
                com1 = (
                    "ssh "
                    "-o StrictHostKeyChecking=no "
                    "-i {sshkey} "
                    "{userhost} "
                    '"{fileop_script} write_tmpfile --suffix {suffix} --dir {dir} \\"{data}\\" "'
                ).format(
                    sshkey=self.sshkey,
                    userhost=self.userhost,
                    fileop_script=self.fileop_script,
                    suffix="_check-exist.tmp",
                    dir=os.path.dirname(next(iter(fileSpec.associatedFiles)).path),
                    data=in_data,
                )
                # execute
                p1 = subprocess.Popen(com1, shell=True, close_fds=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                stdOut, stdErr = p1.communicate()
                retCode = p1.returncode
                if retCode != 0:
                    msgStr = "failed to make tmpargfile remotely with {0}:{1}".format(stdOut, stdErr)
                    tmp_log.error(msgStr)
                    return False, "failed to zip with {0}".format(msgStr)
                stdOut_str = stdOut if (isinstance(stdOut, str) or stdOut is None) else stdOut.decode()
                tmpargfile_name = stdOut_str.strip("\n")
                del p1, stdOut, stdErr
                # record set
                existence_set = set()
                # make command
                com2 = (
                    "ssh "
                    "-o StrictHostKeyChecking=no "
                    "-i {sshkey} "
                    "{userhost} "
                    "\"cat {arg_file} | xargs -I%% sh -c ' test -f %% && echo T || echo F ' \" "
                ).format(
                    sshkey=self.sshkey,
                    userhost=self.userhost,
                    arg_file=tmpargfile_name,
                )
                # execute
                p2 = subprocess.Popen(com2, shell=True, close_fds=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                stdOut, stdErr = p2.communicate()
                retCode = p2.returncode
                if retCode != 0:
                    msgStr = "failed to existence of associate files with {0}:{1}".format(stdOut, stdErr)
                    tmp_log.error(msgStr)
                else:
                    try:
                        stdOut_str = stdOut if (isinstance(stdOut, str) or stdOut is None) else stdOut.decode()
                        ret_list = stdOut_str.strip("\n").split("\n")
                        if len(fileSpec.associatedFiles) == len(ret_list):
                            for assFileSpec, retVal in zip(fileSpec.associatedFiles, ret_list):
                                if retVal == "T":
                                    existence_set.add(assFileSpec.path)
                        else:
                            msgStr = "returned number of files inconsistent! Skipped..."
                            tmp_log.error(msgStr)
                    except Exception:
                        core_utils.dump_error_message(tmp_log)
                del p2, stdOut, stdErr, com2
                # delete tmpargfile
                com3 = ("ssh " "-o StrictHostKeyChecking=no " "-i {sshkey} " "{userhost} " '"{fileop_script} remove_file {file_path} "').format(
                    sshkey=self.sshkey,
                    userhost=self.userhost,
                    fileop_script=self.fileop_script,
                    file_path=tmpargfile_name,
                )
                # execute
                p3 = subprocess.Popen(com3, shell=True, close_fds=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                stdOut, stdErr = p3.communicate()
                retCode = p3.returncode
                if retCode != 0:
                    msgStr = "failed to delete tmpargfile remotely with {0}:{1}".format(stdOut, stdErr)
                    tmp_log.error(msgStr)
                del p3, stdOut, stdErr
                gc.collect()
                return existence_set

            # parallel execution of check existence
            with Pool(max_workers=nThreadsForZip) as pool:
                existence_set_list = pool.map(_check_assfile_existence, outFiles_list)
            # loop
            for fileSpec, existence_set in zip(outFiles_list, existence_set_list):
                if self.zipDir == "${SRCDIR}":
                    # the same directory as src
                    zipDir = os.path.dirname(next(iter(fileSpec.associatedFiles)).path)
                elif self.zipDir == "${WORKDIR}":
                    # work dir
                    workSpec = jobspec.get_workspec_list()[0]
                    zipDir = workSpec.get_access_point()
                else:
                    zipDir = self.zipDir
                zipPath = os.path.join(zipDir, fileSpec.lfn)
                argDict = dict()
                argDict["zipPath"] = zipPath
                argDict["associatedFiles"] = []
                # check existence of files
                for assFileSpec in fileSpec.associatedFiles:
                    if assFileSpec.path in existence_set:
                        argDict["associatedFiles"].append(assFileSpec.path)
                    else:
                        assFileSpec.status = "failed"
                # append
                argDictList.append(argDict)
            # parallel execution of zip
            with Pool(max_workers=nThreadsForZip) as pool:
                retValList = pool.map(self.ssh_make_one_zip, argDictList)
                # check returns
                for fileSpec, retVal in zip(jobspec.outFiles, retValList):
                    tmpRet, errMsg, fileInfo = retVal
                    if tmpRet is True:
                        # set path
                        fileSpec.path = fileInfo["path"]
                        fileSpec.fsize = fileInfo["fsize"]
                        fileSpec.chksum = fileInfo["chksum"]
                        msgStr = "fileSpec.path - {0}, fileSpec.fsize - {1}, fileSpec.chksum(adler32) - {2}".format(
                            fileSpec.path, fileSpec.fsize, fileSpec.chksum
                        )
                        tmp_log.debug(msgStr)
                    else:
                        tmp_log.error("got {0} with {1} when zipping {2}".format(tmpRet, errMsg, fileSpec.lfn))
                        return tmpRet, "failed to zip with {0}".format(errMsg)
        except Exception:
            errMsg = core_utils.dump_error_message(tmp_log)
            return False, "failed to zip with {0}".format(errMsg)
        tmp_log.debug("done")
        return True, ""

    # make one zip file; file operations are done on remote side with ssh
    def ssh_make_one_zip(self, arg_dict):
        try:
            zipPath = arg_dict["zipPath"]
            lfn = os.path.basename(zipPath)
            self.zip_tmp_log.debug("{0} start zipPath={1} with {2} files".format(lfn, zipPath, len(arg_dict["associatedFiles"])))
            in_data = "\\n".join(["{0}".format(path) for path in arg_dict["associatedFiles"]])
            com0 = (
                "ssh " "-o StrictHostKeyChecking=no " "-i {sshkey} " "{userhost} " '"{fileop_script} write_tmpfile --suffix {suffix} --dir {dir} \\"{data}\\" "'
            ).format(
                sshkey=self.sshkey,
                userhost=self.userhost,
                fileop_script=self.fileop_script,
                suffix="_tar-name.tmp",
                dir=os.path.dirname(zipPath),
                data=in_data,
            )
            # execute
            p0 = subprocess.Popen(com0, shell=True, close_fds=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdOut, stdErr = p0.communicate()
            retCode = p0.returncode
            if retCode != 0:
                msgStr = "failed to make tmpargfile remotely with {0}:{1}".format(stdOut, stdErr)
                tmp_log.error(msgStr)
                return False, "failed to zip with {0}".format(msgStr)
            stdOut_str = stdOut if (isinstance(stdOut, str) or stdOut is None) else stdOut.decode()
            tmpargfile_name = stdOut_str.strip("\n")
            del p0, stdOut, stdErr
            # tmp zip file names
            tmpZipPath = zipPath + "." + str(uuid.uuid4())
            com1 = (
                "ssh "
                "-o StrictHostKeyChecking=no "
                "-i {sshkey} "
                "{userhost} "
                "\"test -f {tmpZipPath} || tar -cf {tmpZipPath} -T {arg_file} --transform 's;.*/;;' \""
            ).format(
                sshkey=self.sshkey,
                userhost=self.userhost,
                tmpZipPath=tmpZipPath,
                arg_file=tmpargfile_name,
            )
            # execute
            p1 = subprocess.Popen(com1, shell=True, close_fds=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdOut, stdErr = p1.communicate()
            retCode = p1.returncode
            if retCode != 0:
                msgStr = "failed to make zip for {0} with {1}:{2}".format(lfn, stdOut, stdErr)
                self.zip_tmp_log.error(msgStr)
                return None, msgStr, {}
            del p1, stdOut, stdErr
            # delete tmpargfile
            com1a = ("ssh " "-o StrictHostKeyChecking=no " "-i {sshkey} " "{userhost} " '"{fileop_script} remove_file {file_path} "').format(
                sshkey=self.sshkey,
                userhost=self.userhost,
                fileop_script=self.fileop_script,
                file_path=tmpargfile_name,
            )
            # execute
            p1a = subprocess.Popen(com1a, shell=True, close_fds=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdOut, stdErr = p1a.communicate()
            retCode = p1a.returncode
            if retCode != 0:
                msgStr = "failed to delete tmpargfile remotely with {0}:{1}".format(stdOut, stdErr)
                tmp_log.error(msgStr)
            del p1a, stdOut, stdErr
            gc.collect()
            # avoid overwriting
            lockName = "zip.lock.{0}".format(lfn)
            lockInterval = 60
            tmpStat = False
            # get lock
            for i in range(lockInterval):
                tmpStat = self.dbInterface.get_object_lock(lockName, lock_interval=lockInterval)
                if tmpStat:
                    break
                time.sleep(1)
            # failed to lock
            if not tmpStat:
                msgStr = "failed to lock for {0}".format(lfn)
                self.zip_tmp_log.error(msgStr)
                return None, msgStr, {}
            # rename to be zipPath
            com2 = ("ssh " "-o StrictHostKeyChecking=no " "-i {sshkey} " "{userhost} " '"test -f {zipPath} || mv {tmpZipPath} {zipPath}"').format(
                sshkey=self.sshkey,
                userhost=self.userhost,
                zipPath=zipPath,
                tmpZipPath=tmpZipPath,
            )
            p2 = subprocess.Popen(com2, shell=True, close_fds=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            p2.communicate()
            del p2
            gc.collect()
            # release lock
            self.dbInterface.release_object_lock(lockName)
            # make return
            fileInfo = dict()
            fileInfo["path"] = zipPath
            # get size
            com3 = ("ssh " "-o StrictHostKeyChecking=no " "-i {sshkey} " "{userhost} " '"stat -c %s {zipPath}"').format(
                sshkey=self.sshkey,
                userhost=self.userhost,
                zipPath=zipPath,
            )
            p3 = subprocess.Popen(com3, shell=True, close_fds=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdOut, stdErr = p3.communicate()
            retCode = p3.returncode
            if retCode != 0:
                msgStr = "failed to get file size of {0} with {1}:{2}".format(zipPath, stdOut, stdErr)
                self.zip_tmp_log.error(msgStr)
                return None, msgStr, {}
            else:
                stdOut_str = stdOut if (isinstance(stdOut, str) or stdOut is None) else stdOut.decode()
                file_size = int(stdOut_str.strip("\n"))
                fileInfo["fsize"] = file_size
            del p3, stdOut, stdErr
            gc.collect()
            # get checksum
            com4 = ("ssh " "-o StrictHostKeyChecking=no " "-i {sshkey} " "{userhost} " '"{fileop_script} adler32 {zipPath}"').format(
                sshkey=self.sshkey,
                userhost=self.userhost,
                fileop_script=self.fileop_script,
                zipPath=zipPath,
            )
            p4 = subprocess.Popen(com4, shell=True, close_fds=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdOut, stdErr = p4.communicate()
            retCode = p4.returncode
            if retCode != 0:
                msgStr = "failed to get file adler32 of {0} with {1}:{2}".format(zipPath, stdOut, stdErr)
                self.zip_tmp_log.error(msgStr)
                return None, msgStr, {}
            else:
                stdOut_str = stdOut if (isinstance(stdOut, str) or stdOut is None) else stdOut.decode()
                file_chksum = stdOut_str.strip("\n")
                fileInfo["chksum"] = file_chksum
            del p4, stdOut, stdErr
            gc.collect()
        except Exception:
            errMsg = core_utils.dump_error_message(self.zip_tmp_log)
            return False, "failed to zip with {0}".format(errMsg)
        self.zip_tmp_log.debug("{0} done".format(lfn))
        return True, "", fileInfo
