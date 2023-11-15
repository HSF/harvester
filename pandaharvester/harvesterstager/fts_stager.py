import hashlib
import sys

import requests

# TO BE REMOVED for python2.7
import requests.packages.urllib3

try:
    requests.packages.urllib3.disable_warnings()
except BaseException:
    pass
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils

from .base_stager import BaseStager

# logger
baseLogger = core_utils.setup_logger("fts_stager")


# plugin for stager with FTS
class FtsStager(BaseStager):
    # constructor
    def __init__(self, **kwarg):
        BaseStager.__init__(self, **kwarg)

    # check status
    def check_stage_out_status(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, f"PandaID={jobspec.PandaID}", method_name="check_stage_out_status")
        tmpLog.debug("start")
        # loop over all files
        allChecked = True
        oneErrMsg = None
        trasnferStatus = {}
        for fileSpec in jobspec.outFiles:
            # get transfer ID
            transferID = fileSpec.fileAttributes["transferID"]
            if transferID not in trasnferStatus:
                # get status
                errMsg = None
                try:
                    url = f"{self.ftsServer}/jobs/{transferID}"
                    res = requests.get(
                        url, timeout=self.ftsLookupTimeout, verify=self.ca_cert, cert=(harvester_config.pandacon.cert_file, harvester_config.pandacon.key_file)
                    )
                    if res.status_code == 200:
                        transferData = res.json()
                        trasnferStatus[transferID] = transferData["job_state"]
                        tmpLog.debug(f"got {trasnferStatus[transferID]} for {transferID}")
                    else:
                        errMsg = f"StatusCode={res.status_code} {res.text}"
                except BaseException:
                    if errMsg is None:
                        errtype, errvalue = sys.exc_info()[:2]
                        errMsg = f"{errtype.__name__} {errvalue}"
                # failed
                if errMsg is not None:
                    allChecked = False
                    tmpLog.error(f"failed to get status for {transferID} with {errMsg}")
                    # set dummy not to lookup again
                    trasnferStatus[transferID] = None
                    # keep one message
                    if oneErrMsg is None:
                        oneErrMsg = errMsg
            # final status
            if trasnferStatus[transferID] == "DONE":
                fileSpec.status = "finished"
            elif trasnferStatus[transferID] in ["FAILED", "CANCELED"]:
                fileSpec.status = "failed"
        if allChecked:
            return True, ""
        else:
            return False, oneErrMsg

    # trigger stage out
    def trigger_stage_out(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, f"PandaID={jobspec.PandaID}", method_name="trigger_stage_out")
        tmpLog.debug("start")
        # default return
        tmpRetVal = (True, "")
        # loop over all files
        files = []
        lfns = set()
        fileAttrs = jobspec.get_output_file_attributes()
        for fileSpec in jobspec.outFiles:
            # skip zipped files
            if fileSpec.zipFileID is not None:
                continue
            # source and destination URLs
            if fileSpec.fileType == "es_output":
                srcURL = self.srcEndpointES + fileSpec.path
                dstURL = self.dstEndpointES + fileSpec.path
                # set OS ID
                fileSpec.objstoreID = self.esObjStoreID
            else:
                scope = fileAttrs[fileSpec.lfn]["scope"]
                hash = hashlib.md5()
                hash.update(f"{scope}:{fileSpec.lfn}")
                hash_hex = hash.hexdigest()
                correctedscope = "/".join(scope.split("."))
                if fileSpec.fileType == "output":
                    srcURL = self.srcEndpointOut + fileSpec.path
                    dstURL = f"{self.dstEndpointOut}/{correctedscope}/{hash_hex[0:2]}/{hash_hex[2:4]}/{fileSpec.lfn}"
                elif fileSpec.fileType == "log":
                    # skip if no endpoint
                    if self.srcEndpointLog is None:
                        continue
                    srcURL = self.srcEndpointLog + fileSpec.path
                    dstURL = f"{self.dstEndpointLog}/{correctedscope}/{hash_hex[0:2]}/{hash_hex[2:4]}/{fileSpec.lfn}"
                else:
                    continue
            tmpLog.debug(f"src={srcURL} dst={dstURL}")
            files.append(
                {
                    "sources": [srcURL],
                    "destinations": [dstURL],
                }
            )
            lfns.add(fileSpec.lfn)
        # submit
        if files != []:
            # get status
            errMsg = None
            try:
                url = f"{self.ftsServer}/jobs"
                res = requests.post(
                    url,
                    json={"Files": files},
                    timeout=self.ftsLookupTimeout,
                    verify=self.ca_cert,
                    cert=(harvester_config.pandacon.cert_file, harvester_config.pandacon.key_file),
                )
                if res.status_code == 200:
                    transferData = res.json()
                    transferID = transferData["job_id"]
                    tmpLog.debug(f"successfully submitted id={transferID}")
                    # set
                    for fileSpec in jobspec.outFiles:
                        if fileSpec.fileAttributes is None:
                            fileSpec.fileAttributes = {}
                        fileSpec.fileAttributes["transferID"] = transferID
                else:
                    # HTTP error
                    errMsg = f"StatusCode={res.status_code} {res.text}"
            except BaseException:
                if errMsg is None:
                    errtype, errvalue = sys.exc_info()[:2]
                    errMsg = f"{errtype.__name__} {errvalue}"
            # failed
            if errMsg is not None:
                tmpLog.error(f"failed to submit transfer to {url} with {errMsg}")
                tmpRetVal = (False, errMsg)
        # return
        tmpLog.debug("done")
        return tmpRetVal

    # zip output files
    def zip_output(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, f"PandaID={jobspec.PandaID}", method_name="zip_output")
        return self.simple_zip_output(jobspec, tmpLog)
