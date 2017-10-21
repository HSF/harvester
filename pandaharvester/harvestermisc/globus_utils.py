"""
utilities routines associated with globus

"""
import sys
import inspect
import traceback
from globus_sdk import GlobusAPIError
from globus_sdk import TransferAPIError
from globus_sdk import NetworkError
from globus_sdk import GlobusError
from globus_sdk import GlobusConnectionError
from globus_sdk import GlobusTimeoutError

from pandaharvester.harvestercore import core_utils
from pandalogger.PandaLogger import PandaLogger
from pandalogger.LogWrapper import LogWrapper

# handle exception from globus software
def handle_globus_exception(tmp_log):
    if not isinstance(tmp_log, LogWrapper):
        methodName = '{0} : '.format(inspect.stack()[1][3])
    else:
        methodName = ''
    # extract errtype and check if it a GlobusError Class
    errtype, errvalue = sys.exc_info()[:2]
    errStat = None
    errMsg = "{0} {1} ".format(methodName, errtype.__name__)
    if isinstance(errvalue, GlobusAPIError):
        # Error response from the REST service, check the code and message for
        # details.
        errStat = None
        errMsg += "HTTP status code: {0} Error Code: {1} Error Message: {2} ".format(errvalue.http_status,
                                                                                      errvalue.code, 
                                                                                      errvalue.message)
    elif isinstance(errvalue, TransferAPIError):
        err_args = list(errvalue._get_args())
        errStat = None    
        errMsg += " http_status: {0} code: {1} message: {2} requestID: {3} ".format(err_args[0],
                                                                                     err_args[1],
                                                                                     err_args[2],
                                                                                     err_args[3])
    elif isinstance(errvalue, NetworkError):
        errStat = None    
        errMsg += "Network Failure. Possibly a firewall or connectivity issue "
    elif isinstance(errvalue, GlobusConnectionError):
        errStat = None    
        errMsg += "A connection error occured while making a REST request. "
    elif isinstance(errvalue, GlobusTimeoutError):
        errStat = None    
        errMsg += "A REST request timeout. "
    elif isinstance(errvalue, GlobusError):
        errStat = False
        errMsg += "Totally unexpected GlobusError! "
    else:  # some other error
        errStat = False
        errMsg = "{0} ".format(errvalue)
    #errMsg += traceback.format_exc()
    tmp_log.error(errMsg)
    return (errStat,errMsg)
