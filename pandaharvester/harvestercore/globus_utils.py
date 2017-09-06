"""
utilities routines associated with globus

"""
import sys
import inspect
import traceback
from globus_sdk import GlobusAPIError
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
    err_str = "{0} {1} ".format(methodName, errtype.__name__)
    if isinstance(errvalue,GlobusAPIError):
        # Error response from the REST service, check the code and message for
        # details.
        err_str += "Error Code: {0} Error Message: {1} "
                   .format(errvalue.code, errvalue.message)        
    else if isinstance(errvalue, NetworkError):
        err_str += "Network Failure. Possibly a firewall or connectivity issue "
    else if isinstance(errvalue,  GlobusConnectionError):
        err_str += "A connection error occured while making a REST request. "
    else if isinstance(errvalue,  GlobusTimeoutError):
        err_str += "A REST request timeout. "
    else if isinstance(errvalue,  GlobusError):
        err_str += "Totally unexpected GlobusError! "
    else :  # some other error
        err_str = "{0} ".format(errvalue)
    err_str += traceback.format_exc()
    tmp_log.error(err_str)
    return err_str
