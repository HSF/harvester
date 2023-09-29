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
from globus_sdk import TransferClient
from globus_sdk import TransferData
from globus_sdk import NativeAppAuthClient
from globus_sdk import RefreshTokenAuthorizer

from pandaharvester.harvestercore import core_utils
from pandalogger.PandaLogger import PandaLogger
from pandalogger.LogWrapper import LogWrapper

# handle exception from globus software


def handle_globus_exception(tmp_log):
    if not isinstance(tmp_log, LogWrapper):
        methodName = "{0} : ".format(inspect.stack()[1][3])
    else:
        methodName = ""
    # extract errtype and check if it a GlobusError Class
    errtype, errvalue = sys.exc_info()[:2]
    errStat = None
    errMsg = "{0} {1} ".format(methodName, errtype.__name__)
    if isinstance(errvalue, GlobusAPIError):
        # Error response from the REST service, check the code and message for
        # details.
        errStat = None
        errMsg += "HTTP status code: {0} Error Code: {1} Error Message: {2} ".format(errvalue.http_status, errvalue.code, errvalue.message)
    elif isinstance(errvalue, TransferAPIError):
        err_args = list(errvalue._get_args())
        errStat = None
        errMsg += " http_status: {0} code: {1} message: {2} requestID: {3} ".format(err_args[0], err_args[1], err_args[2], err_args[3])
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
    # errMsg += traceback.format_exc()
    tmp_log.error(errMsg)
    return (errStat, errMsg)


# Globus create transfer client


def create_globus_transfer_client(tmpLog, globus_client_id, globus_refresh_token):
    """
    create Globus Transfer Client and return the transfer client
    """
    # get logger
    tmpLog.info("Creating instance of GlobusTransferClient")
    # start the Native App authentication process
    # use the refresh token to get authorizer
    # create the Globus Transfer Client
    tc = None
    ErrStat = True
    try:
        client = NativeAppAuthClient(client_id=globus_client_id)
        authorizer = RefreshTokenAuthorizer(refresh_token=globus_refresh_token, auth_client=client)
        tc = TransferClient(authorizer=authorizer)
    except BaseException:
        errStat, errMsg = handle_globus_exception(tmpLog)
    return ErrStat, tc


def check_endpoint_activation(tmpLog, tc, endpoint_id):
    """
    check if endpoint is activated
    """
    # test we have a Globus Transfer Client
    if not tc:
        errStr = "failed to get Globus Transfer Client"
        tmpLog.error(errStr)
        return False, errStr
    try:
        endpoint = tc.get_endpoint(endpoint_id)
        r = tc.endpoint_autoactivate(endpoint_id, if_expires_in=3600)

        tmpLog.info("Endpoint - %s - activation status code %s" % (endpoint["display_name"], str(r["code"])))
        if r["code"] == "AutoActivationFailed":
            errStr = "Endpoint({0}) Not Active! Error! Source message: {1}".format(endpoint_id, r["message"])
            tmpLog.debug(errStr)
            return False, errStr
        elif r["code"] == "AutoActivated.CachedCredential":
            errStr = "Endpoint({0}) autoactivated using a cached credential.".format(endpoint_id)
            tmpLog.debug(errStr)
            return True, errStr
        elif r["code"] == "AutoActivated.GlobusOnlineCredential":
            errStr = "Endpoint({0}) autoactivated using a built-in Globus ".format(endpoint_id)
            tmpLog.debug(errStr)
            return True, errStr
        elif r["code"] == "AlreadyActivated":
            errStr = "Endpoint({0}) already active until at least {1}".format(endpoint_id, 3600)
            tmpLog.debug(errStr)
            return True, errStr
    except BaseException:
        errStat, errMsg = handle_globus_exception(tmpLog)
        return errStat, {}


# get transfer tasks


def get_transfer_task_by_id(tmpLog, tc, transferID=None):
    # test we have a Globus Transfer Client
    if not tc:
        errStr = "failed to get Globus Transfer Client"
        tmpLog.error(errStr)
        return False, errStr
    if transferID is None:
        # error need to have task ID
        errStr = "failed to provide transfer task ID "
        tmpLog.error(errStr)
        return False, errStr
    try:
        # execute
        gRes = tc.get_task(transferID)
        # parse output
        tasks = {}
        tasks[transferID] = gRes
        # return
        tmpLog.debug("got {0} tasks".format(len(tasks)))
        return True, tasks
    except BaseException:
        errStat, errMsg = handle_globus_exception(tmpLog)
        return errStat, {}


# get transfer tasks


def get_transfer_tasks(tmpLog, tc, label=None):
    # test we have a Globus Transfer Client
    if not tc:
        errStr = "failed to get Globus Transfer Client"
        tmpLog.error(errStr)
        return False, errStr
    try:
        # execute
        if label is None:
            params = {"filter": "type:TRANSFER/status:SUCCEEDED,INACTIVE,FAILED,SUCCEEDED"}
            gRes = tc.task_list(num_results=1000, **params)
        else:
            params = {"filter": "type:TRANSFER/status:SUCCEEDED,INACTIVE,FAILED,SUCCEEDED/label:{0}".format(label)}
            gRes = tc.task_list(**params)
        # parse output
        tasks = {}
        for res in gRes:
            reslabel = res.data["label"]
            tasks[reslabel] = res.data
        # return
        tmpLog.debug("got {0} tasks".format(len(tasks)))
        return True, tasks
    except BaseException:
        errStat, errMsg = handle_globus_exception(tmpLog)
        return errStat, {}
