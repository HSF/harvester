"""
utilities routines associated with Rucio CLI access

"""
try:
    import subprocess32 as subprocess
except BaseException:
    import subprocess

from pandaharvester.harvestercore import core_utils


def rucio_create_dataset(tmpLog, datasetScope, datasetName):
    # create the dataset
    try:
        # register dataset
        lifetime = 7 * 24 * 60 * 60
        tmpLog.debug("register {0}:{1} lifetime = {2}".format(datasetScope, datasetName, lifetime))
        executable = ["/usr/bin/env", "rucio", "add-dataset"]
        executable += ["--lifetime", ("%d" % lifetime)]
        executable += [datasetName]
        tmpLog.debug("rucio add-dataset command: {0} ".format(executable))
        tmpLog.debug("rucio add-dataset command (for human): %s " % " ".join(executable))
        process = subprocess.Popen(executable, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        stdout, stderr = process.communicate()
        if process.returncode == 0:
            tmpLog.debug(stdout)
            return True, ""
        else:
            # check what failed
            dataset_exists = False
            rucio_sessions_limit_error = False
            for line in stdout.split("\n"):
                if "Data Identifier Already Exists" in line:
                    dataset_exists = True
                    break
                elif "exceeded simultaneous SESSIONS_PER_USER limit" in line:
                    rucio_sessions_limit_error = True
                    break
            if dataset_exists:
                errMsg = "dataset {0}:{1} already exists".format(datasetScope, datasetName)
                tmpLog.debug(errMsg)
                return True, errMsg
            elif rucio_sessions_limit_error:
                # do nothing
                errStr = "Rucio returned error, will retry: stdout: {0}".format(stdout)
                tmpLog.warning(errStr)
                return None, errStr
            else:
                # some other Rucio error
                errStr = "Rucio returned error : stdout: {0}".format(stdout)
                tmpLog.error(errStr)
                return False, errStr
    except Exception as e:
        errMsg = "Could not create dataset {0}:{1} with {2}".format(datasetScope, datasetName, str(e))
        core_utils.dump_error_message(tmpLog)
        tmpLog.error(errMsg)
        return False, errMsg


def rucio_add_files_to_dataset(tmpLog, datasetScope, datasetName, fileList):
    # add files to dataset
    try:
        # create the to DID
        to_did = "{0}:{1}".format(datasetScope, datasetName)
        executable = ["/usr/bin/env", "rucio", "attach", to_did]
        # loop over the files to add
        for filename in fileList:
            from_did = "{0}:{1}".format(filename["scope"], filename["name"])
            executable += [from_did]

        # print executable
        tmpLog.debug("rucio attach command: {0} ".format(executable))
        tmpLog.debug("rucio attach command (for human): %s " % " ".join(executable))

        process = subprocess.Popen(executable, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        stdout, stderr = process.communicate()

        if process.returncode == 0:
            tmpLog.debug(stdout)
            return True, ""
        else:
            # check what failed
            rucio_sessions_limit_error = False
            for line in stdout.split("\n"):
                if "exceeded simultaneous SESSIONS_PER_USER limit" in line:
                    rucio_sessions_limit_error = True
                    break
            if rucio_sessions_limit_error:
                # do nothing
                errStr = "Rucio returned Sessions Limit error, will retry: stdout: {0}".format(stdout)
                tmpLog.warning(errStr)
                return None, errStr
            else:
                # some other Rucio error
                errStr = "Rucio returned error : stdout: {0}".format(stdout)
                tmpLog.error(errStr)
                return False, errStr
            # except FileAlreadyExists:
            #    # ignore if files already exist
            #    pass
    except Exception:
        errMsg = "Could not add files to DS - {0}:{1} files - {2}".format(datasetScope, datasetName, fileList)
        core_utils.dump_error_message(tmpLog)
        tmpLog.error(errMsg)
        return False, errMsg


def rucio_add_rule(tmpLog, datasetScope, datasetName, dstRSE):
    # add rule
    try:
        tmpLog.debug("rucio add-rule {0}:{1} 1 {2}".format(datasetScope, datasetName, dstRSE))
        did = "{0}:{1}".format(datasetScope, datasetName)
        executable = ["/usr/bin/env", "rucio", "add-rule", did, "1", dstRSE]

        # print executable

        tmpLog.debug("rucio add-rule command: {0} ".format(executable))
        tmpLog.debug("rucio add-rule command (for human): %s " % " ".join(executable))

        process = subprocess.Popen(executable, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        stdout, stderr = process.communicate()

        if process.returncode == 0:
            tmpLog.debug(stdout)
            # parse stdout for rule id
            rule_id = stdout.split("\n")[0]
            return True, rule_id
        else:
            # check what failed
            rucio_sessions_limit_error = False
            for line in stdout.split("\n"):
                if "exceeded simultaneous SESSIONS_PER_USER limit" in line:
                    rucio_sessions_limit_error = True
                    break
            if rucio_sessions_limit_error:
                # do nothing
                errStr = "Rucio returned error, will retry: stdout: {0}".format(stdout)
                tmpLog.warning(errStr)
                return None, errStr
            else:
                # some other Rucio error
                errStr = "Rucio returned error : stdout: {0}".format(stdout)
                tmpLog.error(errStr)
                return False, errStr
    except Exception:
        core_utils.dump_error_message(tmpLog)
        # treat as a temporary error
        tmpStat = False
        tmpMsg = "failed to add a rule for {0}:{1}".format(datasetScope, datasetName)
        return tmpStat, tmpMsg


# !!!!!!!
# Need to add files to replica. Need to write code
#
# !!!!!!!


def rucio_rule_info(tmpLog, rucioRule):
    # get rule-info
    tmpLog.debug("rucio rule-info {0}".format(rucioRule))
    try:
        executable = ["/usr/bin/env", "rucio", "rule-info", rucioRule]
        # print executable

        tmpLog.debug("rucio rule-info command: {0} ".format(executable))
        tmpLog.debug("rucio rule-info command (for human): %s " % " ".join(executable))

        process = subprocess.Popen(executable, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        stdout, stderr = process.communicate()

        if process.returncode == 0:
            tmpLog.debug(stdout)
            # parse the output to get the state:
            for line in stdout.split("\n"):
                if "State:" in line:
                    # get the State varible
                    result = line.split()
                    return True, result
            return None, ""
        else:
            # check what failed
            rucio_sessions_limit_error = False
            for line in stdout.split("\n"):
                if "exceeded simultaneous SESSIONS_PER_USER limit" in line:
                    rucio_sessions_limit_error = True
                    break

            if rucio_sessions_limit_error:
                # do nothing
                errStr = "Rucio returned error, will retry: stdout: {0}".format(stdout)
                tmpLog.warning(errStr)
                return None, errStr
            else:
                # some other Rucio error
                errStr = "Rucio returned error : stdout: {0}".format(stdout)
                tmpLog.error(errStr)
                return False, errStr
    except Exception:
        errMsg = "Could not run rucio rule-info {0}".format(rucioRule)
        core_utils.dump_error_message(tmpLog)
        tmpLog.error(errMsg)
        return False, errMsg


"""
[dbenjamin@atlas28 ~]$ rucio rule-info 66a88e4d468a4845adcc66a66080b710
Id:                         66a88e4d468a4845adcc66a66080b710
Account:                    pilot
Scope:                      condR2_data
Name:                       condR2_data.000016.lar.COND
RSE Expression:             CERN-PROD_HOTDISK
Copies:                     1
State:                      OK
Locks OK/REPLICATING/STUCK: 42/0/0
Grouping:                   DATASET
Expires at:                 None
Locked:                     False
Weight:                     None
Created at:                 2016-06-10 12:19:15
Updated at:                 2018-02-14 09:11:45
Error:                      None
Subscription Id:            None
Source replica expression:  None
Activity:                   User Subscriptions
Comment:                    None
Ignore Quota:               False
Ignore Availability:        False
Purge replicas:             False
Notification:               NO
End of life:                None
Child Rule Id:              None
"""
