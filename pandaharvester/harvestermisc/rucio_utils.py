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
        tmpLog.debug(f"register {datasetScope}:{datasetName} lifetime = {lifetime}")
        executable = ["/usr/bin/env", "rucio", "add-dataset"]
        executable += ["--lifetime", ("%d" % lifetime)]
        executable += [datasetName]
        tmpLog.debug(f"rucio add-dataset command: {executable} ")
        tmpLog.debug(f"rucio add-dataset command (for human): {' '.join(executable)} ")
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
                errMsg = f"dataset {datasetScope}:{datasetName} already exists"
                tmpLog.debug(errMsg)
                return True, errMsg
            elif rucio_sessions_limit_error:
                # do nothing
                errStr = f"Rucio returned error, will retry: stdout: {stdout}"
                tmpLog.warning(errStr)
                return None, errStr
            else:
                # some other Rucio error
                errStr = f"Rucio returned error : stdout: {stdout}"
                tmpLog.error(errStr)
                return False, errStr
    except Exception as e:
        errMsg = f"Could not create dataset {datasetScope}:{datasetName} with {str(e)}"
        core_utils.dump_error_message(tmpLog)
        tmpLog.error(errMsg)
        return False, errMsg


def rucio_add_files_to_dataset(tmpLog, datasetScope, datasetName, fileList):
    # add files to dataset
    try:
        # create the to DID
        to_did = f"{datasetScope}:{datasetName}"
        executable = ["/usr/bin/env", "rucio", "attach", to_did]
        # loop over the files to add
        for filename in fileList:
            from_did = f"{filename['scope']}:{filename['name']}"
            executable += [from_did]

        # print executable
        tmpLog.debug(f"rucio attach command: {executable} ")
        tmpLog.debug(f"rucio attach command (for human): {' '.join(executable)} ")

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
                errStr = f"Rucio returned Sessions Limit error, will retry: stdout: {stdout}"
                tmpLog.warning(errStr)
                return None, errStr
            else:
                # some other Rucio error
                errStr = f"Rucio returned error : stdout: {stdout}"
                tmpLog.error(errStr)
                return False, errStr
            # except FileAlreadyExists:
            #    # ignore if files already exist
            #    pass
    except Exception:
        errMsg = f"Could not add files to DS - {datasetScope}:{datasetName} files - {fileList}"
        core_utils.dump_error_message(tmpLog)
        tmpLog.error(errMsg)
        return False, errMsg


def rucio_add_rule(tmpLog, datasetScope, datasetName, dstRSE):
    # add rule
    try:
        tmpLog.debug(f"rucio add-rule {datasetScope}:{datasetName} 1 {dstRSE}")
        did = f"{datasetScope}:{datasetName}"
        executable = ["/usr/bin/env", "rucio", "add-rule", did, "1", dstRSE]

        # print executable

        tmpLog.debug(f"rucio add-rule command: {executable} ")
        tmpLog.debug(f"rucio add-rule command (for human): {' '.join(executable)} ")

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
                errStr = f"Rucio returned error, will retry: stdout: {stdout}"
                tmpLog.warning(errStr)
                return None, errStr
            else:
                # some other Rucio error
                errStr = f"Rucio returned error : stdout: {stdout}"
                tmpLog.error(errStr)
                return False, errStr
    except Exception:
        core_utils.dump_error_message(tmpLog)
        # treat as a temporary error
        tmpStat = False
        tmpMsg = f"failed to add a rule for {datasetScope}:{datasetName}"
        return tmpStat, tmpMsg


# !!!!!!!
# Need to add files to replica. Need to write code
#
# !!!!!!!


def rucio_rule_info(tmpLog, rucioRule):
    # get rule-info
    tmpLog.debug(f"rucio rule-info {rucioRule}")
    try:
        executable = ["/usr/bin/env", "rucio", "rule-info", rucioRule]
        # print executable

        tmpLog.debug(f"rucio rule-info command: {executable} ")
        tmpLog.debug(f"rucio rule-info command (for human): {' '.join(executable)} ")

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
                errStr = f"Rucio returned error, will retry: stdout: {stdout}"
                tmpLog.warning(errStr)
                return None, errStr
            else:
                # some other Rucio error
                errStr = f"Rucio returned error : stdout: {stdout}"
                tmpLog.error(errStr)
                return False, errStr
    except Exception:
        errMsg = f"Could not run rucio rule-info {rucioRule}"
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
