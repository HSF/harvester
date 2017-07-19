import json
import re
import arc

from pandaharvester.harvestercore import core_utils

class DataPoint:
    '''
    Wrapper around arc.datapoint_from_url() which does not clean up DataPoints
    when python objects are destroyed, leading to connection leaking when used
    with gridftp. This class should be used instead of arc.datapoint_from_url().
    It can be called like dp = DataPoint('gsiftp://...', uc); dp.h.Stat()
    where uc is an arc.UserConfig object.
    '''
    def __init__(self, u, uc):
        self.h = arc.datapoint_from_url(u, uc)
    def __del__(self):
        arc.DataPoint.__swig_destroy__(self.h)


def setup_logging(baselogger, workerid):
    '''Set up ARC logging to log to the same file as the baselogger'''

    tmplog = core_utils.make_logger(baselogger, 'workerID={0}'.format(workerid))

    # Get the log file from the baseLogger
    loghandler = baselogger.handlers[0] # Assumes one handler
    logfile = arc.LogFile(loghandler.baseFilename)
    logfile.setFormat(arc.LongFormat)
    arc.Logger_getRootLogger().addDestination(logfile)
    arc.Logger_getRootLogger().setThreshold(arc.VERBOSE) # TODO configurable
    # the LogFile object must be kept alive as long as we use it
    return tmplog, logfile

def workspec2arcjob(workspec):
    '''Convert WorkSpec.workAttributes to arc.Job object'''

    job = arc.Job()
    try:
        wspecattrs = json.loads(workspec.workAttributes)
        wsattrs = wspecattrs['arcjob']
    except:
        # Job was not submitted yet
        return job

    for attr in dir(job):
        if attr not in wsattrs:
            continue

        attrtype = type(getattr(job, attr))
        # Some object types need special treatment
        if attrtype == arc.StringList:
            strlist = arc.StringList()
            for item in wsattrs[attr].split('|'):
                strlist.append(str(item))
            setattr(job, attr, strlist)
        elif attrtype == arc.StringStringMap:
            ssm = arc.StringStringMap()
            for (k, v) in json.loads(wsattrs[attr]).items():
                ssm[str(k)] = str(v)
            setattr(job, attr, ssm)
        else:
            setattr(job, attr, attrtype(str(wsattrs[attr])))

    return job

def arcjob2workspec(arcjob, workspec):
    '''Fill WorkSpec workAttributes with ARC job attributes'''

    jobattrs = {}
    for attr in dir(arcjob):
        # Don't store internal python attrs or job description
        if re.match('^__', attr) or attr == 'JobDescriptionDocument':
            continue

        attrtype = type(getattr(arcjob, attr))
        if attrtype == int or attrtype == str:
            jobattrs[attr] = getattr(arcjob, attr)
        elif attrtype == arc.JobState:
            jobattrs[attr] = getattr(arcjob, attr).GetGeneralState()
        elif attrtype == arc.StringList:
            jobattrs[attr] = '|'.join(getattr(arcjob, attr))
        elif attrtype == arc.URL:
            jobattrs[attr] = getattr(arcjob, attr).str().replace(r'\2f',r'/')
        elif attrtype == arc.StringStringMap:
            ssm = getattr(arcjob, attr)
            tmpdict = dict(zip(ssm.keys(), ssm.values()))
            jobattrs[attr] = str(tmpdict)
        elif attrtype == arc.Period:
            jobattrs[attr] = getattr(arcjob, attr).GetPeriod()
        elif attrtype == arc.Time:
            if getattr(arcjob, attr).GetTime() != -1:
                jobattrs[attr] = getattr(arcjob, attr).str(arc.EpochTime)
        # Other attributes of complex types are not stored

    try:
        wsattrs = json.loads(workspec.workAttributes)
    except:
        wsattrs = {}
    wsattrs['arcjob'] = jobattrs
    workspec.workAttributes = json.dumps(wsattrs)
