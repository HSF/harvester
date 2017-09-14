import inspect
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

class ARCPandaJob:
    '''
    Class containing information on a panda job being processed by ARC plugins.
    Normally at the end of the job this information is encoded in the pickle
    file produced by the pilot and downloaded by harvester. Harvester adjusts
    certain information before sending it to panda. If the pickle is missing
    then default values are used. The fields here are those expected by
    JobDispatcher.updateJob() in the Panda server.
    '''
    
    def __init__(self, jobinfo={}, filehandle=None, filename=''):
        '''
        Make a new ARCPandaJob. jobId and state are mandatory. jobinfo is a
        dictonary which can contain any of this class' attributes. If filehandle
        is given pickled info is loaded from the file object. If filename is
        given pickled info is read from a file.
        '''
        self.jobId = None
        self.state = None
        self.timeout = 60
        
        if jobinfo:
            self.setAttributes(jobinfo)
        elif filehandle:
            self.setAttributes(pickle.load(filehandle))
        elif filename:
            with open(filename) as f:
                jobinfo = pickle.load(f)
                self.setAttributes(jobinfo)
    
    def __setattr__(self, name, value):
        '''
        Override to allow setting arbitrary key value pairs
        '''
        self.__dict__[name] = value

    def setAttributes(self, jobinfo):
        '''
        Set attributes in the jobinfo dictionary
        '''
        for key, value in jobinfo.iteritems():
            self.__dict__[key] = value     

    def dictionary(self):
        '''
        Return a dictionary of all the attributes with set values
        '''
        return self.__dict__
    
    def writeToFile(self, filename):
        '''
        Write a pickle of job info to filename. Overwrites an existing file.
        '''
        try:
            os.makedirs(os.path.dirname(filename), 0755)
        except:
            pass
        
        with open(filename, 'w') as f:
            pickle.dump(self.dictionary(), f)
            
class ARCLogger:
    '''
    Wrapper around harvester logger to add ARC logging to same log file
    '''

    def __init__(self, baselogger, workerid):
        '''Set up ARC logging to log to the same file as the baselogger'''

        # Set method name to the caller of this method
        self.log = core_utils.make_logger(baselogger,
                                          token='workerID={0}'.format(workerid),
                                          method_name=inspect.stack()[1][3])

        # Get the log file from the baseLogger
        loghandler = baselogger.handlers[0] # Assumes one handler
        # LogFile must exist for the lifetime of this object
        self.logfile = arc.LogFile(loghandler.baseFilename)
        self.logfile.setFormat(arc.LongFormat)
        arc.Logger_getRootLogger().setThreadContext()
        arc.Logger_getRootLogger().addDestination(self.logfile)
        arc.Logger_getRootLogger().setThreshold(arc.VERBOSE) # TODO configurable

    def __del__(self):
        '''
        Since self.logfile disappears when this object is deleted we have to
        remove it from the root logger destinations
        '''
        arc.Logger_getRootLogger().removeDestinations()


def workspec2arcjob(workspec):
    '''Convert WorkSpec.workAttributes to arc.Job object'''

    job = arc.Job()
    try:
        wsattrs = workspec.workAttributes['arcjob']
    except:
        # Job was not submitted yet
        return job

    for attr in dir(job):
        if attr not in wsattrs or attr == 'CreationTime':
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
                jobattrs[attr] = getattr(arcjob, attr).str(arc.UTCTime)
        # Other attributes of complex types are not stored

    if workspec.workAttributes:
        workspec.workAttributes['arcjob'] = jobattrs
    else:
        workspec.workAttributes = {'arcjob': jobattrs}
