import re
from .base_extractor import BaseExtractor


# extractor for analysis
class AnalysisExtractor(BaseExtractor):
    # constructor
    def __init__(self, **kwarg):
        BaseExtractor.__init__(self, **kwarg)

    # get auxiliary input files
    def get_aux_inputs(self, jobspec):
        url_list = []
        jobPars = jobspec.jobParams['jobPars']
        # transformation
        trf = jobspec.jobParams['transformation']
        if trf is not None and trf.startswith('http'):
            url_list.append(trf)
        # extract source URL
        tmpM = re.search(' --sourceURL\s+([^\s]+)', jobPars)
        if tmpM is not None:
            sourceURL = tmpM.group(1)
            # extract sandbox
            if jobspec.jobParams['prodSourceLabel'] == 'user':
                tmpM = re.search('-a\s+([^\s]+)', jobPars)
            else:
                tmpM = re.search('-i\s+([^\s]+)', jobPars)
            if tmpM is not None:
                lfn = tmpM.group(1)
                url = '{0}/cache/{1}'.format(sourceURL, lfn)
                url_list.append(url)
        # extract container image
        tmpM = re.search(' --containerImage\s+([^\s]+)', jobPars)
        if tmpM is not None:
            url = tmpM.group(1)
            url_list.append(url)
        return self.make_aux_inputs(url_list)
