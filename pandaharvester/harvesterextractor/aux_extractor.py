import re
from .base_extractor import BaseExtractor


# extractor for auxiliary input files
class AuxExtractor(BaseExtractor):
    # constructor
    def __init__(self, **kwarg):
        self.containerPrefix = None
        BaseExtractor.__init__(self, **kwarg)

    # get auxiliary input files
    def get_aux_inputs(self, jobspec):
        url_list = []
        jobPars = jobspec.jobParams["jobPars"]
        # transformation
        trf = jobspec.jobParams["transformation"]
        if trf is not None and trf.startswith("http"):
            url_list.append(trf)
        # extract source URL
        tmpM = re.search(" --sourceURL\s+([^\s]+)", jobPars)
        if tmpM is not None:
            sourceURL = tmpM.group(1)
            jobspec.jobParams["sourceURL"] = sourceURL
            # extract sandbox
            if jobspec.jobParams["prodSourceLabel"] == "user":
                tmpM = re.search("-a\s+([^\s]+)", jobPars)
            else:
                tmpM = re.search("-i\s+([^\s]+)", jobPars)
            if tmpM is not None:
                lfn = tmpM.group(1)
                url = "{0}/cache/{1}".format(sourceURL, lfn)
                url_list.append(url)
        # extract container image
        if "container_name" in jobspec.jobParams:
            url = jobspec.jobParams["container_name"]
            if self.containerPrefix is not None and not url.startswith(self.containerPrefix):
                url = self.containerPrefix + url
            url_list.append(url)
        return self.make_aux_inputs(url_list)
