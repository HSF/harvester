import urllib
import json
import os
import re


class ARCParser:
    """Converts panda job description to ARC job description using AGIS info"""

    def __init__(self, jobdesc, pandaqueue, siteinfo, logurl, schedulerid, osmap, tmpdir, eventranges, log):
        self.log = log
        # The URL-encoded job that is eventually passed to the pilot
        self.pandajob = urllib.urlencode(jobdesc)
        # The json of the job description
        self.jobdesc = jobdesc
        self.pandaid = self.jobdesc["PandaID"]
        self.xrsl = {}
        self.siteinfo = siteinfo
        self.ncores = siteinfo["corecount"]
        self.logurl = logurl
        self.schedulerid = schedulerid

        self.defaults = {}
        self.defaults["memory"] = 2000
        self.defaults["cputime"] = 2 * 1440 * 60
        self.sitename = siteinfo["panda_resource"]
        self.schedconfig = pandaqueue
        self.truepilot = "mv" not in siteinfo["copytools"] or len(siteinfo["copytools"]) > 1
        self.osmap = osmap
        # Set to 1 week if not specified
        self.maxwalltime = siteinfo["maxtime"] / 60
        if self.maxwalltime == 0:
            self.maxwalltime = 7 * 24 * 60

        self.tmpdir = tmpdir
        self.inputfiledir = os.path.join(self.tmpdir, "inputfiles")
        self.inputjobdir = os.path.join(self.inputfiledir, str(self.pandaid))
        self.eventranges = eventranges
        self.longjob = False
        self.traces = []
        if len(self.pandajob) > 50000:
            self.longjob = True
        self.artes = None
        self.cvmfs = siteinfo["is_cvmfs"]

        # ES merge jobs need unique guids because pilot uses them as dict keys
        if not self.truepilot and self.jobdesc.get("eventServiceMerge") == "True":
            if self.pandajob.startswith("GUID"):
                esjobdesc = self.pandajob[self.pandajob.find("&") :]
            else:
                esjobdesc = self.pandajob[: self.pandajob.find("&GUID")] + self.pandajob[self.pandajob.find("&", self.pandajob.find("&GUID") + 5) :]
            esjobdesc += "&GUID=%s" % "%2C".join(["DUMMYGUID%i" % i for i in range(len(self.jobdesc["GUID"].split(",")))])
            self.pandajob = esjobdesc

    def getNCores(self):
        # Unified panda queues: always use coreCount from job description
        try:
            self.ncores = int(self.jobdesc.get("coreCount", 1))
        except BaseException:  # corecount is NULL
            self.ncores = 1
        self.xrsl["count"] = "(count=%d)" % self.ncores

        # force single-node jobs for now
        if self.ncores > 1:
            self.xrsl["countpernode"] = "(runtimeenvironment = APPS/HEP/ATLAS-MULTICORE-1.0)"
            if self.sitename.find("RAL-LCG2") < 0 and self.sitename.find("TOKYO") < 0 and self.sitename.find("FZK") < 0:
                self.xrsl["countpernode"] = "(runtimeenvironment = APPS/HEP/ATLAS-MULTICORE-1.0)"

        return self.ncores

    def setJobname(self):
        if "jobName" in self.jobdesc:
            jobname = self.jobdesc["jobName"]
        else:
            jobname = "pandajob"
        self.xrsl["jobname"] = '(jobname = "%s")' % jobname

    def setDisk(self):
        if "maxDiskCount" in self.jobdesc:
            disk = int(self.jobdesc["maxDiskCount"])
        else:
            disk = 500

        # Add input file sizes
        if "fsize" in self.jobdesc:
            disk += sum([int(f) for f in self.jobdesc["fsize"].split(",")]) / 1000000
        # Add safety factor
        disk += 2000
        self.log.debug("%s: disk space %d" % (self.pandaid, disk))
        self.xrsl["disk"] = "(disk = %d)" % disk

    def setTime(self):
        if "maxCpuCount" in self.jobdesc:
            cpucount = int(self.jobdesc["maxCpuCount"])

            # hack for group production!!!
            if cpucount == 600:
                cpucount = 24 * 3600

            cpucount = int(2 * cpucount)
            self.log.info("%s: job maxCpuCount %s" % (self.pandaid, cpucount))
        else:
            cpucount = 2 * 24 * 3600
            self.log.info("%s: Using default maxCpuCount %s" % (self.pandaid, cpucount))

        if cpucount == 0:
            cpucount = 2 * 24 * 3600

        # shorten installation jobs
        try:
            if self.jobdesc["prodSourceLabel"] == "install":
                cpucount = 12 * 3600
        except BaseException:
            pass

        if int(cpucount) <= 0:
            cpucount = self.defaults["cputime"]

        walltime = int(cpucount / 60)

        # JEDI analysis hack
        walltime = max(60, walltime)
        walltime = min(self.maxwalltime, walltime)
        if self.sitename.startswith("BOINC"):
            walltime = min(240, walltime)
        cputime = self.getNCores() * walltime
        self.log.info("%s: walltime: %d, cputime: %d" % (self.pandaid, walltime, cputime))

        self.xrsl["time"] = "(walltime=%d)(cputime=%d)" % (walltime, cputime)

    def setMemory(self):
        if "minRamCount" in self.jobdesc:
            memory = int(self.jobdesc["minRamCount"])
        elif not self.sitename.startswith("ANALY"):
            memory = 4000
        else:
            memory = 2000

        if memory <= 0:
            memory = self.defaults["memory"]

        # fix until maxrrs in pandajob is better known
        if memory <= 500:
            memory = 500

        if "BOINC" in self.sitename:
            memory = 2400

        if self.getNCores() > 1:
            # hack for 0 ramcount, defaulting to 4000, see above, fix to 2000/core
            if memory == 4000:
                memory = 2000
            else:
                memory = memory / self.getNCores()

        # fix memory to 500MB units
        memory = int(memory - 1) / 500 * 500 + 500

        self.xrsl["memory"] = "(memory = %d)" % (memory)

    def setRTE(self):
        self.artes = ""
        if self.truepilot:
            self.xrsl["rtes"] = "(runtimeenvironment = ENV/PROXY)(runtimeenvironment = APPS/HEP/ATLAS-SITE-LCG)"
            return
        if self.siteinfo["type"] == "analysis":
            # Require proxy for analysis
            self.xrsl["rtes"] = "(runtimeenvironment = ENV/PROXY)(runtimeenvironment = APPS/HEP/ATLAS-SITE)"
            return
        if self.cvmfs:
            # Normal sites with cvmfs
            self.xrsl["rtes"] = "(runtimeenvironment = APPS/HEP/ATLAS-SITE)"
            return

        # Old-style RTEs for special sites with no cvmfs
        atlasrtes = []
        for package, cache in zip(self.jobdesc["swRelease"].split("\n"), self.jobdesc["homepackage"].split("\n")):
            if cache.find("Production") > 1 and cache.find("AnalysisTransforms") < 0:
                rte = package.split("-")[0].upper() + "-" + cache.split("/")[1]
            elif cache.find("AnalysisTransforms") != -1:
                rte = package.upper()
                res = re.match("AnalysisTransforms-(.+)_(.+)", cache)
                if res is not None:
                    if res.group(1).find("AtlasProduction") != -1:
                        rte = "ATLAS-" + res.group(2)
                    else:
                        rte = "ATLAS-" + res.group(1).upper() + "-" + res.group(2)
            else:
                rte = cache.replace("Atlas", "Atlas-").replace("/", "-").upper()
            rte = str(rte)
            rte = rte.replace("ATLAS-", "")
            rte += "-" + self.jobdesc["cmtConfig"].upper()

            if cache.find("AnalysisTransforms") < 0:
                rte = rte.replace("PHYSICS-", "ATLASPHYSICS-")
                rte = rte.replace("PROD2-", "ATLASPROD2-")
                rte = rte.replace("PROD1-", "ATLASPROD1-")
                rte = rte.replace("DERIVATION-", "ATLASDERIVATION-")
                rte = rte.replace("P1HLT-", "ATLASP1HLT-")
                rte = rte.replace("TESTHLT-", "ATLASTESTHLT-")
                rte = rte.replace("CAFHLT-", "ATLASCAFHLT-")
                rte = rte.replace("21.0.13.1", "ATLASPRODUCTION-21.0.13.1")
                rte = rte.replace("21.0.20.1", "ATLASPRODUCTION-21.0.20.1")
            if cache.find("AnalysisTransforms") != -1:
                res = re.match("(21\..+)", rte)
                if res is not None:
                    rte = rte.replace("21", "OFFLINE-21")

            if rte.find("NULL") != -1:
                rte = "PYTHON-CVMFS-X86_64-SLC6-GCC47-OPT"

            atlasrtes.append(rte)

        self.xrsl["rtes"] = ""
        for rte in atlasrtes[-1:]:
            self.xrsl["rtes"] += "(runtimeenvironment = APPS/HEP/ATLAS-" + rte + ")"

        if self.siteinfo["type"] == "analysis":
            self.xrsl["rtes"] += "(runtimeenvironment = ENV/PROXY)"

        self.artes = ",".join(atlasrtes)

    def setExecutable(self):
        self.xrsl["executable"] = "(executable = ARCpilot)"

    def setArguments(self):
        if self.artes is None:
            self.setRTE()

        # Set options for NG/true pilot
        if self.truepilot:
            pargs = '"pilot3/pilot.py" "-h" "%s" "-s" "%s" "-f" "false" "-p" "25443" "-d" "{HOME}" "-w" "https://pandaserver.cern.ch"' % (
                self.schedconfig,
                self.sitename,
            )
        else:
            pargs = '"pilot3/pilot.py" "-h" "%s" "-s" "%s" "-F" "Nordugrid-ATLAS" "-d" "{HOME}" "-j" "false" "-f" "false" "-z" "true" "-b" "2" "-t" "false"' % (
                self.sitename,
                self.sitename,
            )

        pandajobarg = self.pandajob
        if self.longjob:
            pandajobarg = "FILE"
        self.xrsl["arguments"] = '(arguments = "' + self.artes + '" "' + pandajobarg + '" ' + pargs + ")"

    def setInputsES(self, inf):
        for f, s, i in zip(self.jobdesc["inFiles"].split(","), self.jobdesc["scopeIn"].split(","), self.jobdesc["prodDBlockToken"].split(",")):
            if i == "None":
                # Rucio file
                lfn = "/".join(["rucio://rucio-lb-prod.cern.ch;rucioaccount=pilot;transferprotocol=gsiftp;cache=invariant/replicas", s, f])
            elif int(i) in self.osmap:
                lfn = "/".join([self.osmap[int(i)], f])
            else:
                # TODO this exception is ignored by panda2arc
                raise Exception("No OS defined in AGIS for bucket id %s" % i)
            inf[f] = lfn

    def setInputs(self):
        x = ""
        if self.truepilot:
            x += '(ARCpilot "http://aipanda404.cern.ch;cache=check/data/releases/ARCpilot-true")'
        elif self.eventranges:
            x += '(ARCpilot "http://aipanda404.cern.ch;cache=check/data/releases/ARCpilot-es")'
        else:
            x += '(ARCpilot "http://aipanda404.cern.ch;cache=check/data/releases/ARCpilot")'

        if self.jobdesc["prodSourceLabel"] == "rc_test":
            x += '(pilotcode.tar.gz "http://pandaserver.cern.ch:25080;cache=check/cache/pilot/pilotcode-rc.tar.gz")'
        else:
            x += '(pilotcode.tar.gz "http://pandaserver.cern.ch:25080;cache=check/cache/pilot/pilotcode-PICARD.tar.gz")'

        if self.eventranges:
            x += '(ARCpilot-test.tar.gz "http://aipanda404.cern.ch;cache=check/data/releases/ARCpilot-es.tar.gz")'

        if self.longjob:
            try:
                os.makedirs(self.inputjobdir)
            except BaseException:
                pass
            tmpfile = self.inputjobdir + "/pandaJobData.out"
            with open(tmpfile, "w") as f:
                f.write(self.pandajob)
            x += '(pandaJobData.out "%s/pandaJobData.out")' % self.inputjobdir

        if not self.truepilot:
            x += '(queuedata.pilot.json "http://pandaserver.cern.ch:25085;cache=check/cache/schedconfig/%s.all.json")' % self.schedconfig
            if self.sitename.find("BEIJING") != -1:
                x += '(agis_ddmendpoints.json "/cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_ddmendpoints.json")'

        if "inFiles" in self.jobdesc and not self.truepilot:
            inf = {}
            if self.jobdesc.get("eventServiceMerge") == "True":
                self.setInputsES(inf)

            for filename, scope in zip(self.jobdesc["inFiles"].split(","), self.jobdesc["scopeIn"].split(",")):
                # Skip files which use direct I/O: site has it enabled, token is
                # not 'local', file is root file and --useLocalIO is not used

                # don't use direct I/O - pending new mover switch
                # if token != 'local' and self.siteinfo.get('direct_access_lan', False) and \
                #  not ('.tar.gz' in filename or '.lib.tgz' in filename or '.raw.' in filename) and \
                #  '--useLocalIO' not in self.jobdesc['jobPars'][0]:
                #    continue
                # Hard-coded pilot rucio account - should change based on proxy
                # Rucio does not expose mtime, set cache=invariant so not to download too much
                urloptions = ";rucioaccount=pilot;transferprotocol=gsiftp;cache=invariant"
                ruciourl = "rucio://rucio-lb-prod.cern.ch%s/replicas" % urloptions
                lfn = "/".join([ruciourl, scope, filename])

                inf[filename] = lfn

            # some files are double:
            for k, v in inf.items():
                x += '(%s "%s")' % (k, v)

            if self.jobdesc.get("eventService") and self.eventranges:
                # Create tmp json file to upload with job
                tmpjsonfile = os.path.join(self.tmpdir, "eventranges-%d.json" % self.pandaid)
                jsondata = json.loads(self.eventranges)
                with open(tmpjsonfile, "w") as f:
                    json.dump(jsondata, f)
                x += '("eventranges.json" "%s")' % tmpjsonfile

        self.xrsl["inputfiles"] = "(inputfiles =  %s )" % x

    def setLog(self):
        logfile = self.jobdesc.get("logFile", "LOGFILE")
        self.xrsl["log"] = '(stdout = "%s")(join = yes)' % logfile.replace(".tgz", "")

    def setGMLog(self):
        self.xrsl["gmlog"] = '(gmlog = "gmlog")'
        self.xrsl["rerun"] = '(rerun = "2")'

    def setOutputs(self):
        # dynamic outputs

        output = '("jobSmallFiles.tgz" "")'
        output += '("@output.list" "")'
        # needed for SCEAPI
        # generated output file list"
        output += '("output.list" "")'
        self.xrsl["outputs"] = "(outputfiles = %s )" % output

        if self.truepilot:
            self.xrsl["outputs"] = ""

    def setPriority(self):
        if "currentPriority" in self.jobdesc:
            prio = 50
            try:
                prio = int(self.jobdesc["currentPriority"])
                if prio < 1:
                    prio = 1
                if prio > 0 and prio < 1001:
                    prio = prio * 90 / 1000.0
                    prio = int(prio)
                if prio > 1000 and prio < 10001:
                    prio = 90 + (prio - 1000) / 900.0
                    prio = int(prio)
                if prio > 10000:
                    prio = 100
            except BaseException:
                pass
            self.xrsl["priority"] = "(priority = %d )" % prio
            if "wuppertalprod" in self.sitename:
                self.xrsl["priority"] = ""

    def setEnvironment(self):
        environment = {}
        environment["PANDA_JSID"] = self.schedulerid
        if self.logurl:
            environment["GTAG"] = self.logurl

        # Vars for APFMon
        if self.truepilot and self.monitorurl:
            environment["APFCID"] = self.pandaid
            environment["APFFID"] = schedid
            environment["APFMON"] = self.monitorurl
            environment["FACTORYQUEUE"] = self.sitename

        self.xrsl["environment"] = "(environment = %s)" % "".join(['("%s" "%s")' % (k, v) for (k, v) in environment.items()])

    def parse(self):
        self.setTime()
        self.setJobname()
        # self.setDisk()
        self.setMemory()
        self.setRTE()
        self.setExecutable()
        self.setArguments()
        self.setInputs()
        self.setLog()
        self.setGMLog()
        self.setOutputs()
        self.setPriority()
        self.setEnvironment()

    def getXrsl(self):
        x = "&"
        x += "\n".join([val for val in self.xrsl.values()])
        return x
