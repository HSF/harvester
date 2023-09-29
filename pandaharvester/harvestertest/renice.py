import subprocess
from pandaharvester.harvesterconfig import harvester_config

proc = subprocess.Popen(
    "ps -l x -U {0}".format(harvester_config.master.uname),
    shell=True,
    stdout=subprocess.PIPE,
)
stdoutList = proc.communicate()[0].split("\n")
for line in stdoutList:
    try:
        items = line.split()
        if len(items) < 6:
            continue
        pid = items[3]
        nice = int(items[7])
        if "master.py" in line and nice > 0:
            reniceProc = subprocess.Popen(
                "renice 0 {0}".format(pid),
                shell=True,
                stdout=subprocess.PIPE,
            )
    except Exception:
        pass
