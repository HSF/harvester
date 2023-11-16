import subprocess

from pandaharvester.harvesterconfig import harvester_config

proc = subprocess.Popen(
    f"ps -l x -U {harvester_config.master.uname}",
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
                f"renice 0 {pid}",
                shell=True,
                stdout=subprocess.PIPE,
            )
    except Exception:
        pass
