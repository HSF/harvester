import os

from pandaharvester.harvesterbody import watcher
from pandaharvester.harvesterbody.watcher import Watcher

try:
    os.remove(watcher.lockFileName)
except BaseException:
    pass

watcher = Watcher(single_mode=True)
watcher.run()
