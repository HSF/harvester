from hatchling.builders.hooks.plugin.interface import BuildHookInterface

import os
import stat
import glob


class CustomBuildHook(BuildHookInterface):

    def initialize(self, version, build_data):
        # chmod +x
        for f in glob.glob("./templates/bin/*"):
            st = os.stat(f)
            os.chmod(f, st.st_mode | stat.S_IEXEC | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
