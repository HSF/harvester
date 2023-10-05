import errno
import json
import hashlib
import psutil

from pandaharvester.commit_timestamp import timestamp as commitTimestamp
from pandaharvester.panda_pkg_info import release_version as releaseVersion


class harvesterPackageInfo(object):
    """ """

    _attributes = ("commit_info", "version", "info_digest")

    def __init__(self, local_info_file):
        self.local_info_file = local_info_file
        self.commit_info = commitTimestamp
        self.version = releaseVersion

    @staticmethod
    def _get_hash(data):
        h = hashlib.md5()
        h.update(str(data).encode("utf-8"))
        return h.hexdigest()

    @property
    def info_digest(self):
        return self._get_hash(self.commit_info)

    @property
    def _local_info_dict(self):
        info_dict = {}
        try:
            with open(self.local_info_file, "r") as f:
                info_dict = json.load(f)
        except IOError as e:
            if e.errno == errno.ENOENT:
                pass
            else:
                raise
        return info_dict

    def renew_local_info(self):
        info_dict = {}
        for attr in self._attributes:
            info_dict[attr] = getattr(self, attr)
        with open(self.local_info_file, "w") as f:
            json.dump(info_dict, f)

    @property
    def package_changed(self):
        return self.info_digest != self._local_info_dict.get("info_digest")
