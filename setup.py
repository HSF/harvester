#
#
# Setup for Harvester
#
#
import sys

from setuptools import find_packages, setup

from pandaharvester import panda_pkg_info

sys.path.insert(0, ".")

# get release version
release_version = panda_pkg_info.release_version

setup(
    name="pandaharvester",
    version=release_version,
    description="Harvester Package",
    long_description="""This package contains Harvester components""",
    license="GPL",
    author="Panda Team",
    author_email="atlas-adc-panda@cern.ch",
    url="https://github.com/PanDAWMS/panda-harvester/wiki",
    python_requires=">=2.7",
    packages=find_packages(),
    install_requires=[
        "requests",
        "python-daemon",
        "pycryptodomex",
        "panda-common",
        "pyjwt",
        "rpyc",
        "paramiko",
        "pexpect",
        "psutil >= 5.4.8",
        "panda-pilot >= 2.7.2.1",
    ],
    # optional pip dependencies
    extras_require={
        "kubernetes": ["kubernetes", "pyyaml"],
        "mysql": ["mysqlclient"],
        "atlasgrid": ["uWSGI >= 2.0.20", "htcondor>=10.3.0,<=24.12.4", "mysqlclient >= 2.1.1"],
    },
    data_files=[
        # config and cron files
        (
            "etc/panda",
            [
                "templates/panda/panda_harvester.cfg.rpmnew.template",
                "templates/panda/panda_harvester-httpd.conf.rpmnew.template",
                "templates/panda/panda_supervisord.cfg.rpmnew.template",
                "templates/panda/panda_harvester-uwsgi.ini.rpmnew.template",
            ],
        ),
        # sysconfig
        (
            "etc/sysconfig",
            [
                "templates/sysconfig/panda_harvester.rpmnew.template",
                "templates/sysconfig/panda_harvester_env.systemd.rpmnew.template",
            ],
        ),
        # init script
        (
            "etc/rc.d/init.d",
            [
                "templates/init.d/panda_harvester.rpmnew.template",
                "templates/init.d/panda_harvester-apachectl.rpmnew.template",
                "templates/init.d/panda_harvester-uwsgi.rpmnew.template",
            ],
        ),
        # systemd
        (
            "etc/systemd",
            [
                "templates/systemd/panda_harvester-uwsgi.service.template",
            ],
        ),
        # logrotate
        (
            "etc/logrotate.d",
            [
                "templates/logrotate.d/panda_harvester",
                "templates/logrotate.d/panda_harvester_service",
            ],
        ),
        # admin tool
        (
            "local/bin",
            [
                "templates/bin/harvester-admin.rpmnew.template",
            ],
        ),
    ],
    scripts=[
        "templates/bin/panda_jedi-renice",
        "templates/bin/panda_harvester-sqlite3backup",
    ],
)
