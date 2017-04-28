#
#
# Setup for Harvester
#
#
import panda_pkg_info
import sys
from setuptools import setup, find_packages
sys.path.insert(0, '.')

# get release version
release_version = panda_pkg_info.release_version

setup(
    name="pandaharvester",
    version=release_version,
    description='Harvester Package',
    long_description='''This package contains Harvester components''',
    license='GPL',
    author='Panda Team',
    author_email='atlas-adc-panda@cern.ch',
    url='https://twiki.cern.ch/twiki/bin/view/Atlas/PanDA',
    packages=find_packages(),
    install_requires=['requests',
                      'python-daemon',
                      ],
    data_files=[
        # config and cron files
        ('etc/panda', ['templates/panda_harvester.cfg.rpmnew.template',
                       'templates/logrotate.d/panda_harvester',
                       ]
         ),
        # sysconfig
        ('etc/sysconfig', ['templates/sysconfig/panda_harvester.rpmnew.template',
                           ]
         ),
        # init script
        ('etc/rc.d/init.d', ['templates/init.d/panda_harvester',
                             ]
         ),
        ],
    scripts=['templates/panda_jedi-renice']
    )
