Harvester installer

Harvester installer script created initial directory structures and initial configuration.

./install-harvester.sh \
                    -d <top_directory> \
                    -h <harvester_id> \
                    -q <queue_name> \
                    -b <batch_system> \
                    [ -p <path_to_your_proxy>] \
                    [ -c <path_to_CA_certificates, usually: /etc/grid-security/certificates> ]

For example:

./install-harvester.sh \
                    -d /home/user_home/harvester_topdir \
                    -h BNL_IC_HARVESTER \
                    -q ANALY-BNL_IC-LQCD \
                    -b torque \
                    -p /home/user_home/proxy \
                    -c /home/user_home/grid-security/certificates

This will create the following defaultdirectory structure:

|-- /home/user_home/harvester_topdir
    |-- harvester-messenger
    |-- harvester-preparator
    |-- harvester-worker-maker
    |-- harvester-BNL_IC_HARVESTER
        |-- bin
        |-- etc
        |-- include
        |-- lib
        |-- lib64 -> lib
        |-- share
        |-- start_harvester.sh
        |-- stop_harvester.sh
        |-- clean_logs.sh
        `-- var
            |-- harvester
            |   `-- test.db
            `-- log
                `-- panda

A virtual environment is created under harvester-BNL_IC_HARVESTER directory, which contains patched bin/activate script which sets all of the necessary environment variable is created by the installer,
Thus, in order to activate the environment you simply have to run:

source /home/user_home/harvester_topdir/harvester-BNL_IC_HARVESTER/bin/activate

The installer also creates tools for simple Harvester control (start_harvester, stop_harvester, clean_logs)

Some manual configuration may be needed if you need to tune parameters in queueconfig.json of etc/panda/panda_harvester.cfg

If you have some restrictions on installing software (not possible to install virtualenv, system-wide python does not have sqlite modules, other problems): please refer to PYTHON-INSTALLATION.md
