#!/bin/bash

QUEUENAME=
TOPDIR=
PATH_TO_PROXY=
HARVESTERID=
BATCH_SYSTEM=
PATH_TO_CERTIFICATES=
USERNAME=$(whoami)
GROUPNAME=$(IFS=" " ; set -- $(groups) ; echo $1)

HARVESTER_REPO="git+git://github.com/PanDAWMS/panda-harvester.git@OLCF_validation"

usage() { echo -e "Usage: $0 -d <directory_to_install_to> -h <harvester_id> -q <queue_name> -b <batchsystem: e.g. torque|lsf|slurm|condor> -p <path_to_proxy> -c <path_to_certificates>\n\n" 1>&2; exit 1; }

set -e

while getopts ":q:d:p:h:b:c:" o; do
    case "${o}" in
        q)
            QUEUENAME=${OPTARG}
            ;;
        d)
            TOPDIR=${OPTARG}
            ;;
        p)
            PATH_TO_PROXY=${OPTARG}
            ;;
        h)
            HARVESTERID=${OPTARG}
            ;;
        b)
            BATCH_SYSTEM=${OPTARG}
            ;;
        c)
            PATH_TO_CERTIFICATES=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${QUEUENAME}" ] || [ -z "${TOPDIR}" ] || [ -z "${HARVESTERID}" ] || [ -z "${BATCH_SYSTEM}" ]; then
    usage
    exit 1
fi

cat << EOF
QUEUENAME=${QUEUENAME}
TOPDIR=${TOPDIR}
PATH_TO_PROXY=${PATH_TO_PROXY}
HARVESTERID=${HARVESTERID}
BATCH_SYSTEM=${BATCH_SYSTEM}
PATH_TO_CERTIFICATES=${PATH_TO_CERTIFICATES}
EOF

# checking python version
if [ -z "$(python --version 2>&1 | grep 2.7)" ]; then
    echo "Python 2.7 is recommended"
fi

# check for virtualenv
python -c "import virtualenv" 2>/dev/null || (echo "No virtualenv installed, exiting..." ; exit 1 )


# check top dir exists, then - writeable
[[ -e "${TOPDIR}" ]] && [[ ! -w "${TOPDIR}" ]] && (echo "Installation directory exists but not writeable, exiting..." ; exit 1)

# check top dir is writeable
if [ ! -e "${TOPDIR}" ]; then
    mkdir -p ${TOPDIR} || (echo "Error creating installation directory, exiting..." ; exit 1 )
fi

cd ${TOPDIR}

#virtualenv harvester-venv # or: python -m virtualenv harvester-vens  # or: python -m venv harvester-venv (for python 3)
python -m virtualenv "harvester-${HARVESTERID}"
cd "harvester-${HARVESTERID}"
. bin/activate

# install additional python packages if missing
pip install pip --upgrade

# install panda components
# do not run this one: pip install git+git://github.com/PanDAWMS/panda-harvester.git

pip install ${HARVESTER_REPO}  --upgrade
pip install saga-python backports.ssl-match-hostname

# copy sample setup and config files
mv etc/sysconfig/panda_harvester.rpmnew.template  etc/sysconfig/panda_harvester

# mv etc/panda/panda_common.cfg.rpmnew.template etc/panda/panda_common.cfg
# mv etc/panda/panda_harvester.cfg.rpmnew.template etc/panda/panda_harvester.cfg

mkdir -p var/log/panda var/harvester

export PANDA_HOME=$VIRTUAL_ENV
export PYTHONPATH=$VIRTUAL_ENV/lib/python2.7/site-packages/pandacommon:$VIRTUAL_ENV/lib/python2.7/site-packages

# add these values to “activate” script so you do not have to re-enter them every time on virtual env boot
cat << EOF >> ${VIRTUAL_ENV}/bin/activate
export PANDA_HOME=\$VIRTUAL_ENV
export PYTHONPATH=\$VIRTUAL_ENV/lib/python2.7/site-packages/pandacommon:$VIRTUAL_ENV/lib/python2.7/site-packages
EOF

# copy panda_harvester.cfg , panda_common.cfg , EC2_queueconfig_LQCD.json to $VIRTUAL_ENV/etc/panda/
wget https://www.dropbox.com/s/dcr3rysrtmo1cjs/panda_common.cfg?dl=0 -O etc/panda/panda_common.cfg
wget https://www.dropbox.com/s/2fkakqwgbfhgdxl/panda_harvester.cfg?dl=0 -O etc/panda/panda_harvester.cfg
wget https://www.dropbox.com/s/ijlhb1mpmbdvloo/EC2_queueconfig.json?dl=0 -O etc/panda/EC2_queueconfig.json

# putting values into configuration
sed -i -e "s#\${BASE_DIR}#$VIRTUAL_ENV#" etc/panda/panda_common.cfg
sed -i -e "s#\${BASE_DIR}#$VIRTUAL_ENV#" etc/panda/panda_harvester.cfg

# fix “uname” and “gname” items with actual values of username/groupname
sed -i -e "s#\${USERNAME}#$USERNAME#" etc/panda/panda_harvester.cfg
sed -i -e "s#\${GROUPNAME}#$GROUPNAME#" etc/panda/panda_harvester.cfg
sed -i -e "s#\${HARVESTERID}#$HARVESTERID#" etc/panda/panda_harvester.cfg
sed -i -e "s#\${QUEUENAME}#$QUEUENAME#" etc/panda/panda_harvester.cfg
sed -i -e "s#\${PATH_TO_PROXY}#$PATH_TO_PROXY#" etc/panda/panda_harvester.cfg
sed -i -e "s#\${PATH_TO_CERTIFICATES}#$PATH_TO_CERTIFICATES#" etc/panda/panda_harvester.cfg

# putting values into queue file
sed -i -e "s#\${QUEUENAME}#$QUEUENAME#" etc/panda/EC2_queueconfig.json
sed -i -e "s#\${BATCH_SYSTEM}#$BATCH_SYSTEM#" etc/panda/EC2_queueconfig.json
sed -i -e "s#\${TOP_DIR}#$TOPDIR#" etc/panda/EC2_queueconfig.json

# update paths to your proxy in items “key_file” , “cert_file” and point “ca_cert” to a right directory with CA certificates, usually it is “/etc/grid-security/certificates"

mkdir ${VIRTUAL_ENV}/../harvester-messenger \
    ${VIRTUAL_ENV}/../harvester-preparator \
    ${VIRTUAL_ENV}/../harvester-worker-maker

# and update the corresponding paths in etc/panda/EC2_queueconfig_LQCD.json

# copy corresponding attached files to files to locations:
mv ${VIRTUAL_ENV}/lib/python2.7/site-packages/pandaharvester/harvestermonitor/saga_monitor.py ${VIRTUAL_ENV}/lib/python2.7/site-packages/pandaharvester/harvestermonitor/saga_monitor.py.orig
wget https://www.dropbox.com/s/bz3tie6xbug8lpz/saga_monitor.py?dl=0 -O ${VIRTUAL_ENV}/lib/python2.7/site-packages/pandaharvester/harvestermonitor/saga_monitor.py
wget https://www.dropbox.com/s/o9iygiidlj2d7a2/saga_yaml_submitter.py?dl=0 -O ${VIRTUAL_ENV}/lib/python2.7/site-packages/pandaharvester/harvestersubmitter/saga_yaml_submitter.py


# copy start_harvester.sh , stop_harvester.sh , clean_logs.sh to ${VIRTUAL_ENV}
cat << EOF > ./start_harvester.sh
#!/bin/bash
python lib/python*/site-packages/pandaharvester/harvesterbody/master.py --pid $PWD/tmp.pid
EOF


cat << EOF > ./stop_harvester.sh
#!/bin/bash
kill -USR2 \`cat $PWD/tmp.pid\`
EOF


cat << EOF > ./clean_logs.sh
#!/bin/bash

if [ -z $VIRTUAL_ENV ]; then
        exit 0
fi

rm -f $VIRTUAL_ENV/var/log/panda/*
echo Logs cleaned
EOF

chmod +x ./start_harvester.sh ./stop_harvester.sh ./clean_logs.sh
