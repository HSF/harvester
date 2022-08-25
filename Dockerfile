FROM docker.io/centos:7

RUN yum update -y
RUN yum install -y epel-release
RUN yum install -y python3 python3-devel gcc less git mysql-devel curl mariadb voms-clients-cpp wget

RUN mkdir -p /data/condor; cd /data/condor; \
    curl -fsSL https://get.htcondor.org | /bin/bash -s -- --download --channel stable; \
    mv condor.tar.gz condor.tar.gz.stable; \
    curl -fsSL https://get.htcondor.org | /bin/bash -s -- --download

#install gcloud
tee -a /etc/yum.repos.d/google-cloud-sdk.repo << EOM
[google-cloud-cli]
name=Google Cloud CLI
baseurl=https://packages.cloud.google.com/yum/repos/cloud-sdk-el8-x86_64
enabled=1
gpgcheck=1
repo_gpgcheck=0
gpgkey=https://packages.cloud.google.com/yum/doc/yum-key.gpg
       https://packages.cloud.google.com/yum/doc/rpm-package-key.gpg
EOM

RUN yum install -y google-cloud-sdk-gke-gcloud-auth-plugin kubectl
RUN yum install -y google-cloud-cli

RUN python3 -m venv /opt/harvester
RUN /opt/harvester/bin/pip install -U pip
RUN /opt/harvester/bin/pip install -U setuptools
RUN /opt/harvester/bin/pip install -U mysqlclient uWSGI pyyaml
RUN /opt/harvester/bin/pip install -U kubernetes
RUN mkdir /tmp/src
WORKDIR /tmp/src
COPY . .
RUN /opt/harvester/bin/python setup.py sdist; /opt/harvester/bin/pip install `ls dist/*.tar.gz`
WORKDIR /
RUN rm -rf /tmp/src

RUN mv /opt/harvester/etc/sysconfig/panda_harvester.rpmnew.template  /opt/harvester/etc/sysconfig/panda_harvester
RUN mv /opt/harvester/etc/panda/panda_common.cfg.rpmnew /opt/harvester/etc/panda/panda_common.cfg
RUN mv /opt/harvester/etc/panda/panda_harvester.cfg.rpmnew.template /opt/harvester/etc/panda/panda_harvester.cfg
RUN mv /opt/harvester/etc/panda/panda_harvester-uwsgi.ini.rpmnew.template /opt/harvester/etc/panda/panda_harvester-uwsgi.ini
RUN mv /opt/harvester/etc/rc.d/init.d/panda_harvester-uwsgi.rpmnew.template /opt/harvester/etc/rc.d/init.d/panda_harvester-uwsgi

RUN ln -fs /opt/harvester/etc/queue_config/panda_queueconfig.json /opt/harvester/etc/panda/panda_queueconfig.json

RUN adduser atlpan
RUN groupadd zp
RUN usermod -a -G zp atlpan
RUN passwd -d atlpan

RUN mkdir -p /var/log/panda
RUN chown -R atlpan:zp /var/log/panda

RUN mkdir -p /data/harvester
RUN chown -R atlpan:zp /data/harvester

# to run with non-root PID
RUN mkdir -p /etc/grid-security/certificates
RUN chmod -R 777 /etc/grid-security/certificates
RUN chmod -R 777 /data/harvester
RUN chmod -R 777 /data/condor

# make lock dir
ENV PANDA_LOCK_DIR /var/run/panda
RUN mkdir -p ${PANDA_LOCK_DIR} && chmod 777 ${PANDA_LOCK_DIR}

# make a wrapper script to launch services and periodic jobs in non-root container
RUN echo $'#!/bin/bash \n\
set -m \n\
/data/harvester/init-harvester \n\
/data/harvester/run-harvester-crons & \n\
source /data/harvester/setup-harvester \n\
cd /data/condor \n\
tar -x -f condor.tar.gz${CONDOR_CHANNEL} \n\
mv condor-*stripped condor \n\
cd condor \n\
./bin/make-personal-from-tarball \n\
. condor.sh \n\
cp /data/harvester/condor_config.local /data/condor/condor/local/config.d/ \n\
condor_master \n\
/opt/harvester/etc/rc.d/init.d/panda_harvester-uwsgi start \n ' > /opt/harvester/etc/rc.d/init.d/run-harvester-services

RUN chmod +x /opt/harvester/etc/rc.d/init.d/run-harvester-services

CMD exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"
