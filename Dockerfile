FROM docker.io/centos:7

RUN yum update -y
RUN yum install -y epel-release
RUN yum install -y python3 python3-devel gcc less git mysql-devel curl mariadb voms-clients-cpp wget httpd logrotate

RUN mkdir -p /data/condor; cd /data/condor; \
    wget https://research.cs.wisc.edu/htcondor/tarball/9.0/9.0.17/release/condor-9.0.17-x86_64_CentOS7-stripped.tar.gz -O condor.tar.gz.9; \
    curl -fsSL https://get.htcondor.org | /bin/bash -s -- --download --channel stable; \
    mv condor.tar.gz condor.tar.gz.stable; \
    curl -fsSL https://get.htcondor.org | /bin/bash -s -- --download; \
    ln -fs condor.tar.gz condor.tar.gz.latest
    
#install gcloud
RUN echo $'[google-cloud-cli] \n\
name=Google Cloud CLI \n\
baseurl=https://packages.cloud.google.com/yum/repos/cloud-sdk-el8-x86_64 \n\
enabled=1 \n\
gpgcheck=1 \n\
repo_gpgcheck=0 \n\
gpgkey=https://packages.cloud.google.com/yum/doc/yum-key.gpg \n\
       https://packages.cloud.google.com/yum/doc/rpm-package-key.gpg \n ' > /etc/yum.repos.d/google-cloud-sdk.repo

RUN yum install -y google-cloud-sdk-gke-gcloud-auth-plugin kubectl

# install voms
RUN yum install -y https://repo.opensciencegrid.org/osg/3.6/el7/release/x86_64/osg-ca-certs-1.109-1.osg36.el7.noarch.rpm
RUN yum install -y https://repo.opensciencegrid.org/osg/3.6/el7/release/x86_64/vo-client-130-1.osg36.el7.noarch.rpm

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
RUN chmod -R 777 /etc/httpd
RUN chmod -R 777 /var/log/httpd
RUN chmod -R 777 /var/lib/logrotate
RUN mkdir -p /opt/harvester/etc/queue_config && chmod 777 /opt/harvester/etc/queue_config
COPY docker/httpd.conf /etc/httpd/conf/

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
/sbin/httpd \n\
/opt/harvester/etc/rc.d/init.d/panda_harvester-uwsgi start \n ' > /opt/harvester/etc/rc.d/init.d/run-harvester-services

RUN chmod +x /opt/harvester/etc/rc.d/init.d/run-harvester-services

# add condor setup ins sysconfig
RUN echo source /data/condor/condor/condor.sh >> /opt/harvester/etc/sysconfig/panda_harvester

CMD exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"

EXPOSE 8080
