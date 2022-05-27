FROM docker.io/centos:7

RUN yum update -y
RUN yum install -y epel-release
RUN yum install -y python3 python3-devel gcc less git mysql-devel curl mariadb voms-clients-cpp wget

RUN curl -fsSL https://get.htcondor.org | /bin/bash -s -- --no-dry-run

RUN python3 -m venv /opt/harvester
RUN /opt/harvester/bin/pip install -U pip
RUN /opt/harvester/bin/pip install -U setuptools
RUN /opt/harvester/bin/pip install -U mysqlclient uWSGI
RUN /opt/harvester/bin/pip install git+https://github.com/HSF/harvester.git

RUN mv /opt/harvester/etc/sysconfig/panda_harvester.rpmnew.template  /opt/harvester/etc/sysconfig/panda_harvester
RUN mv /opt/harvester/etc/panda/panda_common.cfg.rpmnew /opt/harvester/etc/panda/panda_common.cfg
RUN mv /opt/harvester/etc/panda/panda_harvester.cfg.rpmnew.template /opt/harvester/etc/panda/panda_harvester.cfg
RUN mv /opt/harvester/etc/panda/panda_harvester-uwsgi.ini.rpmnew.template /opt/harvester/etc/panda/panda_harvester-uwsgi.ini
RUN mv /opt/harvester/etc/rc.d/init.d/panda_harvester-uwsgi.rpmnew.template /opt/harvester/etc/rc.d/init.d/panda_harvester-uwsgi

RUN ln -fs /opt/harvester/etc/queue_config/panda_queueconfig.json /opt/harvester/etc/panda/panda_queueconfig.json

RUN adduser atlpan
RUN groupadd zp
RUN usermod -a -G zp atlpan

RUN mkdir -p /var/log/panda
RUN chown -R atlpan:zp /var/log/panda

RUN mkdir -p /data/harvester
RUN chown -R atlpan:zp /data/harvester

# to run with non-root PID
RUN mkdir -p /etc/grid-security/certificates
RUN chmod -R 777 /etc/grid-security/certificates

# make a wrapper script to launch services and periodic jobs in non-root container
RUN echo $'#!/bin/bash \n\
set -m \n\
while true; do /opt/harvester/bin/panda_common-install_igtf_ca > /var/log/panda/install_igtf_ca.log 2>&1; sleep 36000; done & \n\
/opt/harvester/etc/rc.d/init.d/panda_harvester-uwsgi start \n ' > /opt/harvester/etc/rc.d/init.d/run-harvester-services

RUN chmod +x /opt/harvester/etc/rc.d/init.d/run-harvester-services

CMD exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"
