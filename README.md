# Harvester

### Introduction

Harvester is a resource-facing service between the PanDA server and collection of pilots. It is a lightweight stateless service running on a VObox or an edge node of HPC centers to provide a uniform view for various resources.

----------

### Installation
Harvester can be installed with or without root privilege.


#### With root privilege

#### Without root privilege
```sh
$ virtualenv harvester
$ cd harvester
$ . bin/activate

$ # install additional python packages if missing
$ pip install pip --upgrade
$ pip install python-daemon

$ # install panda components
$ pip install git+git://github.com/PanDAWMS/panda-common.git@setuptools
$ pip install git+git://github.com/PanDAWMS/panda-harvester.git

$ # copy sample setup and config files
$ mv etc/sysconfig/panda_harvester.rpmnew.template  etc/sysconfig/panda_harvester
$ mv etc/panda/panda_common.cfg.rpmnew.template etc/panda/panda_common.cfg
$ mv etc/panda/panda_harvester.cfg.rpmnew.template etc/panda/panda_harvester.cfg

```


#### Setup and system configuration files
Several parameters need to be adjusted in the setup file (etc/sysconfig/panda_harvester)
and two system config files (etc/panda/panda_common.cfg and etc/panda/panda_harvester.cfg).

The following parameters need to be modified in the setup file.
| Name | Description  
- | - 
PANDA_HOME | Config files must be under $PANDA_HOME/etc
PYTHONPATH | Must contain the pandacommon package

Example	   :
```
export PANDA_HOME=$VIRTUAL_ENV
export PYTHONPATH=$VIRTUAL_ENV/lib/python2.6/site-packages/pandacommon
```

The **logdir** needs to be set in etc/panda/panda_common.cfg. It is recommended to use a non-NFS directory to avoid buffering.
| Name | Description 
- | - | - 
- logdir | A directory for log files

Example :
```
logdir = /var/log/panda
```

The following list shows parameters need to be adjusted in etc/panda/panda_harvester.cfg.

| Name | Description  
- | - 
master.uname | User ID of the daemon process
master.gname | Group ID of the daemon process
db.database_filename | Filename of the local database
db.verbose | Set True to dump all SQL queries in the log file
pandacon.ca_cert | CERN CA certificate file
pandacon.cert_file | A grid proxy file to access the panda server
pandacon.key_file | The same as pandacon.cert_file
qconf.configFile | The queue configuration file. See the next section for details
qconf.queueList | The list of PandaQueues for which the harvester instance works
credmanager.moduleName | The module name of the credential manager
credmanager.className | The class name of the credential manager
credmanager.certFile | A grid proxy without VOMS extension. NoVomsCredManager generates VOMS proxy using the file


##### Queue configuration file

Plug-ins for each PandaQueue is configured in the queue configuration file.
The filename is defined in **qconf.configFile** and has to be put in the $PANDA_HOME/etc/panda
directory. This file might be integrated in the information system json in the future, but for
now it has to be manually created. Here is an
[example](https://github.com/PanDAWMS/panda-harvester/blob/master/examples/panda_queueconfig.json)
of the queue configuration file. The contents is a json dump of 

```json
{
"PandaQueueName1": {
		   "QueueAttributeName1": ValueQ_1,
		   "QueueAttributeName2": ValueQ_2,
		   ...
		   "QueueAttributeNameN": ValueQ_N,
		   "Agent1": {
		   	     "AgentAttribute1": ValueA_1,
			     "AgentAttribute2": ValueA_2,
			     ...
			     "AgentAttributeM": ValueA_M
			     },
		   "Agent2": {
		   	     ...
			     },
		   ...
		   "AgentX": {
		   	     ...
		   	     },
		   },
"PandaQueueName2": {
		   ...
		   },
...
"PandaQueueNameY": {
		   ...
		   },

}
```