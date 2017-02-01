# Folder structure
* **Body**: Harvester agent modules (subclasses of thread) using code in their specific subfolder. Master is the main module spawning rest of agents.
* **Config**: Module to read configuration file
* **Core**: Central components (e.g. class for DB access, classes to represent files/jobs/events, class to talk with panda server) needed in other modules
* **Cred manager**: Module to renew proxy
* **Messenger**: Communication classes to interact between workers and harvester
* **Monitor**: Classes to monitor jobs, e.g. through SLURM, SAGA...
* **Mover**: Base classes or tools for Preparator and Stager
* **Preparator**: Classes to prepare (stage **in**) data for jobs
* **Stager**: Classes to stage **out** data
* **Submitter**: Classes to submit jobs to the batch system
* **Test**: Test scripts
* **Worker Maker**: Makes workers

# Table structure

SQLite DB structures are generated in the harvestercore/db_proxy.py. Current tables are:
```python
jobTableName = 'job_table'
workTableName = 'work_table'
fileTableName = 'file_table'
cacheTableName = 'cache_table'
eventTableName = 'event_table'
seqNumberTableName = 'seq_table'
pandaQueueTableName = 'pq_table'
jobWorkerTableName = 'jw_table'
```

Individual table structure is defined in each *_spec.py file. The initial definitions below, but will evolve over time, so please consult the relevant spec file for the up to date structure.
### job_table
```python
attributesWithTypes = ('PandaID:integer primary key',
                       'taskID:integer',
                       'attemptNr:integer',
                       'status:text',
                       'subStatus:text',
                       'currentPriority:integer',
                       'computingSite:text',
                       'creationTime:timestamp',
                       'modificationTime:timestamp',
                       'stateChangeTime:timestamp',
                       'startTime:timestamp',
                       'endTime:timestamp',
                       'nCore:integer',
                       'jobParams:blob',
                       'jobAttributes:blob',
                       'hasOutFile:integer',
                       'metaData:blob',
                       'outputFilesToReport:blob',
                       'lockedBy:text',
                       'propagatorLock:text',
                       'propagatorTime:timestamp',
                       'preparatorTime:timestamp',
                       'submitterTime:timestamp',
                       'stagerLock:text',
                       'stagerTime:timestamp',
                       'zipPerMB:integer',
                       )
```

### work_table
```python
attributesWithTypes = ('workerID:integer',
                       'batchID:text',
                       'mapType:text',
                       'queueName:text',
                       'status:text',
                       'hasJob:integer',
                       'workParams:blob',
                       'workAttributes:blob',
                       'eventsRequestParams:blob',
                       'eventsRequest:integer',
                       'computingSite:text',
                       'creationTime:timestamp',
                       'submitTime:timestamp',
                       'startTime:timestamp',
                       'endTime:timestamp',
                       'nCore:integer',
                       'walltime:timestamp',
                       'accessPoint:text',
                       'modificationTime:timestamp',
                       'stateChangeTime:timestamp',
                       'eventFeedTime:timestamp',
                       'lockedBy:text',
                       'postProcessed:integer'
                       )
```
### jw_table (job worker)
```python
attributesWithTypes = ('PandaID:integer',
                       'workerID:integer',
                       'relationType:text',
                       )
```        
### file_table
```python
attributesWithTypes = ('fileID:integer primary key',
                       'PandaID:integer',
                       'taskID:integer',
                       'lfn:text',
                       'status:text',
                       'fsize:integer',
                       'chksum:text',
                       'path:text',
                       'fileType:text',
                       'eventRangeID:text',
                       'modificationTime:timestamp',
                       'fileAttributes:blob',
                       'isZip:integer',
                       'zipFileID:integer',
                       'objstoreID:integer'
                       )
```
### cache_table
```python
attributesWithTypes = ('mainKey:text',
                       'subKey:text',
                       'data:blob',
                       'lastUpdate:timestamp'
                       )
```
### event_table
```python
attributesWithTypes = ('eventRangeID:text',
                       'PandaID:integer',
                       'eventStatus:text',
                       'coreCount:integer',
                       'cpuConsumptionTime:integer',
                       'subStatus:text',
                       'fileID:integer'
                       )
```
### seq_table
```python
attributesWithTypes = ('numberName:text',
                       'curVal:integer',
                       )
```
### pq_table (panda queue)
```python
attributesWithTypes = ('queueName:text',
                       'nQueueLimitJob:integer',
                       'nQueueLimitWorker:integer',
                       'maxWorkers:integer',
                       'jobFetchTime:timestamp',
                       'submitTime:timestamp',
                       )
```


