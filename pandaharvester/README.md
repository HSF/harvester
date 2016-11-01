# Folder structure
* **Body**: Harvester agent modules (subclasses of thread) using code in their specific subfolder. Master is the main module spawning rest of agents.
* **Config**: Module to read configuration file
* **Core**: Central components (e.g. class for DB access, classes to represent files/jobs/events, class to talk with panda server) needed in other modules
* **Cred manager**: Module to renew proxy
* **Messenger**: Communication classes to interact between workers and harvester
* **Monitor**: Classes to monitor jobs, e.g. through SLURM, SAGA, etc.
* **Preparator**: Classes to prepare data for jobs?
* **Stager**: Classes to stage out data
* **Submitter**: Classes to submit jobs to the batch system
* **Test**: Test scripts
* **Workmaker**: Makes workers
