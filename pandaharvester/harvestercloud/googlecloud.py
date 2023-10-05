import googleapiclient.discovery
import os

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercloud import cernvm_aux
from pandaharvester.harvestermisc.frontend_utils import HarvesterToken

PROXY_PATH = harvester_config.pandacon.cert_file
USER_DATA_PATH = harvester_config.googlecloud.user_data_file
HARVESTER_FRONTEND = harvester_config.googlecloud.harvester_frontend

IMAGE = harvester_config.googlecloud.image
ZONE = harvester_config.googlecloud.zone
PROJECT = harvester_config.googlecloud.project
SERVICE_ACCOUNT_FILE = harvester_config.googlecloud.service_account_file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_FILE

compute = googleapiclient.discovery.build("compute", "v1")


class GoogleVM:
    def __init__(self, work_spec, queue_config):
        self.harvester_token = HarvesterToken()
        self.work_spec = work_spec
        self.queue_config = queue_config
        harvester_id_clean = harvester_config.master.harvester_id.replace("-", "").replace("_", "").lower()
        self.name = "{0}-gce-{1}".format(harvester_id_clean, work_spec.workerID)
        # self.name = self.name.replace('_', '-') # underscores in VM names are not allowed by GCE
        self.image = self.resolve_image_url()
        self.instance_type = self.resolve_instance_type()
        self.config = self.prepare_metadata()

    def resolve_image_url(self):
        """
        TODO: implement
        :param work_spec: worker specifications
        :return: URL pointing to the machine type to use
        """
        # Get the latest Debian Jessie image
        image_response = compute.images().getFromFamily(project=PROJECT, family="cernvm").execute()
        source_disk_image = image_response["selfLink"]

        return source_disk_image

    def resolve_instance_type(self):
        """
        Resolves the ideal instance type for the work specifications. An overview on VM types can be found here: https://cloud.google.com/compute/docs/machine-types

        TODO: for the moment we will just assume we need the standard type, but in the future this function can be expanded
        TODO: to consider also custom VMs, hi/lo mem, many-to-one mode, etc.

        :param work_spec: worker specifications
        :return: instance type name
        """

        # Calculate the number of VCPUs
        cores = 8  # default value. TODO: probably should except if we don't find a suitable number
        standard_cores = [1, 2, 4, 8, 16, 32, 64, 96]
        for standard_core in standard_cores:
            if self.work_spec.nCore <= standard_core:
                cores = standard_core
                break

        # Calculate the memory: 2 GBs per core. It needs to be expressed in MB
        # https://cloud.google.com/compute/docs/instances/creating-instance-with-custom-machine-type
        try:
            ram_per_core = self.queue_config.submitter["ram_per_core"]
        except KeyError:
            ram_per_core = 2
        memory = cores * ram_per_core * 1024

        try:
            zone = self.queue_config.zone
        except AttributeError:
            zone = ZONE

        # instance_type = 'zones/{0}/machineTypes/n1-standard-{1}'.format(zone, cores)
        # Use custom machine types to reduce cost
        instance_type = "zones/{0}/machineTypes/custom-{1}-{2}".format(zone, cores, memory)

        return instance_type

    def prepare_metadata(self):
        """
        TODO: prepare any user data and metadata that we want to pass to the VM instance
        :return:
        """

        # read the proxy
        with open(PROXY_PATH, "r") as proxy_file:
            proxy_string = proxy_file.read()

        with open(USER_DATA_PATH, "r") as user_data_file:
            user_data = user_data_file.read()

        try:
            preemptible = self.queue_config.submitter["preemptible"]
        except KeyError:
            preemptible = False

        try:
            disk_size = self.queue_config.submitter["disk_size"]
        except KeyError:
            disk_size = 50

        config = {
            "name": self.name,
            "machineType": self.instance_type,
            "scheduling": {"preemptible": preemptible},
            # Specify the boot disk and the image to use as a source.
            "disks": [{"boot": True, "autoDelete": True, "initializeParams": {"sourceImage": IMAGE, "diskSizeGb": 50}}],
            # Specify a network interface with NAT to access the public internet
            "networkInterfaces": [{"network": "global/networks/default", "accessConfigs": [{"type": "ONE_TO_ONE_NAT", "name": "External NAT"}]}],
            # Allow the instance to access cloud storage and logging.
            "serviceAccounts": [
                {"email": "default", "scopes": ["https://www.googleapis.com/auth/devstorage.read_write", "https://www.googleapis.com/auth/logging.write"]}
            ],
            "metadata": {
                "items": [
                    {"key": "user-data", "value": str(cernvm_aux.encode_user_data(user_data))},
                    {"key": "proxy", "value": proxy_string},
                    {"key": "panda_queue", "value": self.work_spec.computingSite},
                    {"key": "harvester_frontend", "value": HARVESTER_FRONTEND},
                    {"key": "worker_id", "value": self.work_spec.workerID},
                    {"key": "auth_token", "value": self.harvester_token.generate(payload={"sub": str(self.work_spec.batchID)})},
                    {"key": "logs_url_w", "value": "{0}/{1}".format(harvester_config.pandacon.pandaCacheURL_W, "updateLog")},
                    {"key": "logs_url_r", "value": harvester_config.pandacon.pandaCacheURL_R},
                ]
            },
        }

        return config
