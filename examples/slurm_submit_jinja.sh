#!/bin/bash

# Job Name and Files (also --job-name)
#SBATCH -J w_{{worker.workerID}}
#Output and error (also --output, --error):
#SBATCH -o {{worker.accessPoint}}/slurm_stdout.txt
#SBATCH -e {{worker.accessPoint}}/slurm_stderr.txt

#Initial working directory (also --chdir):
#SBATCH -D ./

#SBATCH --no-requeue
#Setup of execution environment
#SBATCH --export=NONE
#SBATCH --get-user-env
#SBATCH --account={% if nNode<16 %}"pr58be"{% else %} "general" {% endif %}
#SBATCH --mcs-label=pr58be

# Wall clock limit:
#SBATCH --time={{walltime}}
#SBATCH --partition=micro
##SBATCH --time={{walltime}}
##SBATCH --partition={% if nNode<16 %}"test"{% else %}"general" {% endif %}
#SBATCH --ntasks-per-node 1

<<<<<<< HEAD
####SBATCH -N {{nNode}}
#SBATCH -N {{worker.nJobs}}
=======
#SBATCH -N {{nNode}}
>>>>>>> f75cee727755c4f9c6d301c06bcdd1b9cb53db8d

set -e 

POSTMORTEM_FILE={{worker.accessPoint}}/FINISHED

function write_postmortem(){
    python -c "$(printf "import json,sys; a={'error_code': sys.argv[1], 'worker_status': 'finished' if sys.argv[1]=='0' else 'failed', 'last_command': sys.argv[2]}; print json.dump(a, open(sys.argv[3], 'w'));")" $? "$BASH_COMMAND" "$POSTMORTEM_FILE"
}

trap write_postmortem INT TERM EXIT


# avoid some libpsm_infinipath.so.1: cannot open shared object file: No such file or directory
module load slurm_setup


nTasks=$SLURM_JOB_NUM_NODES

export PANDA_QUEUE=LRZ-LMU_MUC

export HARVESTER_DIR=$PANDA_HOME  # PANDA_HOME is defined in etc/sysconfig/panda_harvester
# Actually not for virtualenv
export HARVESTER_DIR=/home/r/Rodney.Walker/harvester
export HARVESTER_WORKER_ID={{worker.workerID}}
export HARVESTER_ACCESS_POINT={{worker.accessPoint}}
export HARVESTER_NNODE={{nNode}}
export HARVESTER_NTASKS=$nTasks
export HARVESTER_CONTAINER_IMAGE=not_used

#set variable to define HARVESTER running mode (mapType) for this worker
#  possible choices OneToOne, OneToMany, ManyToOne
export HARVESTER_MAPTYPE=ManyToOne

# Directory containing wrapper,pilot tarball and json
#PILOT_BOOTSTRAP_DIR=/hppfs/work/pr58be/ri32buz3/pilot
#PILOT_BOOTSTRAP_DIR=/hppfs/work/pr58be/ri32buz3/harvester/cache
PILOT_BOOTSTRAP_DIR={{submitter.pilotBootstrapDir}}

export pilot_wrapper_file=$PILOT_BOOTSTRAP_DIR/runpilot2-wrapper.sh
export pilot_tar_file=$PILOT_BOOTSTRAP_DIR/pilot2.tar.gz
export pilot_schedconfig_JSON_file=$PILOT_BOOTSTRAP_DIR/agis_schedconf.json
export pilot_queuedata_file=$PILOT_BOOTSTRAP_DIR/LRZ-LMU_MUC.all.json
export pilot_ddmendpoints_file=$PILOT_BOOTSTRAP_DIR/agis_ddmendpoints.json

export create_singularity_wrapper_file=$PILOT_BOOTSTRAP_DIR/create_singularity_srun_wrapper.sh

# get the PanDA Job IDs from worker_pandaids.json 
PANDA_JOB_IDS=( "$(python -c "import json;pids = json.load(open('worker_pandaids.json'));print(' '.join(str(x) for x in pids))")" )


# environment variable initialization finished


# record the various HARVESTER envars
echo
echo [$SECONDS] "Harvester Top level directory - "$HARVESTER_DIR
echo [$SECONDS] "Harvester accessPoint - "$HARVESTER_ACCESS_POINT
echo [$SECONDS] "Harvester Worker ID - "$HARVESTER_WORKER_ID
echo [$SECONDS] "Harvester workflow (MAPTYPE) - "$HARVESTER_MAPTYPE
echo [$SECONDS] "Harvester accessPoint - "$HARVESTER_ACCESS_POINT
echo [$SECONDS] "pilot wrapper file - "$pilot_wrapper_file
echo [$SECONDS] "Pilot tar file - "$pilot_tar_file
echo [$SECONDS] "Container image - "$HARVESTER_CONTAINER_IMAGE
echo [$SECONDS] "Command to setup release in container - "$HARVESTER_CONTAINER_RELEASE_SETUP_FILE
echo [$SECONDS] "PANDA_JOB_IDS: $PANDA_JOB_IDS"
echo [$SECONDS] "Number of Nodes to use - "$HARVESTER_NNODE
echo [$SECONDS] "Number of tasks for srun - "$HARVESTER_NTASKS 
echo [$SECONDS] "ATHENA_PROC_NUMBER - "$ATHENA_PROC_NUMBER
echo

echo [$SECONDS] TMPDIR = $TMPDIR
echo [$SECONDS] unset TMPDIR
unset TMPDIR
echo [$SECONDS] TMPDIR = $TMPDIR

echo [$SECONDS] ATLAS_LOCAL_ROOT_BASE = $ATLAS_LOCAL_ROOT_BASE
#echo [$SECONDS] unset ATLAS_LOCAL_ROOT_BASE
#unset ATLAS_LOCAL_ROOT_BASE
echo [$SECONDS] ATLAS_LOCAL_ROOT_BASE = $ATLAS_LOCAL_ROOT_BASE

# In OneToOne running Harvester expects files in $HARVESTER_ACCESS_POINT
# In OneToMany running (aka Jumbo jobs) Harvester expects fdiles in $HARVESTER_ACCESS_POINT
#    Note in jumbo job running pilot wrapper will ensure each pilot runs in a different directory



# Loop over PanDA ID's

for PANDA_JOB_ID in $PANDA_JOB_IDS;  do

    echo [$SECONDS] "PanDA Job ID - "$PANDA_JOB_ID
    
    # test if running in ManyToOne mode and set working directory accordingly
    export HARVESTER_WORKDIR=$HARVESTER_ACCESS_POINT
    if [[ "$HARVESTER_MAPTYPE" = "ManyToOne" ]] ; then
	export HARVESTER_WORKDIR=$HARVESTER_ACCESS_POINT/$PANDA_JOB_ID
    fi    
    echo [$SECONDS] "setting Harvester Work Directory to - "$HARVESTER_WORKDIR

    # seems have to mkdir when testing with 1-to-1
    # move into job directory
    echo [$SECONDS] "Changing to $HARVESTER_WORKDIR "
    cd $HARVESTER_WORKDIR
    
    #copy pilot wrapper and panda pilot tarball to working directory
    echo [$SECONDS] copy $pilot_wrapper_file to $HARVESTER_WORKDIR
    cp -v $pilot_wrapper_file $HARVESTER_WORKDIR
    echo [$SECONDS] copy $pilot_tar_file to $HARVESTER_WORKDIR
    cp -v $pilot_tar_file $HARVESTER_WORKDIR

   # test for json files used by pilot's infoservice
    if [ ! -f $pilot_schedconfig_JSON_file ] ; then
	echo [$SECONDS] Warning the file - "$pilot_schedconfig_JSON_file" does not exist
    else
	cp -v $pilot_schedconfig_JSON_file $HARVESTER_WORKDIR/agis_schedconf.json 
    fi   

    if [ ! -f $pilot_queuedata_file ] ; then
	echo [$SECONDS] Warning the file - "$pilot_queuedata_file" does not exist
    else
	cp -v $pilot_queuedata_file $HARVESTER_WORKDIR/queuedata.json 
    fi   

    if [ ! -f $pilot_ddmendpoints_file ] ; then
	echo [$SECONDS] Warning the file - "$pilot_ddmendpoints_file" does not exist
    else
	cp -v $pilot_ddmendpoints_file $HARVESTER_WORKDIR/agis_ddmendpoints.json 
    fi   

    # create singularity wrapper script file in $HARVESTER_WORKDIR 
    $create_singularity_wrapper_file 
    if [ ! -x ./singularity_wrapper_file.sh ] ; then
	echo "Error - Singularity wrapper_file.sh does not exist or is not executable - exit early!"
	exit 2
    fi
    ls -l ./singularity_wrapper_file.sh
    echo [$SECONDS] "singularity_wrapper_file.sh contents: "
    cat ./singularity_wrapper_file.sh
    echo 

    # start execution

    # In ManyToOne running or OnetoOne running want one launch on srun per node - per PanDAid
    if [[ "$HARVESTER_MAPTYPE" = "OneToMany" ]] ; then    #AKA Jumbo jobs
	# RW not supported at MUC
        # now start things up
	echo [$SECONDS] "Launching srun command from : " $PWD

	#export IMAGE_BASE=/hppfs/work/pr58be/ri32buz3/images/
	export IMAGE_BASE={{submitter.imageBase}}
	# CONTAINER_IMAGE=
        # read it from file

	echo -n "[$SECONDS] "
        #  srun -n $nTasks  -o PandaID-$PANDA_JOB_ID-%N-%j-%t.log \
	#         singularity exec --bind "/hpcgpfs01/work/benjamin/yampl:/opt/yampl" $HARVESTER_CONTAINER_IMAGE \
        #      /bin/bash ./BNL_runpilot2-wrapper.sh -s $PANDA_QUEUE -r $PANDA_QUEUE \
	#         -q $PANDA_QUEUE -j managed -i PR -t --mute --harvester SLURM_NODEID \
	#         --harvester_workflow $HARVESTER_MAPTYPE \
	#         --container -w generic --pilot-user=atlas --hpc-resource cori --use-https False \
	#         -d --harvester-submit-mode=PUSH \
	#         --harvester-workdir=$HARVESTER_WORKDIR \
	#         --allow-same-user=False --resource-type MCORE -z 

	set -x
	srun -n $nTasks  -o PandaID-$PANDA_JOB_ID-%N-%j-%t.log \
	         singularity exec --bind "/hpcgpfs01/work/benjamin/yampl:/opt/yampl" $HARVESTER_CONTAINER_IMAGE \
             /bin/bash ./BNL_runpilot2-wrapper.sh -s $PANDA_QUEUE -r $PANDA_QUEUE \
	         -q $PANDA_QUEUE -j managed -i PR -t --mute --harvester SLURM_NODEID \
	         --harvester_workflow $HARVESTER_MAPTYPE \
	         --container -w generic --pilot-user=atlas --hpc-resource cori --use-https False \
	         -d --harvester-submit-mode=PUSH \
	         --harvester-workdir=$HARVESTER_WORKDIR \
	         --allow-same-user=False --resource-type MCORE -z &
        set +x
    else
        # now start things up
	echo [$SECONDS] "Hostname : " $HOSTNAME
	echo [$SECONDS] "Launching srun command from : " $PWD

	echo [$SECONDS] srun -n 1 -N 1 -o PandaID-$PANDA_JOB_ID-%N-%j-%t.log ./singularity_wrapper_file.sh 
        srun -n 1 -N 1 -o PandaID-$PANDA_JOB_ID-%N-%j-%t.log ./singularity_wrapper_file.sh &
    fi
    

done

echo [$SECONDS] " wait command  "
wait

