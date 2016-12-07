#!/bin/bash -l
#SBATCH -p debug
#SBATCH -N {nNode}
#SBATCH --signal=SIGUSR1@60
#SBATCH -t 00:30:00
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task={nCorePerNode}
#SBATCH -J ES_job
#SBATCH -o {accessPoint}/athena_stdout.txt
#SBATCH -e {accessPoint}/athena_stderr.txt

module load mpi4py

export RUCIO_ACCOUNT=panda

export PROJ_DIR=/project/projectdirs/atlas
export PYTHONPATH=$PROJ_DIR/sw/python-yampl/python-yampl/1.0/lib.linux-x86_64-2.6:$PYTHONPATH
export PYTHONPATH=$PROJ_DIR/pilot/grid_env/boto/lib/python2.6/site-packages:$PYTHONPATH
export PYTHONPATH=$PROJ_DIR/pilot/grid_env/external:$PYTHONPATH

export LD_LIBRARY_PATH=$PROJ_DIR/sw/python-yampl/yampl/1.0/lib:$LD_LIBRARY_PATH

source $PROJ_DIR/pilot/grid_env/setup.sh

export PILOT_DIR=/global/homes/t/tmaeno/pilot
export PYTHONPATH=$PILOT_DIR:$PYTHONPATH

export WORK_DIR={accessPoint}
cd $WORK_DIR

srun -N {nNode} python-mpi $PILOT_DIR/HPC/HPCJob.py \
    --globalWorkingDir=$WORK_DIR  --localWorkingDir=$WORK_DIR --dumpEventOutputs \
    --dumpFormat json
