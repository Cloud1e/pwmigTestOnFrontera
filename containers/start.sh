#!/bin/bash
#SBATCH -J pwmig_stack
#SBATCH -o output.%j
#SBATCH -p rtx               # Queue name
#SBATCH -N 1                 # Single node
#SBATCH -n 2                 # Two tasks for two containers
#SBATCH -t 02:00:00         # Run time (hh:mm:ss)


# Start MongoDB in background
# rm -rf $SCRATCH/mongodb/data
# mkdir -p $SCRATCH/mongodb/data
module load tacc-apptainer
cd $SCRATCH/containers
apptainer instance start --bind $SCRATCH/mongodb/data:/data/db mongo_4.4.sif mongodb
apptainer exec instance://mongodb mongod --bind_ip_all --fork --logpath $SCRATCH/mongodb/mongod.log


# Start parallel_pwmig
apptainer shell --bind $SCRATCH/pwmig_work:/test parallel_pwmig_latest.sif
cd /test
python pwmig_testsuite_dataprep.py
python pwstack.py
python pwmig.py