#!/bin/bash
#
#SBATCH --job-name=pwmig_test
#SBATCH --output=/scratch1/10208/chenboyin/frontera/out/pwmig_test_%j.out
#SBATCH --error=/scratch1/10208/chenboyin/frontera/err/pwmig_test_%j.err
#SBATCH --time=3:00:00
#SBATCH --nodes=3                   
#SBATCH --ntasks-per-node=1         
#SBATCH -p normal

# module load tacc-apptainer
cd $SCRATCH/containers

apptainer instance list | grep mongodb_instance && apptainer instance stop mongodb_instance
rm -rf $SCRATCH/mongodb/data
mkdir -p $SCRATCH/mongodb/data
mkdir -p $SCRATCH/mongodb/log

apptainer instance start --bind $SCRATCH/mongodb/data:/data/db mongo_4.4.sif mongodb_instance
apptainer exec instance://mongodb_instance mongod --bind_ip_all --fork --logpath $SCRATCH/mongodb/log/mongod.log
# Run the main container with the processing scripts
apptainer exec --bind $SCRATCH/pwmig_work:/test parallel_pwmig_latest.sif bash -c "
cd /test
python pwmig_testsuite_dataprep.py
python pwstack.py
python pwmig.py
"
