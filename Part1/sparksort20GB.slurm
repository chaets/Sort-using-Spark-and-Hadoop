#!/bin/bash

#SBATCH --nodes=1
#SBATCH --exclude=hadoop-c,hadoop-a,hadoop-b,hadoop-d
#SBATCH --output=SparkSort20GB.log
#SBATCH --wait-all-nodes=1

srun ./sparksort20GB.sh $SLURM_JOB_NODELIST
