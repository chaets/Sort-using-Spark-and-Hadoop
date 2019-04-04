#!/bin/bash

#SBATCH --nodes=1
#SBATCH --exclude=hadoop-c,hadoop-i,hadoop-b,hadoop-e,hadoop-g,hadoop-d
#SBATCH --output=HadoopSort8GB.log
#SBATCH --wait-all-nodes=1

srun ./hadoopsort8GB.sh $SLURM_JOB_NODELIST
