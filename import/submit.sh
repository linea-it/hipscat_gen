#!/bin/bash

#SBATCH --time=04:00:00
#SBATCH -p cpu
#SBATCH -J hipscat
#SBATCH --nodes=1
#SBATCH --propagate

export EXECUTOR=slurm

printf 'Executing hipsimport...\n\n'
printf '%b\n\n' "$(cat $1)"
time python hipsimport.py $1
printf '\nDone!\n'