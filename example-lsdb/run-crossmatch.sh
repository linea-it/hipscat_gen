#!/bin/bash

#SBATCH --time=04:00:00
#SBATCH -p cpu
#SBATCH -J DASK.crossmatch
#SBATCH --nodes=1
#SBATCH --propagate

export EXECUTOR=slurm

printf 'Executing crossmatch...\n\n'
time python crossmatch.py
printf '\nDone!\n'