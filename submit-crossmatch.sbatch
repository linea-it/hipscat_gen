#!/bin/bash

#SBATCH --time=04:00:00
#SBATCH -p cpu
#SBATCH -J DASK.crossmatch
#SBATCH --nodes=1
#SBATCH --propagate

echo 'Executing crossmatch...'
time python crossmatch.py
echo 'Done!'

exit
