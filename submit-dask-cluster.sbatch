#!/bin/bash

#SBATCH --time=04:00:00
#SBATCH -p cpu
#SBATCH -J DASK.hipscat
#SBATCH --nodes=1
#SBATCH --propagate

echo 'Executing hipsimport...'
cat $1
time python hipsimport-dask-cluster.py $1
echo 'Done!'

exit
