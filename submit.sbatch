#!/bin/bash

#SBATCH --time=04:00:00
#SBATCH -p cpu
#SBATCH -J hipscat.SOLO
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=56
#SBATCH --propagate

echo 'Executing hipsimport...'
cat $1
python hipsimport.py $1
echo 'Done!'

exit
