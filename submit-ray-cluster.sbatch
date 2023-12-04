#!/bin/bash

#SBATCH --time=04:00:00
#SBATCH -p cpu
#SBATCH -J RAY.hipscat
#SBATCH --nodes=3
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=56
#SBATCH --propagate

head_node=$(hostname)
export RAY_NODE_IP=$(hostname --ip-address)
# if we detect a space character in the RAY_NODE_IP, we'll
# convert it to an ipv4 address. This step is optional.
if [[ "$RAY_NODE_IP" == *" "* ]]; then
IFS=' ' read -ra ADDR <<<"$RAY_NODE_IP"
if [[ ${#ADDR[0]} -gt 16 ]]; then
  export RAY_NODE_IP=${ADDR[1]}
else
  export RAY_NODE_IP=${ADDR[0]}
fi
fi
port=6379

echo "STARTING HEAD at $head_node"
echo "Head node IP: $RAY_NODE_IP"

srun --nodes=1 --ntasks=1 --propagate -w $head_node start-head.sh $RAY_NODE_IP &
sleep 10

worker_num=$(($SLURM_JOB_NUM_NODES - 1)) #number of nodes other than the head node
srun -n $worker_num --nodes=$worker_num --ntasks-per-node=1 --propagate --exclude $head_node start-worker.sh $RAY_NODE_IP:$port &
sleep 5
##############################################################################################

python pipeline_on_ray.py $1

exit
