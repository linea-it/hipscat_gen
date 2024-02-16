#!/bin/bash

#SBATCH --time=04:00:00
#SBATCH -p cpu
#SBATCH -J RAY.hipscat
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=56
#SBATCH --propagate
#SBATCH --exclusive

head_node=$(hostname)

# export RAY_NODE_IP=$(hostname --ip-address)
# export RAY_NODE_IP=$(srun --nodes=1 --ntasks=1 -w "$head_node" ip -o -4 addr list ib0 | awk '{print $4}' | cut -d/ -f1)
export RAY_NODE_IP=$(ip -o -4 addr list ib0 | awk '{print $4}' | cut -d/ -f1)
port=6379

redis_passsword=$(uuidgen)
export redis_password

echo "STARTING HEAD at $head_node"
echo "Head node IP: $RAY_NODE_IP"

srun --nodes=1 --ntasks=1 --propagate -w $head_node start-head.sh $RAY_NODE_IP $port &
sleep 10

worker_num=$(($SLURM_JOB_NUM_NODES - 1)) #number of nodes other than the head node
srun -n $worker_num --nodes=$worker_num --ntasks-per-node=1 --propagate --exclude $head_node start-worker.sh $RAY_NODE_IP:$port &
sleep 5
##############################################################################################

# python pipeline_on_ray.py $1

exit
