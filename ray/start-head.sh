#!/bin/bash

echo "starting ray head node"
# Launch the head node
# ray start --head --node-ip-address=$1 --disable-usage-stats --port=$2
ray start --head --node-ip-address=$1 --port=$2 -–redis-password="$redis_password"
sleep infinity
