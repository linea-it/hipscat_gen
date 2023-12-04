#!/bin/bash

echo "starting ray head node"
# Launch the head node
ray start --head --node-ip-address=$1 --disable-usage-stats --port=6379
sleep infinity
