#!/bin/bash

echo "starting ray worker node"
ray start --address $1
sleep infinity
