#!/bin/bash

source `dirname $CONDA_EXE`/activate || { echo "Failed to activate Conda environment"; exit 1; }
conda activate lsdb