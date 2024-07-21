#!/bin/bash

# Get directory of run script
basedir=$( cd "$(dirname "$0")" ; pwd -P)

# Activate environment that was created in the build-env.sh file
conda activate nmbim-env

# Create output dir
mkdir -p output

# URLs to L1B and L2A input files
L1B_URL=$1
L2A_URL=$2

python ${basedir}/main.py ${L1B_URL} ${L2A_URL} output
