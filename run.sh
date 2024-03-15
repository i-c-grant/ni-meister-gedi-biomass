#!/bin/bash

# Gets directory of .sh file
basedir=$( cd "$(dirname "$0")" ; pwd -P)

# Activate environment that was created in the builed-env.sh file
# source activate osgeo-env

# Create output dir
# OUTPUTDIR="${PWD}/output"
mkdir -p output
# INPUT_FILES
L1B_URL=$1
L2A_URL=$2 # e.g. GEDI01_B or GEDI02_A
# YR=$3
# python ${basedir}/main.py ${L1B_URL} ${L2A_URL} ${OUTPUTDIR}
python ${basedir}/main.py ${L1B_URL} ${L2A_URL} output
