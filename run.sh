#!/bin/bash

# Gets directory of .sh file
basedir=$( cd "$(dirname "$0")" ; pwd -P)

##
source activate base

# Create output dir
OUTPUTDIR="${PWD}/output"
mkdir -p ${OUTPUTDIR}
# INPUT_FILE=$(ls -d *.txt)
L1B_URL=$1
L2A_URL=$2 # e.g. GEDI01_B or GEDI02_A
# YR=$3
python ${basedir}/main.py ${L1B_URL} ${L2A_URL} ${OUTPUTDIR}
