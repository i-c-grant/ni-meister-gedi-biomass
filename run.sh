#!/usr/bin/env -S bash --login

# Get directory of run script
basedir=$( cd "$(dirname "$0")" ; pwd -P)

# Activate environment created in build-env.sh
conda activate nmbim-env

# Create input and output directories
mkdir -p input
mkdir -p output

# Provide filenames to L1B and L2A files
L1B=$1
L2A=$2

# Download GEDI files
python ${basedir}/download_gedi_granules.py ${L1B} ${L2A} ${basedir}/input

# Process GEDI files
python ${basedir}/process_gedi_granules.py ${basedir}/input/${L1B} \
       ${basedir}/input/${L2A} ${basedir}/output
