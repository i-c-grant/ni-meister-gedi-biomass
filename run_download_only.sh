#!/usr/bin/env -S bash --login

# Get directory of run script
basedir=$( cd "$(dirname "$0")" ; pwd -P)

# Create input and output directories if they don't exist
mkdir -p "${basedir}/input"
mkdir -p "${basedir}/output"

# Redirect stdout and stderr to a log file
logfile="${basedir}/output/output.log"
exec > >(tee -i "$logfile") 2>&1

# Download GEDI granules to the input directory
L1B_name=$1
L2A_name=$2

conda run --live-stream -n nmbim-env \
      python "${basedir}/download_gedi_granules.py" \
      "$L1B_name" "$L2A_name" "${basedir}/input"
