#!/usr/bin/env -S bash --login

# Get directory of run script
basedir=$( cd "$(dirname "$0")" ; pwd -P)

# Create input and output directories if they don't exist
mkdir -p input
mkdir -p output

# Redirect stdout and stderr to a log file
logfile="output/output.log"
exec > >(tee -i "$logfile") 2>&1

# Get L1B and L2A names from command line arguments
L1B_name=$1
L2A_name=$2

if [ -z "$L1B_name" ] || [ -z "$L2A_name" ]; then
	echo "Error: L1B and L2A names must be provided!"
	exit 1
fi

# Get temporal bounds from command line arguments, if provided
date_range=""
if [ -n "$3" ]; then
    date_range=$3
fi
   
# Download GEDI granules to the input directory
conda run --live-stream -n nmbim-env \
      python "${basedir}/download_gedi_granules.py" \
      "$L1B_name" "$L2A_name" input

# Find the files in the input directory
L1B_path=$(find input -type f -name 'GEDI01_B*.h5')
L2A_path=$(find input -type f -name 'GEDI02_A*.h5')
# Since DPS uses symlinks for file arguments,
# we need to check for file or link for the boundary file
boundary_path=$(find input \( \
    -type f -name '*.gpkg' -o -name '*.shp' \) -o \( \
    -type l -lname '*.gpkg' -o -lname '*.shp' \) \
)

# Check if unique L1B file was found
if [ -z "$L1B_path" ]; then
    echo "Error: No L1B file found!"
    exit 1
fi

if [ $(echo "$L1B_path" | wc -l) -gt 1 ]; then
    echo "Warning: Multiple L1B files found:"
    echo "$L1B_path"
    exit 1
fi

# Check if unique L2A file was found
if [ -z "$L2A_path" ]; then
    echo "Error: No L2A file found!"
    exit 1
fi

if [ $(echo "$L2A_path" | wc -l) -gt 1 ]; then
    echo "Warning: Multiple L2A files found:"
    echo "$L2A_path"
    exit 1
fi

# Check if unique boundary file was found, but allow for no boundary
if [ -z "$boundary_path" ]; then
    echo "No boundary file found; proceeding without it."
else
    if [ $(echo "$boundary_path" | wc -l) -gt 1 ]; then
        echo "Warning: Multiple boundary files found; using the first one."
        boundary_path=$(echo "$boundary_path" | head -n 1)
    fi
fi

# Find the config file, which may be a symlink
config_path=$(find input -type f -name 'config.yaml' -o -type l -lname 'config.yaml')

# Print the identified paths
echo "L1B file: $L1B_path"
echo "L2A file: $L2A_path"
echo "Config file: $config_path"
if [ -n "$boundary_path" ]; then
    echo "Boundary file: $boundary_path"
else
    echo "No boundary file specified."
fi

# Print the total contents of input directory
echo
echo "Input directory contents:"
ls -lh input
echo

# Run the processing script
cmd=(
    conda
    run
    --live-stream
    -n
    nmbim-env
    python
    "${basedir}/process_gedi_granules.py"
    "${L1B_path}"
    "${L2A_path}"
    "output"
    "--config"
    "${config_path}"
)

if [ -n "$date_range" ]; then
	cmd+=("--date_range" "$date_range")
fi

if [ -n "$boundary_path" ]; then
    cmd+=("--boundary" "$boundary_path")
fi

"${cmd[@]}"
