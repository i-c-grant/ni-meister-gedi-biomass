#!/usr/bin/env -S bash --login

# Get directory of run script
basedir=$( cd "$(dirname "$0")" ; pwd -P)

# Redirect stdout and stderr to a log file
logfile="${basedir}/output.log"
exec > >(tee -i "${logfile}") 2>&1

# Create input and output directories if they don't exist
mkdir -p "${basedir}/input"
mkdir -p "${basedir}/output"

# Download GEDI granules to the input directory
L1B_name=$1
L2A_name=$2

conda run --live-stream -n nmbim-env \
      python "${basedir}/download_gedi_granules.py" \
      "$L1B_name" "$L2A_name" "${basedir}/input"

# Find the L1B, L2A, and boundary files in the basedir/input directory
# Function to find exactly one file matching a pattern
find_single_file() {
    local search_path=$1
    local file_pattern=$2
    local files=($(find "$search_path" $file_pattern))

    if [ ${#files[@]} -gt 1 ]; then
        echo "Error: Multiple files found for pattern '$file_pattern': ${files[@]}"
        exit 1
    elif [ ${#files[@]} -eq 0 ]; then
        echo "Error: No file found for pattern '$file_pattern'!"
        exit 1
    fi

    # Return the found file
    echo "${files[0]}"
}

# Find the L1B, L2A, and boundary files
L1B_path=$(find_single_file "${basedir}/input" "-name 'GEDI01_B*.h5'")
L2A_path=$(find_single_file "${basedir}/input" "-name 'GEDI02_A*.h5'")
boundary_path=$(find_single_file "${basedir}/input" "\( -name '*.gpkg' -o -name '*.shp' \)")

# Check if the required files were found
if [ -z "$L1B_path" ] || [ -z "$L2A_path" ]; then
    echo "L1B or L2A file not found in input directory!"
    exit 1
fi

if [ -z "$boundary_path" ]; then
    echo "Boundary file not found! Proceeding without boundary file."
fi

echo "L1B file: $L1B_path"
echo "L2A file: $L2A_path"
echo "Boundary file: ${boundary_path:-none}"

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
    "${basedir}/output"
)

if [ -n "$boundary_path" ]; then
    cmd+=("--boundary" "$boundary_path")
fi

"${cmd[@]}"
