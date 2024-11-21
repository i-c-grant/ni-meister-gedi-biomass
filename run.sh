#!/usr/bin/env -S bash --login

# Get directory of run script
basedir=$( cd "$(dirname "$0")" ; pwd -P)

# Create input and output directories if they don't exist
mkdir -p input
mkdir -p output

# Redirect stdout and stderr to a log file
logfile="output/output.log"
exec > >(tee -i "$logfile") 2>&1

# Check if required arguments are provided
if [ $# -lt 3 ]; then
    echo "Error: L1B, L2A, and L4A names are required!"
    echo "Usage: $0 <L1B_name> <L2A_name> <L4A_name> [date_range]"
    exit 1
fi

# Assign arguments to variables
L1B_name="$1"
L2A_name="$2"
L4A_name="$3"
date_range="${4:-}"  # Optional 4th argument for date range

# print parsed arguments
echo "L1B name: $L1B_name"
echo "L2A name: $L2A_name"
echo "L4A name: $L4A_name"
echo "Date range: $date_range"
   
# Download GEDI granules to the input directory
conda run --live-stream -n nmbim-env \
      python "${basedir}/download_gedi_granules.py" \
      "$L1B_name" "$L2A_name" "$L4A_name" input

# Print the total contents of input directory
echo
echo "Input directory contents at run start:"
ls -lh input
echo


# Find the files in the input directory
L1B_path=$(find input -type f -name 'GEDI01_B*.h5')
L2A_path=$(find input -type f -name 'GEDI02_A*.h5')
L4A_path=$(find input -type f -name 'GEDI04_A*.h5')
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

# Check if unique L4A file was found
if [ -z "$L4A_path" ]; then
    echo "Error: No L4A file found!"
    exit 1
fi

if [ $(echo "$L4A_path" | wc -l) -gt 1 ]; then
    echo "Warning: Multiple L4A files found:"
    echo "$L4A_path"
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
config_path=$(find input \( \
    -type f \( -name '*config.yaml' -o -name '*config.yml' \) -o \
    -type l \( -lname '*config.yaml' -o -lname '*config.yml' \) \
\))

# Check if unique config file was found
if [ -z "$config_path" ]; then
	echo "Error: No config file found!"
	exit 1
fi

if [ $(echo "$config_path" | wc -l) -gt 1 ]; then
	echo "Warning: Multiple config files found:"
	echo "$config_path"
	exit 1
fi

# Find the HSE raster, which is named 'hse.tif' and may be a symlink
hse_path=$(find input \( \
	-type f -name '*hse.tif' -o \
	-type l -lname '*hse.tif' \))

# Check if unique HSE raster was found
if [ -z "$hse_path" ]; then
	echo "Error: No HSE raster found!"
	exit 1
fi

if [ $(echo "$hse_path" | wc -l) -gt 1 ]; then
    echo "Warning: Multiple HSE files found:"
    echo "$hse_path"
    exit 1
fi

# Find the k_allom raster, which is named 'k_allom.tif'
# and may be a symlink    
k_allom_path=$(find input \( \
	-type f -name '*k_allom.tif' -o \
	-type l -lname '*k_allom.tif' \))

# Check if unique k_allom raster was found
if [ -z "$k_allom_path" ]; then
	echo "Error: No k_allom raster found!"
	exit 1
fi

if [ $(echo "$k_allom_path" | wc -l) -gt 1 ]; then
    echo "Warning: Multiple k_allom files found:"
    echo "$k_allom_path"
    exit 1
fi

# Print the identified paths
echo "L1B file: $L1B_path"
echo "L2A file: $L2A_path"
echo "L4A file: $L4A_path"
echo "Config file: $config_path"
echo "HSE raster: $hse_path"
echo "k_allom raster: $k_allom_path"
if [ -n "$boundary_path" ]; then
    echo "Boundary file: $boundary_path"
else
    echo "No boundary file specified."
fi

# Print the total contents of input directory again
echo
echo "Input directory contents after downloads:"
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
    "${L4A_path}"
    "${hse_path}"
    "${k_allom_path}"
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
cmd_exit_code=$?

# If there's a .gpkg in output, compress it with bzip2
output_gpkg=$(find output -type f -name '*.gpkg')

if [ -n "$output_gpkg" ]; then
	echo "Compressing output .gpkg file with bzip2..."
	bzip2 -9 "$output_gpkg"
fi

# Exit with the exit code from cmd
exit $cmd_exit_code

