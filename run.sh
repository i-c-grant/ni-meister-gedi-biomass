#!/usr/bin/env -S bash --login

# Get directory of run script
basedir=$( cd "$(dirname "$0")" ; pwd -P)

# Create output directory if it doesn't exist
mkdir -p output

# Find the L1B, L2A, and boundary files in the basedir/input directory based on correct patterns
L1B=$(find "${basedir}/input" -name "GEDI01_B*.h5" | head -n 1)
L2A=$(find "${basedir}/input" -name "GEDI02_A*.h5" | head -n 1)
boundary=$(find "${basedir}/input" \( -name "*.gpkg" -o -name "*.shp" \) \
	       | head -n 1)

# Check if the required files are found
if [ -z "$L1B" ] || [ -z "$L2A" ]; then
    echo "L1B or L2A file not found in input directory!"
    exit 1
fi

if [ -z "$boundary" ]; then
    echo "Boundary file not found! Proceeding without boundary file."
fi

# Display the files that were found
echo "L1B file: $L1B"
echo "L2A file: $L2A"
echo "Boundary file: ${boundary:-none}"

cmd=(
  conda run --live-stream -n nmbim-env
  python "${basedir}/process_gedi_granules.py"
  "${L1B}"
  "${L2A}"
  "${basedir}/output"
)

if [ -n "$boundary" ]; then
    cmd+=("--boundary" "$boundary")
fi

# Execute the command
"${cmd[@]}"
