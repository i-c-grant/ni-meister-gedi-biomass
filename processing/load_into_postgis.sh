#!/bin/bash

# Check if correct number of arguments is provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <input_directory> <target_table>"
    exit 1
fi

INPUT_DIR="$1"
TARGET_TABLE="$2"
BATCH_SIZE=10

# Set your PostgreSQL connection parameters
PG_HOST="localhost"
PG_PORT="5432"
PG_DB="nmbim_results"
PG_USER="ian"
PG_PASS="grant"

# Check if input directory exists
if [ ! -d "$INPUT_DIR" ]; then
    echo "Error: Input directory does not exist."
    exit 1
fi

# Debugging: Check for .gpkg files
echo "Checking for .gpkg files in $INPUT_DIR"
find "$INPUT_DIR" -name "*.gpkg" | head -n 5

# Debugging: List all .gpkg files and count
echo "Listing found .gpkg files:"
find "$INPUT_DIR" -name "*.gpkg"
echo "Number of .gpkg files found: $(find "$INPUT_DIR" -name "*.gpkg" | wc -l)"

# Debugging: Check for hidden characters in filenames
echo "Listing files with special characters visible:"
find "$INPUT_DIR" -name "*.gpkg" -print0 | xargs -0 ls -b

# Function to merge and load a batch of GeoPackages
process_batch() {
    local batch_num="$1"
    shift
    local input_gpkgs=("$@")
    
    local output_gpkg="/vsimem/merged_batch_${batch_num}.gpkg"
    echo "Merging batch $batch_num"
    # Use ogrmerge.py to merge the GeoPackages
    ogrmerge.py -o "$output_gpkg" -f GPKG "${input_gpkgs[@]}" -overwrite_ds -single
    if [ $? -ne 0 ]; then
        echo "Error merging batch $batch_num"
        return 1
    fi

    echo "Loading in-memory GeoPackage into PostGIS"
    ogr2ogr -f PostgreSQL PG:"host=$PG_HOST port=$PG_PORT dbname=$PG_DB user=$PG_USER password=$PG_PASS" \
        "$output_gpkg" \
        -nln "$TARGET_TABLE" \
        -append \
        -update \
        -lco COPY_WKB=YES \
        -skipfailures
    if [ $? -ne 0 ]; then
        echo "Error loading batch $batch_num into PostGIS"
        return 1
    fi

    # Cleanup in-memory file
    gdal_translate -f MEM /vsimem/null "$output_gpkg" -q
    gdal_translate -f MEM /vsimem/null /vsimem/null -q
    echo "Batch $batch_num completed"
}

export -f process_batch
export PG_HOST PG_PORT PG_DB PG_USER PG_PASS TARGET_TABLE

# Find all GeoPackages (compatible with older Bash versions)
IFS=$'\n' read -r -d '' -a gpkg_files < <(find "$INPUT_DIR" -name "*.gpkg" && printf '\0')
total_files=${#gpkg_files[@]}

# Debugging: Check array content
echo "Number of files in gpkg_files array: ${#gpkg_files[@]}"
echo "First few files:"
printf '%s\n' "${gpkg_files[@]:0:5}"

# Debugging: Check if ogrmerge.py can see the files
if [ ${#gpkg_files[@]} -gt 0 ]; then
    echo "Testing ogrmerge.py with first file:"
    first_file="${gpkg_files[0]}"
    ogrmerge.py -list "$first_file"
else
    echo "No .gpkg files found in the array"
fi

# Debugging: Verify GDAL installation
echo "GDAL version:"
gdalinfo --version

# Determine number of CPU cores and set max parallel jobs
max_jobs=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 1)
echo "Using up to $max_jobs parallel jobs"

# Create batches and process in parallel
seq 0 $BATCH_SIZE $((total_files - 1)) | \
parallel --jobs $max_jobs --halt now,fail=1 --joblog parallel.log --eta \
'
    batch_num={#}
    start_index={}
    end_index=$((start_index + BATCH_SIZE - 1))
    if [ $end_index -ge '"$total_files"' ]; then
        end_index=$(('"$total_files"' - 1))
    fi
    batch_files=("${gpkg_files[@]:$start_index:$BATCH_SIZE}")
    process_batch "$batch_num" "${batch_files[@]}"
'

echo "All GeoPackages processed and loaded into PostGIS table: $TARGET_TABLE"
