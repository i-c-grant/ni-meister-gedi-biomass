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
    
    local output_gpkg="merged_batch_${batch_num}.gpkg"
    echo "Merging batch $batch_num"
    echo "Input files for this batch:"
    printf '%s\n' "${input_gpkgs[@]}"
    
    # Check file permissions and existence
    for file in "${input_gpkgs[@]}"; do
        if [ ! -r "$file" ]; then
            echo "Error: Cannot read file $file"
        fi
        if [ ! -s "$file" ]; then
            echo "Error: File $file is empty"
        fi
    done
    
    # Use ogrinfo to check each file
    for file in "${input_gpkgs[@]}"; do
        echo "Checking $file with ogrinfo:"
        ogrinfo -so "$file"
    done
    
    # Use ogrmerge.py to merge the GeoPackages
    echo "Running ogrmerge.py command:"
    echo ogrmerge.py -o "$output_gpkg" -f GPKG "${input_gpkgs[@]}" -overwrite_ds -single
    ogrmerge.py -o "$output_gpkg" -f GPKG "${input_gpkgs[@]}" -overwrite_ds -single
    if [ $? -ne 0 ]; then
        echo "Error merging batch $batch_num"
        return 1
    fi
    ogrinfo $output_gpkg

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

# Find all GeoPackages
mapfile -d $'\0' gpkg_files < <(find "$INPUT_DIR" -name "*.gpkg" -print0)
total_files=${#gpkg_files[@]}

# Debugging: Check array content
echo "Number of files in gpkg_files array: ${#gpkg_files[@]}"
echo "First few files:"
printf '%s\n' "${gpkg_files[@]:0:5}"

# Debugging: Check if ogrinfo can see the files
if [ ${#gpkg_files[@]} -gt 0 ]; then
    echo "Testing ogrinfo with first file:"
    first_file="${gpkg_files[0]}"
    ogrinfo -so "$first_file"
else
    echo "No .gpkg files found in the array"
fi

# Debugging: Verify GDAL installation
echo "GDAL version:"
gdalinfo --version

# Process batches sequentially
for ((i=0; i<total_files; i+=BATCH_SIZE)); do
    batch_num=$((i / BATCH_SIZE + 1))
    end=$((i + BATCH_SIZE))
    if [ $end -gt $total_files ]; then
        end=$total_files
    fi
    batch_files=("${gpkg_files[@]:i:BATCH_SIZE}")
    process_batch "$batch_num" "${batch_files[@]}"
    if [ $? -ne 0 ]; then
        echo "Error processing batch $batch_num. Stopping execution."
        exit 1
    fi
done

echo "All GeoPackages processed and loaded into PostGIS table: $TARGET_TABLE"
