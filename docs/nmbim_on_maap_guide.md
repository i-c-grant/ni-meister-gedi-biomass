# User Guide for NMBIM Algorithm on MAAP 
This guide provides step-by-step instructions to run the NMBIM algorithm on MAAP, then gives more detailed information about the deployment on MAAP. 

## Quick Start
Here are minimal instructions to run the NMBIM algorithm on MAAP for a given spatial and temporal query.

1. Create parameter rasters (HSE and k_allom) according to your chosen parameterization method
   - Format: GeoTIFF in EPSG:4326 projection
   - Names must end in `hse.tif` and `k_allom.tif`
   - Ensure complete coverage of your area of interest

2. Create boundary layer for the region to be processed
   - Format: GeoPackage (.gpkg) or Shapefile (.shp) in EPSG:4326
   - Best practice: Generate from rasters to ensure complete parameterization
   - Must only include areas with valid HSE and k_allom values
   - Multiple polygons supported but must not overlap

3. Get or create a configuration file
   - Option A: Use an existing config file (see config/config.yaml in the ni-meister-gedi-biomass repository for a default)
   - Option B: Create new config file:
     - Name as `config.yaml` or `config.yml`
     - Define filters section (spatial, temporal, quality)
     - Define processing pipeline steps
     - Can leave temporal/spatial parameters blank if using MAAP job submission API

4. Clone source repository into MAAP ADE from the MAAP GitLab.
     ```bash
     git clone https://gitlab.maap-project.org/iangrant/ni-meister-gedi-biomass.git
     ```

5. Upload files to your MAAP workspace bucket
   - Navigate my-private-bucket in the MAAP ADE graphical file browser
   - Upload hse.tif, k_allom.tif, boundary.gpkg, and config.yaml using the interface
   - It's easiest to isolate these in an "inputs" folder, with a subfolder for a particular model run


6. Run processing script
   - Invoke run_on_maap.py in a MAAP terminal or notebook
   - Jobs will be identified, submitted, and monitored
   - Once complete, the scripts creates a local output directory (`run_output_<YYYYMMDD_HHMMSS>`) containing `run.log` and a copy of your config file
   ```bash
   # Navigate to the cloned repository
   cd ni-meister-gedi-biomass

   # Run the script with additional options
   python run_on_maap.py \
     --username {your_username} \
     --tag {unique_processing_id} \
     --config s3://maap-ops-workspace/{username}/my-private-bucket/config.yaml \
     --hse s3://maap-ops-workspace/{username}/my-private-bucket/hse.tif \
     --k_allom s3://maap-ops-workspace/{username}/my-private-bucket/k_allom.tif \
     --algo_id nmbim_biomass_index \
     --algo_version main \
     --boundary s3://maap-ops-workspace/{username}/my-private-bucket/boundary.gpkg \
     --date_range "2019-01-01,2020-12-31" \
     --job_limit 100 \
     --redo-of previous_tag \
     --no-redo
   ```

This script will figure out what GEDI files are necessary to cover the query and submit the necessary jobs to the MAAP DPS.

7. Monitor job progress
The script will display a progress bar showing:
     - Total completed jobs
     - Current status counts (Succeeded, Failed, Running)
     - Time of last status update (updates occur infrequently for very large job batches)

Wait until all or most jobs are complete. Press Ctrl-C once to suspend monitoring. You can then choose to resume, resubmit failed jobs, or exit. Unless you've run the command with the "--no-redo" option, you'll also get an option to resubmit failed jobs at the end of the run.

8. Download the results
   
Once the run is complete, you'll probably want to download the results from MAAP for further processing. (Note: In theory, you could implement whatever further processing steps you needed as additional MAAP algorithms, but in practice the MAAP algorithm registration process makes this cumbersome for many post-processing tasks). 
IMPORTANT: Do not try to download big files (> 1GB or so) directly from the MAAP ADE interface (i.e. your MAAP workspace). This will route the download through the MAAP ADE cluster, which is shared by all MAAP users and has limited resources.

Instead, you should use the AWS S3 CLI interface to download the data to your computer using AWS. To do that, you need to get temporary credentials to access the MAAP S3 bucket from your own machine. 

I use a simple script run on MAAP to do this. This writes the credentials you need to "temp_credentials.json" in your MAAP workspace. 

``` python
from maap.maap import MAAP
import json

maap = MAAP(maap_host='api.maap-project.org')
cred = maap.aws.workspace_bucket_credentials()

with open('temp_credentials.json', 'w') as f:
    json.dump(cred, f)
```

With these credentials, you can use the provided `download_from_workspace.py`  (use `download_from_workspace.py --help` to see usage hints) script along with your temporary AWS credentials JSON file. I've found the easiest way to do this is to get the download link of the credentials file on MAAP, then embed the credentials request within the download call (shown below). However, you can use any method that passes the credentials JSON to download_from_workspace.py.

   ```bash
   # Save temporary credentials to creds.json
   CREDENTIALS_URL={<insert link from MAAP "Copy Download Link" option here>} 

   # Download results for your run
   python download_from_workspace.py \
     --credentials <(curl -s $CREDENTIALS_URL) \
     --output-dir ./run_results \
     --algorithm nmbim_biomass_index \
     --version main \
     --tag {unique_processing_id}

   # Decompress downloaded GeoPackages
   bunzip2 run_results/*.gpkg.bz2
   ```

Alternatively, you can still use AWS CLI directly:

   ```bash
   # List output files
   aws s3 ls s3://maap-ops-workspace/{username}/dps_output/nmbim_biomass_index/main/{unique_processing_id}/ --recursive | grep '.gpkg.bz2$'
   # Download compressed GeoPackages
   aws s3 cp s3://maap-ops-workspace/{username}/dps_output/nmbim_biomass_index/main/{unique_processing_id}/ ./run_results \
     --recursive --exclude "*" --include "*.gpkg.bz2"
   # Decompress files
   bunzip2 run_results/*.gpkg.bz2
   ```

## Detailed description of arguments for run_on_maap.py 
## Inputs

### Boundary

The boundary input defines the geographic area of interest for processing. It must be provided as either a geopackage (.gpkg) or shapefile (.shp) in the EPSG:4326 coordinate system. The boundary should encompass only areas where both HSE and k_allom parameters have valid values. While multiple polygons are supported, they should not overlap.

### Configuration

The algorithm accepts a configuration file that specifies the filters and processing pipeline to be run on the GEDI waveforms within the query window. This file is in YAML format and must be named config.yaml or config.yml.

The first section of the configuration file, `filters`, references spatial, temporal, and quality filters defined in filters.py. Each filter configuration within this section also specifies any filter-specific parameters.

Note: Since the MAAP deployment of NMBIM algorithm accepts temporal and spatial queries as arguments through the MAAP job submission API, it is permitted to leave the parameters for temporal and spatial filters blank in the configuration file. The parameters supplied through the job submission API will be applied to the filters as necessary.

The second section of the configuration file specifies the processing pipeline to be run on the GEDI waveforms. Each entry in this section specifies one step of the pipeline; these steps are applied sequentially to each waveform in the model run. The `alg_fun` value within each processing step indicates the name of the Python function to be applied for that step, `input_map` determines what data from each Waveform object is supplied as input to that function, and `output_path` gives the path within each Waveform object to which the outputs will be written.

### HSE and k_allom

The height scaling exponent and allometric coefficient are specified using GeoTIFF raster files in EPSG:4326 coordinate system. The files must be named hse.tif and k_allom.tif, respectively (this convention facilitates worker-side processing of the rasters). The rasters should fully cover the area of interest defined in the boundary file; one way to ensure this is to generate the boundary file from the rasters.

#### Date Range

The date range parameter filters GEDI data by acquisition time. It must be formatted according to NASA's Common Metadata Repository (CMR) conventions. Valid formats include:
- A single date with a leading comma for start date (e.g., ",2020-12-31" for all data before December 31, 2020)
- A single date with a trailing comma for end date (e.g., "2019-04-01," for all data after April 1, 2019)
- Two dates separated by a comma (e.g., "2019-04-01,2020-12-31" for all data between those dates)
Dates should be in YYYY-MM-DD format.

### Output Management

After submitting a batch of job with the same job tag, the MAAP platform automatically creates a timestamped output directory containing:
- A log file with detailed processing information
- A list of all job IDs for reference
- Status summaries for succeeded and failed jobs

All job outputs are stored in the MAAP workspace S3 bucket under a directory structure that includes your username and the specified job tag. For example, if your username is "jsmith" and you used the tag "biomass_2020", the outputs would be in "s3://maap-ops-workspace/jsmith/dps_output/nmbim_biomass_index/main/biomass_2020". You can use AWS CLI tools to locate and download all GeoPackage files within this directory:

```bash
# Get a list of all the output files in the bucket
aws s3 ls s3://maap-ops-workspace/jsmith/dps_output/nmbim_biomass_index/main/biomass_2020/ --recursive | grep '.gpkg.bz2$'

# Copy the files to the local directory
aws s3 cp s3://maap-ops-workspace/jsmith/biomass_2020/ . --recursive --exclude "*" --include "*.gpkg.bz2"
```
### Post-processing
The output of a single model run through run_on_maap.py is a hierarchical directory structure containing output GeoPackage files. The method chosen to process these files will depend on the end goal, but one workflow is outlined below. Future work could automate this process.

1. Download compressed GeoPackages from S3 to your local machine:
```bash
# Get a list of all the output files in the bucket
aws s3 ls s3://maap-ops-workspace/jsmith/dps_output/nmbim_biomass_index/main/biomass_2020/ --recursive | grep '.gpkg.bz2$'

# Copy the files to the local directory
aws s3 cp s3://maap-ops-workspace/jsmith/biomass_2020/ . --recursive --exclude "*" --include "*.gpkg.bz2"
```

2. Decompress the GeoPackages. You can use GNU Parallel for faster processing:
```bash
# Using GNU Parallel
find . -name "*.gpkg.bz2" | parallel bunzip2

# Or using Python with Dask for parallel processing
python -c '
import dask.bag as db
from pathlib import Path
import subprocess

files = list(Path(".").rglob("*.gpkg.bz2"))
def decompress(f):
    subprocess.run(["bunzip2", str(f)])
    
db.from_sequence(files).map(decompress).compute()
'
```

3. Consolidate the decompressed GeoPackages into a single file using GDAL's ogrmerge.py or a Python script.

4. Load the consolidated data into a spatial database using ogr2ogr. Set a high value for the -gt option to improve performance.
