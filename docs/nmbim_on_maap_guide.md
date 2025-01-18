# User Guide for NMBIM Algorithm on MAAP 
This guide provides step-by-step instructions to run the NMBIM algorithm on MAAP, then gives more detailed information about the deployment on MAAP. 

## Quick Start
Here are minimal instructions to run the NMBIM algorithm on MAAP for a given spatial and temporal query.

1. Create parameter rasters (HSE and k_allom) according to your chosen parameterization method
   - Format: GeoTIFF in EPSG:4326 projection
   - Names must be: `hse.tif` and `k_allom.tif`
   - Ensure complete coverage of your area of interest

2. Create boundary layer for the region to be processed
   - Format: GeoPackage (.gpkg) or Shapefile (.shp) in EPSG:4326
   - Best practice: Generate from rasters to ensure complete parameterization
   - Must only include areas with valid HSE and k_allom values
   - Multiple polygons supported but must not overlap

p3. Get or create a configuration file
   - Option A: Use an existing config file (see config/config.yaml in the ni-meister-gedi-biomass repository for a default)
   - Option B: Create new config file:
     - Name as `config.yaml` or `config.yml`
     - Define filters section (spatial, temporal, quality)
     - Define processing pipeline steps
     - Can leave temporal/spatial parameters blank if using MAAP job submission API

4. Clone source repository into MAAP ADE from the MAAP GitLab or Ian Grant's GitHub account (the two are identical as of the writing of this guide).
   - Option A: From MAAP GitLab:
     ```bash
     git clone https://gitlab.maap-project.org/iangrant/ni-meister-gedi-biomass.git
     ```
   - Option B: From Ian Grant's GitHub:
     ```bash
     git clone https://github.com/i-c-grant/ni-meister-gedi-biomass.git
     ```

5. Upload files to MAAP workspace
   - Navigate my-private-bucket in the MAAP ADE graphical file browser
   - Upload hse.tif, k_allom.tif, boundary.gpkg, and config.yaml using the interface

6. Run processing script
   ```bash
   # Navigate to the cloned repository
   cd ni-meister-gedi-biomass
   
   # Run the script
   python run_on_maap.py \
     --username {your_username} \
     --tag {unique_processing_id} \
     --config s3://maap-ops-workspace/{username}/my-private-bucket/config.yaml \
     --hse s3://maap-ops-workspace/{username}/my-private-bucket/hse.tif \
     --k_allom s3://maap-ops-workspace/{username}/my-private-bucket/k_allom.tif \
     --algo_id nmbim_biomass_index \
     --algo_version main
   ```

This script will figure out what GEDI files are necessary to cover the query and submit the necessary jobs to the MAAP DPS.

7. Monitor job progress
The script will display a progress bar showing:
     - Total completed jobs
     - Current status counts (Succeeded, Failed, Running)
     - Time of last status update (updates occur infrequently for very large job batches)

Wait until all or most jobs are complete. If jobs are hung in 'Offline' status for a long time, you can safely cancel the run with Ctrl-C Ctrl-C. The completed jobs will still be available for download.

8. Get temporary MAAP credentials
In order to download the outputs from the MAAP s3 bucket, it is necessary to obtain temporary credentials to access the bucket.

   ```python
   from maap.maap import MAAP
   maap = MAAP(maap_host='api.maap-project.org')
   credentials = maap.aws.workspace_bucket_credentials()
   ```

9. Download and process results

The temporary credentials can be used with AWS CLI tools locally to download the results:

   ```bash
   # List output files
   aws s3 ls s3://maap-ops-workspace/{username}/dps_output/nmbim_biomass_index/main/{unique_processing_id}/ --recursive | grep '.gpkg.bz2$'

   # Download compressed GeoPackages
   aws s3 cp s3://maap-ops-workspace/{username}/{unique_processing_id}/ . \
     --recursive --exclude "*" --include "*.gpkg.bz2"

   # Decompress files
   bunzip2 *.gpkg.bz2
   ```

## Deployment Overview

The NMBIM algorithm on NASA's MAAP platform processes GEDI waveform data to produce biomass estimates for user-defined geographic regions and time periods. Rather than requiring users to manually identify and download GEDI data files, the algorithm interfaces with NASA's Common Metadata Repository (CMR) to automatically locate all relevant GEDI granules that intersect the specified spatial and temporal bounds. The algorithm handles downloading and processing of Level 1B, 2A, and 4A GEDI data products, applying configurable quality filters and processing steps to each waveform. This automation significantly simplifies the workflow - users only need to provide their area of interest (as a boundary file), desired time range, and processing parameters through a configuration file.

The algorithm is deployed on the MAAP platform as `nmbim_biomass_index:main`. This algorithm runs the NMBIM model on a single set of corresponding L1B, L2A, and L4A files (three total files). `run_on_maap.py` is a script that automates the process of submitting MAAP jobs for this algorithm in order to cover a given spatial extent and duration. Direct calls to the `nmbim_biomass_index:main` algorithm (i.e. by submitting through the MAAP ADE graphical interface via Jobs -> Submit Jobs) are typically not practical since processing a given spatial area requires multiple algorithm calls, and GEDI granule names may not be known in advance. Instead, the `run_on_maap.py` script orchestrates the necessary sequence of calls to `nmbim_biomass_index:main` to process the desired area. However, the following section describes the operation of `nmbim_biomass_index:main` in detail in order to support future development and debugging.

## MAAP Algorithm Details

The NMBIM algorithm is registered under the name `nmbim_biomass_index:main` on the MAAP platform. 'main' corresponds to the branch of the source repository that the algorithm was deployed from; other versions of the algorithm may also be registered, but 'main' should be used unless development is ongoing on a different branch.

Each run of the algorithm processes one set of corresponding L1B, L2A, and L4A files, which must be specified at runtime.

### Inputs

#### Boundary

The boundary input defines the geographic area of interest for processing. It must be provided as either a geopackage (.gpkg) or shapefile (.shp) in the EPSG:4326 coordinate system. The boundary should encompass only areas where both HSE and k_allom parameters have valid values. While multiple polygons are supported, they should not overlap.

#### Configuration

The algorithm accepts a configuration file that specifies the filters and processing pipeline to be run on the GEDI waveforms within the query window. This file is in YAML format and must be named config.yaml or config.yml.

The first section of the configuration file, `filters`, references spatial, temporal, and quality filters defined in filters.py. Each filter configuration within this section also specifies any filter-specific parameters.

Note: Since the MAAP deployment of NMBIM algorithm accepts temporal and spatial queries as arguments through the MAAP job submission API, it is permitted to leave the parameters for temporal and spatial filters blank in the configuration file. The parameters supplied through the job submission API will be applied to the filters as necessary.

The second section of the configuration file specifies the processing pipeline to be run on the GEDI waveforms. Each entry in this section specifies one step of the pipeline; these steps are applied sequentially to each waveform in the model run. The `alg_fun` value within each processing step indicates the name of the Python function to be applied for that step, `input_map` determines what data from each Waveform object is supplied as input to that function, and `output_path` gives the path within each Waveform object to which the outputs will be written.

#### HSE and k_allom

The height scaling exponent and allometric coefficient are specified using GeoTIFF raster files in EPSG:4326 coordinate system. The files must be named hse.tif and k_allom.tif, respectively (this convention facilitates worker-side processing of the rasters). The rasters should fully cover the area of interest defined in the boundary file; one way to ensure this is to generate the boundary file from the rasters.

#### GEDI Data Products

The algorithm requires GEDI Level 1B, 2A, and 4A data products for processing. For each granule to be processed, provide the full granule name without any file path or extension. The algorithm will automatically download the necessary files from the LP DAAC or the ORNL DAAC as necessary.

#### Date Range

The date range parameter filters GEDI data by acquisition time. It must be formatted according to NASA's Common Metadata Repository (CMR) conventions. Valid formats include:
- A single date with a leading comma for start date (e.g., ",2020-12-31" for all data before December 31, 2020)
- A single date with a trailing comma for end date (e.g., "2019-04-01," for all data after April 1, 2019)
- Two dates separated by a comma (e.g., "2019-04-01,2020-12-31" for all data between those dates)
Dates should be in YYYY-MM-DD format.

### Output

The algorithm outputs a single compressed GeoPackage with point features corresponding to each processed footprint and output attributes. The attributes written to the GeoPackage are currently determined by the write_waveforms function in app_utils.py; in the future, it would be better to make the output attributes configurable in the configuration file. The GeoPackage path will end with the .bz2 extension because the algorithm compresses the output using bzip2. This feature makes it easier to transfer the output from very large model runs out of the MAAP S3 bucket. The output GeoPackage can be decompressed with the bunzip2 command.

## Running with run_on_maap.py

The run_on_maap.py script provides a high-level interface for running the NMBIM algorithm on MAAP. Instead of requiring you to specify individual GEDI granule names, you provide a spatial query (as a boundary file) and a temporal query (as a date range). The script then automatically queries NASA's Common Metadata Repository (CMR) to find all GEDI granules that intersect your area and time period of interest. For each matching L1B granule found, it identifies the corresponding L2A and L4A granules, submits separate processing jobs for each matched set, and monitors their progress. This automation makes it easy to process large areas that may span multiple GEDI orbits and time periods, as it eliminates the need to manually identify and download individual granules.

### Basic Usage

In the MAAP workspace, run `python run_on_maap.py --help` to see usage hints.

The script requires several mandatory arguments:
- username: Your MAAP username
- tag: A unique identifier for this processing run
- config: Path to the configuration YAML file (must be accessible to MAAP workers)
- hse: Path to the height scaling exponent raster
- k_allom: Path to the allometric coefficient raster
- algo_id: The algorithm ID ("nmbim_biomass_index")
- algo_version: The algorithm version (typically "main")

Optional arguments allow you to:
- Restrict processing to a specific geographic boundary
- Filter by date range
- Limit the number of jobs submitted
- Adjust the job status checking interval

Note: all file arguments (config, hse, and k_allom) should be passed as s3 paths, not paths within the locally mounted MAAP filesystem. Passing local paths will result in a high load on the MAAP ADE cluster, as each transfer of a file argument to a worker will be routed through the MAAP ADE in order to resolve the local path into the s3 path. Accordingly, the configuration file and HSE and k_allom rasters should be stored in the `my-private-bucket` or `my-public-bucket` folders of your MAAP environment.

### Job Management

The script submits jobs in batches to avoid overwhelming the MAAP API. It maintains a progress bar showing:
- Total number of completed jobs
- Current status counts (Succeeded, Failed, Running, etc)
- Time of last status update

You can safely interrupt processing with Ctrl+C; the script will ask for confirmation and then cleanly cancel any pending jobs if you press Ctrl+C again. Since the script is conservative about making MAAP API calls, you can expect several minutes between progress bar updates.

Note: The status 'Offline', which falls under 'Other' in the run_on_maap.py progress bar, indicates that the AWS spot instance that then MAAP worker has been reclaimed by AWS. Offline jobs may or may not resume. If a job has been offline for a long time and it is the only job left, it is often best to just stop the run with Ctrl+C Ctrl+C.

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
