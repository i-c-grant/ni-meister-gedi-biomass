# User Guide for NMBIM Algorithm on MAAP 
This guide provides step-by-step instructions to run the NMBIM algorithm on MAAP, then gives more detailed information about the deployment on MAAP. 

## Quick Start
Here are minimal instructions to run the NMBIM algorithm on MAAP for a given spatial and temporal query.

### 1. Create Parameter Rasters 
Create height scaling exponent (HSE) and allometric coefficient (k_allom) rasters according to your chosen parameterization method:
   - Format: GeoTIFF in EPSG:4326 projection
   - Names must end in `hse.tif` and `k_allom.tif`
   - Ensure complete coverage of your area of interest

### 2. Define Processing Boundary
Create a boundary layer for the region to be processed:
   - Format: GeoPackage (.gpkg) or Shapefile (.shp) in EPSG:4326
   - Best practice: Generate from rasters to ensure complete parameterization
   - Must only include areas with valid HSE and k_allom values
   - Multiple polygons supported but must not overlap

### 3. Configure Processing Pipeline 
Get or create a configuration file:
   - Option A: Use an existing config file (see config/config.yaml in the ni-meister-gedi-biomass repository for a default)
   - Option B: Create new config file:
     - Name as `config.yaml` or `config.yml`
     - Define filters section (spatial, temporal, quality)
     - Define processing pipeline steps
     - Can leave temporal/spatial parameters blank if using MAAP job submission API

### 4. Clone Source Repository
Clone the processing code into your MAAP ADE environment:
     ```bash
     git clone https://gitlab.maap-project.org/iangrant/ni-meister-gedi-biomass.git
     ```

### 5. Upload Input Files 
Transfer required files to your MAAP workspace bucket:
   - Navigate my-private-bucket in the MAAP ADE graphical file browser
   - Upload hse.tif, k_allom.tif, boundary.gpkg, and config.yaml using the interface
   - It's easiest to isolate these in an "inputs" folder, with a subfolder for a particular model run


### 6. Start Processing Jobs 
Execute the main processing script:
   - Invoke run_on_maap.py in a MAAP terminal or notebook
   - Jobs will be identified, submitted, and monitored
   - Once complete, the scripts creates a local output directory (`run_output_<YYYYMMDD_HHMMSS>`) containing `run.log` and a copy of your config file
   ```bash
   # Navigate to the cloned repository
   cd ni-meister-gedi-biomass

   # For convenience, set the input directory (use s3 path, not local path)
   INPUT_DIR="s3://maap-ops-workspace/iangrant94/inputs/conus_5-9"

   # Run the script
	python run_on_maap.py \
       -u iangrant94 \
       -t conus_bifurcated_2019 \
       -d "2019-01-01T10:00:00Z,2020-01-01T00:00:00Z" \
       -b ${INPUT_DIR}/conus.gpkg \
       -c ${INPUT_DIR}/config.yaml \
       --hse ${INPUT_DIR}/conus_region_bifurcation_hse.tif \
       --k_allom ${INPUT_DIR}/conus_region_bifurcation_k_allom.tif \
       -a nmbim_biomass_index \
       -v main \
       -j 3000
   ```

This script will figure out what GEDI files are necessary to cover the query and submit the necessary jobs to the MAAP DPS.

### 7. Monitor Job Progress 
Track processing status through the MAAP interface:
The script will display a progress bar showing:
     - Total completed jobs
     - Current status counts (Succeeded, Failed, Running)
     - Time of last status update (updates occur infrequently for very large job batches)

Wait until all or most jobs are complete. Press Ctrl-C once to suspend monitoring. You can then choose to resume, resubmit failed jobs, or exit. Unless you've run the command with the "--no-redo" option, you'll also get an option to resubmit failed jobs at the end of the run.

### 8. Retrieve Results 
Download processed outputs from MAAP:
   
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
   ```

### 9. Post-Process Results 
Prepare downloaded data for analysis:

The result of the download will be a directory structure under your 'output-dir' that mirrors the output structure on MAAP--outputs will be organized hierarchically by date and time. The files you're interested in are the output GeoPackages, which are compressed by default to ease huge downloads. To decompress them, use an option like the following:

```
find . -name "*.gpkg.bz2" | bunzip2
```

Or, if there are thousands of files and you're on a powerful computer with GNU parallel, you could do the following. Python based-solutions like dask or futures also work, of course.

```
find . -name "*.gpkg.bz2" | parallel bunzip2
```

Further processing is up to you. It may be advantageous to combine all the output GeoPackages one (typically huge) GeoPackage for visualization:

```
ogrmerge.py -progress -single -o <output path for combined GPKG> $(find <dir with results downloaded from MAAP> -name "*.gpkg")
```

However, it may also be more efficient to do some processing tasks without first combining the results, as this allows parallelization over the output files.

## Detailed description of run_on_maap.py options

The script accepts these required arguments:

| Argument | Description |
|----------|-------------|
| `-u/--username` | MAAP username for job tracking |
| `-t/--tag` | Unique identifier for the processing run |
| `-c/--config` | Path to YAML config file with filters/pipeline |
| `--hse` | Path to height scaling exponent GeoTIFF |
| `--k_allom` | Path to allometric coefficient GeoTIFF |
| `-a/--algo_id` | MAAP algorithm ID to execute |
| `-v/--algo_version` | Version of the algorithm to run |

Optional arguments:

| Argument | Description |
|----------|-------------|
| `-b/--boundary` | GeoPackage/Shapefile boundary (EPSG:4326) |
| `-d/--date_range` | CMR-formatted temporal filter |
| `-j/--job_limit` | Maximum jobs to submit |
| `-r/--redo-of` | Tag of previous run to redo |
| `--force-redo` | Bypass same-tag validation checks |
| `--no-resubmit` | Disable automatic failed job resubmission |

### Boundary Requirements
- Must be in EPSG:4326 (WGS 84) coordinate system
- Polygons should exactly match areas with valid HSE/k_allom values
- Non-overlapping multipolygons supported
- Recommended to generate from parameter rasters

### Configuration File
- **Format**: YAML (.yaml/.yml)
- **Structure**:
  - `filters`: Dict of quality filters from filters.py
  - `pipeline`: Ordered processing steps with:
    - `alg_fun`: Processing function name
    - `input_map`: Waveform data paths for inputs  
    - `output_path`: Waveform path to store results
- Temporal/spatial filters can be left blank when using CLI args

### Parameter Rasters (HSE/k_allom)
- **Format**: GeoTIFF in EPSG:4326
- **Coverage**: Must fully contain boundary
- **Naming**: Arbitrary filenames (no longer required to end with _hse/_k_allom)

### Date Range Formatting
Uses NASA CMR temporal syntax:
- Start and end dates in UTC: `YYYY-MM-DDThh:mm:ssZ`
- Open ranges supported: 
  - `,END_DATE` for data before END_DATE
  - `START_DATE,` for data after START_DATE
  - `START_DATE,END_DATE` for bounded range
