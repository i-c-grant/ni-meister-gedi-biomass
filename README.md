# NMBIM GEDI Processing

This repository contains a Python framework for processing GEDI (Global Ecosystem Dynamics Investigation) data using the MAAP (Multi-Mission Algorithm and Analysis Platform) API. It was developed for applying the Ni-Meister Biomass Index model, but the framework can be adapted for other processing tasks.

## Overview

The main script `run_on_maap.py` performs the following tasks:

1. Searches for GEDI L1B and L2A granules in the NASA CMR based on specified criteria (date range, boundary).
2. Pairs corresponding L1B and L2A granules.
3. Submits processing jobs to MAAP for each pair of granules.
4. Monitors job progress and handles user interruptions.
5. Collects and organizes results from successful jobs.

## Prerequisites

- Python 3.6+
- MAAP API access
- Required Python packages (install via `pip install -r requirements.txt`):
  - click
  - tqdm
  - geopandas
  - pandas
  - maap-py

## Usage

Run the script from the command line in a MAAP workspace with the following options:

```bash
python run_on_maap.py -u <username> -c <config_file> [-b <boundary_file>] [-d <date_range>] [-j <job_limit>] [-i <check_interval>]
```

Options:
- `-u, --username`: MAAP username (required)
- `-c, --config`: Path to the configuration YAML file (required)
- `-b, --boundary`: Path or URL to a shapefile or GeoPackage containing a boundary polygon
- `-d, --date_range`: Date range for granule search
- `-j, --job_limit`: Limit the number of jobs submitted
- `-i, --check_interval`: Time interval (in seconds) between job status checks 

## Output

The script creates an output directory containing:
- Log file (`run.log`)
- GeoPackage files from successful jobs
- A zip archive of the output directory