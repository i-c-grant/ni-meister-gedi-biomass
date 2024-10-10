import datetime
import logging
import os
import shutil
import time
import warnings
from pathlib import Path
from typing import Dict, List

import click
import geopandas as gpd
import pandas as pd
from tqdm import tqdm
from geopandas import GeoDataFrame
from maap.maap import MAAP
from maap.Result import Granule

maap = MAAP(maap_host='api.maap-project.org')

def granules_match(l1b: Granule, l2a: Granule):
    l1b_name = l1b['Granule']['GranuleUR']
    l2a_name = l2a['Granule']['GranuleUR']
    l1b_base = l1b_name.split("_")[2:5]
    l2a_base = l2a_name.split("_")[2:5]
    return l1b_base == l2a_base

def job_status_for(job_id: str) -> str:
    return maap.getJobStatus(job_id)

def job_result_for(job_id: str) -> str:
    return maap.getJobResult(job_id)[0]

def to_job_output_dir(job_result_url: str, username: str) -> str:
    return (f"/projects/my-private-bucket/"
            f"{job_result_url.split(f'/{username}/')[1]}")

def check_jobs_status(job_ids: list) -> dict:
    """Check the status of all jobs and return a count of each status."""
    status_counts = {"Succeeded": 0,
                     "Failed": 0,
                     "Accepted": 0,
                     "Running": 0,
                     "Deleted": 0,
                     "Other": 0}

    for job_id in job_ids:
        status = job_status_for(job_id)
        if status in status_counts:
            status_counts[status] += 1
        else:
            status_counts["Other"] += 1

    return status_counts

def log_and_print(message: str):
    logging.info(message)
    click.echo(message)

@click.command()
@click.option("username", "-u", type=str, required=True, help="MAAP username.")
@click.option("boundary", "-b", type=str,
              help=("Path or URL to a shapefile or GeoPackage containing "
                    "a boundary polygon. Note: should be accessible "
                    "to MAAP DPS workers."))
@click.option("date_range", "-d", type=str,
              help=("Date range for granule search. "
                    "See <https://cmr.earthdata.nasa.gov/search/site/"
                    "docs/search/api.html#temporal-range-searches> "
                    "for valid formats."))
@click.option("config", "-c", type=str, required=True,
              help="Path to the configuration YAML file. Filename must be 'config.yaml' or 'config.yml'.")
@click.option("job_limit", "-j", type=int,
              help="Limit the number of jobs submitted.")
@click.option("check_interval", "-i", type=int, default=120,
              help="Time interval (in seconds) between job status checks.")
def main(username: str,
         boundary: str,
         date_range: str,
         job_limit: int,
         check_interval: int,
         config: str):

    start_time = datetime.datetime.now()

    # Set up output directory
    output_dir = Path(f"run_output_"
                      f"{start_time.strftime('%Y%m%d_%H%M%S')}")
    os.makedirs(output_dir, exist_ok=False)

    # Set up log
    logging.basicConfig(filename=output_dir / "run.log",
                        level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    log_and_print(f"Starting new model run at MAAP at {start_time}.")
    log_and_print(f"Boundary: {boundary}")
    log_and_print(f"Date Range: {date_range}")

    # Log full configuration
    with open(config, 'r') as config_file:
        full_config = config_file.read()

    log_and_print(f"Configuration:\n{full_config}")

    l1b_id = maap.searchCollection(
            short_name="GEDI01_B",
            version="002",
            cmr_host="cmr.earthdata.nasa.gov",
            cloud_hosted="true"
        )[0]['concept-id']

    l2a_id = maap.searchCollection(
        short_name="GEDI02_A",
        version="002",
        cmr_host="cmr.earthdata.nasa.gov",
        cloud_hosted="true"
    )[0]['concept-id']

    max_results = 10000
    search_kwargs = {'concept_id': [l1b_id, l2a_id],
                     'cmr_host': 'cmr.earthdata.nasa.gov',
                     'limit': max_results}

    if date_range:
        search_kwargs['temporal'] = date_range

    if boundary:
        # Get bounding box of boundary to restrict granule search
        boundary_gdf: GeoDataFrame = gpd.read_file(boundary,driver='GPKG')
        boundary_bbox: tuple = boundary_gdf.total_bounds
        boundary_bbox_str: str = ','.join(map(str, boundary_bbox))
        search_kwargs['bounding_box'] = boundary_bbox_str

    log_and_print(f"Searching for granules.")
    click.echo("(This may take a few minutes.)")

    granules: List[Granule] = maap.searchGranule(**search_kwargs)

    log_and_print(f"Found {len(granules)} granules.")

    # pair corresponding L1B and L2A granules
    l1b_granules = (
        [granule for granule in granules
         if granule['Granule']['Collection']['ShortName'] == 'GEDI01_B']
    )

    l2a_granules = (
        [granule for granule in granules
         if granule['Granule']['Collection']['ShortName'] == 'GEDI02_A']
    )

    paired_granule_ids: List[Dict[str, str]] = []

    for l1b_granule in l1b_granules:
        for l2a_granule in l2a_granules:
            if granules_match(l1b_granule, l2a_granule):
                paired_granule_ids.append(
                    {"l1b": l1b_granule['Granule']['GranuleUR'],
                     "l2a": l2a_granule['Granule']['GranuleUR']})
                break
                
    log_and_print(f"Found {len(paired_granule_ids)} matching "
                  f"pairs of granules.")

    # Submit jobs for each pair of granules
    if job_limit:
        n_jobs = min(len(paired_granule_ids), job_limit)
    else:
        n_jobs = len(paired_granule_ids)
    log_and_print(f"Submitting {n_jobs} "
                  f"jobs.")

    job_kwargs_list = []
    for pair in paired_granule_ids:
        job_kwargs = {
            "identifier": "nmbim_gedi_processing",
            "algo_id": "nmbim_biomass_index",
            "version": "main",
            "username": username,
            "queue": "maap-dps-worker-8gb",
            "L1B": pair['l1b'],
            "L2A": pair['l2a'],
            "config": config
        }

        if boundary:
            job_kwargs['boundary'] = boundary

        if date_range:
            job_kwargs['date_range'] = date_range

        job_kwargs_list.append(job_kwargs)

    jobs = []
    for job_kwargs in job_kwargs_list[:job_limit]:
        job = maap.submitJob(**job_kwargs)
        jobs.append(job)

    print(f"Submitted {len(jobs)} jobs.")

    job_ids = [job.id for job in jobs]

    # Give the jobs time to start
    click.echo("Waiting for jobs to start...")
    time.sleep(10)

    # can refactor this to use a dict tracking the last known status
    # of each job, only check the ones that are not in a final state
    
    with tqdm(total=len(job_ids), desc="Jobs Completed", unit="job") as pbar:
        completed_jobs = 0
        while completed_jobs < len(job_ids):
            status_counts = check_jobs_status(job_ids)
            new_completed = (status_counts['Succeeded'] + 
                             status_counts['Failed'] + 
                             status_counts['Deleted']) - completed_jobs

            if new_completed > 0:
                pbar.update(new_completed)
                completed_jobs += new_completed

            pbar.set_postfix({
                "Queued": status_counts['Accepted'],
                "Running": status_counts['Running'],
                "Succeeded": status_counts['Succeeded'],
                "Failed": status_counts['Failed'],
                "Deleted": status_counts['Deleted']
            }, refresh=True)

            if completed_jobs == len(job_ids):
                break

            time.sleep(check_interval)    
    # Process the results once all jobs are completed
    succeeded_job_ids = [job_id for job_id in job_ids
                         if job_status_for(job_id) == "Succeeded"]
    
    failed_job_ids = [job_id for job_id in job_ids
                      if job_status_for(job_id) == "Failed"]

    other_job_ids = [job_id for job_id in job_ids
                     if job_status_for(job_id)
                     not in ["Succeeded", "Failed"]]

    click.echo(f"Processing results for {len(succeeded_job_ids)} "
               f"succeeded jobs.")

    click.echo(f"Gathering GeoPackage paths from succeeded jobs.")
    gpkg_paths = []
    for job_id in tqdm(succeeded_job_ids):
        job_result_url = job_result_for(job_id)
        job_output_dir = to_job_output_dir(job_result_url, username)
        # Find .gpkg file in the output dir
        gpkg_file = [f for f in os.listdir(job_output_dir)
                     if f.endswith('.gpkg')]
        if len(gpkg_file) > 1:
            warnings.warn(f"Multiple .gpkg files found in "
                          f"{job_output_dir}.")
        if len(gpkg_file) == 0:
            warnings.warn(f"No .gpkg files found in "
                          f"{job_output_dir}.")
        if gpkg_file:
            gpkg_paths.append(os.path.join(job_output_dir, gpkg_file[0]))

    # Copy all GeoPackages to the output directory
    click.echo(f"Copying {len(gpkg_paths)} GeoPackages to {output_dir}.")
    for gpkg_path in tqdm(gpkg_paths):
        shutil.copy(gpkg_path, output_dir)

    # Log the succeeded and failed job IDs
    logging.info(f"{len(succeeded_job_ids)} jobs succeeded.")
    logging.info(f"Succeeded job IDs: {succeeded_job_ids}\n")
    logging.info(f"{len(failed_job_ids)} jobs failed.")
    logging.info(f"Failed job IDs: {failed_job_ids}\n")
    logging.info(f"{len(other_job_ids)} jobs in other states.")
    logging.info(f"Other job IDs: {other_job_ids}\n")

    end_time = datetime.datetime.now()

    log_and_print(f"Model run completed at {end_time}.")
    
if __name__ == "__main__":
    main()
