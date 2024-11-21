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

def extract_s3_url_from_granule(granule: Granule) -> str:
    urls = granule['Granule']['OnlineAccessURLs']['OnlineAccessURL']
    s3_urls = [url['URL'] for url in urls if url['URL'].startswith("s3")]
    
    if len(s3_urls) > 1:
        warnings.warn(f"Multiple S3 URLs found in granule: {s3_urls}")

    s3_url = s3_urls[0]

    return s3_url

def granules_match(l1b: Granule, l2a: Granule):
    l1b_name = l1b['Granule']['GranuleUR']
    l2a_name = l2a['Granule']['GranuleUR']
    l1b_base = l1b_name.split("_")[2:5]
    l2a_base = l2a_name.split("_")[2:5]
    return l1b_base == l2a_base

def l4a_matches(l1b: Granule, l4a: Granule):
    l1b_name = l1b['Granule']['GranuleUR']
    l1b_base = l1b_name.split("_")[2:5]

    l4a_name = l4a['Granule']['GranuleUR']
    # L4A granule names have a product descriptor at the beginning,
    # so we split it off before comparing
    l4a_base = l4a_name.split(".")[1].split("_")[2:5]
    return l1b_base == l4a_base

def job_status_for(job_id: str) -> str:
    return maap.getJobStatus(job_id)

def job_result_for(job_id: str) -> str:
    return maap.getJobResult(job_id)[0]

def to_job_output_dir(job_result_url: str, username: str) -> str:
    return (f"/projects/my-private-bucket/"
            f"{job_result_url.split(f'/{username}/')[1]}")

def s3_url_to_local_path(s3_url: str) -> str:
    """
    Converts MAAP S3 URLs to local filesystem paths.
    
    Args:
        s3_url: S3 URL starting with s3://maap-ops-workspace/
        
    Returns:
        Local filesystem path
    """
    if not s3_url.startswith("s3://maap-ops-workspace/"):
        raise ValueError("URL must start with s3://maap-ops-workspace/")
        
    # Remove the s3://maap-ops-workspace/ prefix
    path = s3_url.replace("s3://maap-ops-workspace/", "")
    
    # Extract username and determine bucket based on path structure
    if path.startswith("shared/"):
        _, username, *rest = path.split("/")
        bucket = "my-public-bucket"
        path = "/".join(rest)
    else:
        username, *rest = path.split("/")
        bucket = "my-private-bucket"
        path = "/".join(rest)
        
    return f"/projects/{bucket}/{path}"


def log_and_print(message: str):
    logging.info(message)
    click.echo(message)

def update_job_states(job_states: Dict[str, str],
                      final_states: List[str],
                      batch_size: int,
                      delay: int) -> Dict[str, str]:
    """Update the job states dictionary in place.

    Updating occurs in batches, with a delay in seconds between batches.

    Return the number of jobs updated to final states.
    """
    batch_count = 0
    n_updated_to_final = 0
    for job_id, state in job_states.items():
        if state not in final_states:
            new_state: str = job_status_for(job_id)
            job_states[job_id] = new_state
            if new_state in final_states:
                n_updated_to_final += 1
            batch_count += 1
        # Sleep after each batch to avoid overwhelming the API
        if batch_count == batch_size:
            time.sleep(delay)
            batch_count = 0

    return n_updated_to_final

@click.command()
@click.option("--username", "-u", type=str, required=True, help="MAAP username.")
@click.option("--tag", "-t", type=str, required=True, help="Job tag.")
@click.option("--boundary", "-b", type=str,
              help=("Path or URL to a shapefile or GeoPackage containing "
                    "a boundary polygon. Note: should be accessible "
                    "to MAAP DPS workers."))
@click.option("--date_range", "-d", type=str,
              help=("Date range for granule search. "
                    "See <https://cmr.earthdata.nasa.gov/search/site/"
                    "docs/search/api.html#temporal-range-searches> "
                    "for valid formats."))
@click.option("--config", "-c", type=str, required=True,
              help="Path to the configuration YAML file. Filename must be 'config.yaml' or 'config.yml'.")
@click.option("--hse", type=str, required=True,
              help="Path to HSE raster file.")
@click.option("--k_allom", type=str, required=True,
              help="Path to k_allom raster file.")
@click.option("--algo_id", "-a", type=str, required=True,
              help="Algorithm ID to run.")
@click.option("--algo_version", "-v", type=str, required=True,
              help="Algorithm version to run.")
@click.option("--job_limit", "-j", type=int,
              help="Limit the number of jobs submitted.")
@click.option("--check_interval", "-i", type=int, default=120,
              help="Time interval (in seconds) between job status checks.")
def main(username: str,
         tag: str,
         boundary: str,
         date_range: str,
         job_limit: int,
         check_interval: int,
         config: str,
         hse: str,
         k_allom: str,
         algo_id: str,
         algo_version: str):

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

    # Read and log full configuration
    config_path = s3_url_to_local_path(config)
    try:
        with open(config_path, 'r') as config_file:
            full_config = config_file.read()
    except Exception as e:
        log_and_print(f"Error reading config file from {config_path}: {str(e)}")
        raise

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

    l4a_id = maap.searchCollection(
        short_name="GEDI_L4A_AGB_Density_V2_1_2056",
        cmr_host="cmr.earthdata.nasa.gov",
        cloud_hosted="true"
    )[0]['concept-id']

    max_results = 10000
    search_kwargs = {'concept_id': [l1b_id, l2a_id, l4a_id],
                     'cmr_host': 'cmr.earthdata.nasa.gov',
                     'limit': max_results}

    if date_range:
        search_kwargs['temporal'] = date_range

    if boundary:
        # Get bounding box of boundary to restrict granule search
        boundary_path = s3_url_to_local_path(boundary)
        boundary_gdf: GeoDataFrame = gpd.read_file(boundary_path, driver='GPKG')
        boundary_bbox: tuple = boundary_gdf.total_bounds
        boundary_bbox_str: str = ','.join(map(str, boundary_bbox))
        search_kwargs['bounding_box'] = boundary_bbox_str

    log_and_print(f"Searching for granules.")
    click.echo("(This may take a few minutes.)")

    granules: List[Granule] = maap.searchGranule(**search_kwargs)

    log_and_print(f"Found {len(granules)} granules.")

    # match corresponding L1B and L2A granules
    l1b_granules = (
        [granule for granule in granules
         if granule['Granule']['Collection']['ShortName'] == 'GEDI01_B']
    )

    l2a_granules = (
        [granule for granule in granules
         if granule['Granule']['Collection']['ShortName'] == 'GEDI02_A']
    )

    l4a_granules = (
        [granule for granule in granules
         if granule['Granule']['Collection']['ShortName'] ==
         'GEDI_L4A_AGB_Density_V2_1_2056']
    )

    matched_granules: List[Dict[str, Granule]] = []

    for l1b_granule in l1b_granules:
        for l2a_granule in l2a_granules:
            if granules_match(l1b_granule, l2a_granule):
                l1b_id = l1b_granule['Granule']['GranuleUR']
                l2a_id = l2a_granule['Granule']['GranuleUR']
                
                # Find matching L4A granules
                matching_l4a = [l4a_granule for l4a_granule in l4a_granules 
                                if l4a_matches(l1b_granule, l4a_granule)]
                
                if len(matching_l4a) == 0:
                    log_and_print(f"Warning: No matching L4A granule "
                                  f"found for L1B: {l1b_id}")
                elif len(matching_l4a) > 1:
                    raise ValueError(f"Multiple matching L4A granules found "
                                     f"for L1B: {l1b_id}")
                else:
                    l4a_granule = matching_l4a[0]
                    matched_granules.append({
                        "l1b": l1b_granule,
                        "l2a": l2a_granule,
                        "l4a": l4a_granule
                    })
                    break 

    log_and_print(f"Found {len(matched_granules)} matching "
                  f"sets of granules.")

    # Submit jobs for each pair of granules
    if job_limit:
        n_jobs = min(len(matched_granules), job_limit)
    else:
        n_jobs = len(matched_granules)
    log_and_print(f"Submitting {n_jobs} "
                  f"jobs.")

    job_kwargs_list = []
    for matched in matched_granules:
        job_kwargs = {
            "identifier": tag,
            "algo_id": algo_id,
            "version": algo_version,
            "username": username,
            "queue": "maap-dps-worker-16gb",
            "L1B": extract_s3_url_from_granule(matched['l1b']),
            "L2A": extract_s3_url_from_granule(matched['l2a']),
            "L4A": extract_s3_url_from_granule(matched['l4a']),
            "config": config,  # Pass S3 URL directly
            "hse": hse,  # Pass S3 URL directly
            "k_allom": k_allom  # Pass S3 URL directly
        }

        if boundary:
            job_kwargs['boundary'] = boundary  # Pass S3 URL directly

        if date_range:
            job_kwargs['date_range'] = date_range

        job_kwargs_list.append(job_kwargs)

    jobs = []
    job_batch_counter = 0
    job_batch_size = 50
    job_submit_delay = 10
    for job_kwargs in job_kwargs_list[:job_limit]:
        job = maap.submitJob(**job_kwargs)
        jobs.append(job)
        job_batch_counter += 1

        if job_batch_counter == job_batch_size:
            time.sleep(job_submit_delay)
            job_batch_counter = 0

    print(f"Submitted {len(jobs)} jobs.")

    job_ids = [job.id for job in jobs]

    # Write job IDs to a file in case processing is interrupted
    job_ids_file = output_dir / "job_ids.txt"
    with open(job_ids_file, 'w') as f:
        for job_id in job_ids:
            f.write(f"{job_id}\n")
    log_and_print(f"Job IDs written to {job_ids_file}")

    # Give the jobs time to start
    click.echo("Waiting for jobs to start...")
    time.sleep(10)

    # Initialize job states
    final_states = ["Succeeded", "Failed", "Deleted"]

    job_states = {job_id: "" for job_id in job_ids}
    update_job_states(job_states, final_states, batch_size=50, delay=10)

    known_completed = len([state for state in job_states.values()
                           if state in final_states])

    while True:
        try:
            with tqdm(total=len(job_ids), desc="Jobs Completed", unit="job") as pbar:
                while any(state not in final_states for state in job_states.values()):

                    # Update the job states
                    n_new_completed: int = update_job_states(job_states,
                                                             final_states,
                                                             batch_size = 50,
                                                             delay = 10)

                    # Update the progress bar
                    pbar.update(n_new_completed)
                    last_updated = datetime.datetime.now()
                    known_completed += n_new_completed
                    
                    status_counts = {status: list(job_states.values()).count(status)
                                     for status in final_states + ["Accepted", "Running"]}
                    status_counts["Other"] = len(job_states) - sum(status_counts.values())
                    status_counts["Last updated"] = last_updated.strftime("%H:%M:%S")

                    pbar.set_postfix(status_counts, refresh=True)

                    if known_completed == len(job_ids):
                        break

                    time.sleep(check_interval)

        except KeyboardInterrupt:
            print("Are you sure you want to cancel the process?")
            print("Press Ctrl+C again to confirm, or wait to continue.")
            try:
                time.sleep(3)
                print("Continuing...")
            except KeyboardInterrupt:
                print("Model run aborted.")
                pending_jobs = [job_id for job_id, state in job_states.items()
                                if state not in final_states]
                click.echo(f"Cancelling {len(pending_jobs)} pending jobs.")
                for job_id in pending_jobs:
                    maap.cancelJob(job_id)
                break
        else:
            break

    # Process the results once all jobs are completed
    succeeded_job_ids = [job_id for job_id in job_ids
                         if job_status_for(job_id) == "Succeeded"]
    
    failed_job_ids = [job_id for job_id in job_ids
                      if job_status_for(job_id) == "Failed"]

    other_job_ids = [job_id for job_id in job_ids
                     if job_status_for(job_id)
                     not in ["Succeeded", "Failed"]]

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
