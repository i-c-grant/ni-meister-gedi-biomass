import os
import time
from typing import Dict, List
from urllib.parse import urlparse

import click
import geopandas as gpd
import pandas as pd
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

@click.command()
@click.option("username", "-u", type=str, required=True)
@click.option("boundary", "-b", type=click.Path(exists=True),
              help=("Path to a shapefile or GeoPackage containing "
                    "a boundary polygon. Note: should be accessible "
                    "to MAAP DPS workers."))
@click.option("date_range", "-d", type=str,
              help=("Date range for granule search. "
                    "See <https://cmr.earthdata.nasa.gov/search/site/"
                    "docs/search/api.html#temporal-range-searches> "
                    "for valid formats."))
@click.option("job_limit", "-j", type=int,
              help="Limit the number of jobs submitted.")
@click.option("check_interval", "-i", type=int, default=120,
              help="Time interval (in seconds) between job status checks.")
def main(username: str, boundary: str, date_range: str, job_limit: int, check_interval: int):
    l1b_id = maap.searchCollection(
            short_name="GEDI01_B",
            version="002",
            cmr_host="cmr.earthdata.nasa.gov",
            cloud_hosted=True
        )[0]['concept-id']

    l2a_id = maap.searchCollection(
        short_name="GEDI02_A",
        version="002",
        cmr_host="cmr.earthdata.nasa.gov",
        cloud_hosted=True
    )[0]['concept-id']

    kwargs = {'concept_id': [l1b_id, l2a_id]}

    if date_range:
        kwargs['temporal'] = date_range

    if boundary:
        # Get bounding box of boundary to restrict granule search
        boundary_gdf: GeoDataFrame = gpd.read_file(boundary)
        boundary_bbox: tuple = boundary_gdf.total_bounds
        boundary_bbox_str: str = ','.join(map(str, boundary_bbox))
        kwargs['bounding_box'] = boundary_bbox_str

    granules: List[Granule] = maap.searchGranule(**kwargs)

    # pair corresponding L1B and L2A granules
    l1b_granules = (
        [granule for granule in granules
         if granule['collection']['short_name'] == 'GEDI01_B']
    )

    l2a_granules = (
        [granule for granule in granules
         if granule['collection']['short_name'] == 'GEDI02_A']
    )

    paired_granule_ids: List[Dict[str, str]] = []

    for l1b_granule in l1b_granules:
        for l2a_granule in l2a_granules:
            if granules_match(l1b_granule, l2a_granule):
                paired_granule_ids.append({"l1b": l1b_granule['concept-id'],
                                           "l2a": l2a_granule['concept-id']})
                break
                
    click.echo(f"Found {len(paired_granule_ids)} matching "
               f"pairs of granules.")

    # Submit jobs for each pair of granules
    jobs = []
    for pair in paired_granule_ids:
        job_kwargs = {
            "identifier": "nmbim_gedi_processing",
            "algo_id": "gedi_nmbim",
            "username": username,
            "queue": "maap-dps-worker-8gb",
            "l1b_id": pair['l1b'],
            "l2a_id": pair['l2a'],
        }

        if boundary:
            job_kwargs['boundary'] = boundary

        if date_range:
            job_kwargs['date_range'] = date_range

        job = maap.submitJob(**job_kwargs)
        jobs.append(job)
        if job_limit and len(jobs) >= job_limit:
            break

    print(f"Submitted {len(jobs)} jobs.")

    job_ids = [job.id for job in jobs]
    
    while True:
        status_counts = check_jobs_status(job_ids)
        total_jobs = len(job_ids)
        
        print(f"Job Status Update: {status_counts['Succeeded']} Succeeded, "
              f"{status_counts['Failed']} Failed, "
              f"{status_counts['Running']} Running, "
              f"{status_counts['Deleted']} Deleted. "
              f"Total jobs: {total_jobs}.")
        
        # If all jobs are done (Succeeded or Failed), exit loop
        total_completed = sum(status_counts[status]
                              for status in status_counts
                              if status != 'Running')

        if total_completed == total_jobs:
            break

        time.sleep(check_interval)
    
    # Process the results once all jobs are completed
    gpkg_paths = []
    for job_id in job_ids:
        if job_status_for(job_id) == "Succeeded":
            job_result_url = job_result_for(job_id)
            job_output_dir = to_job_output_dir(job_result_url, username)
            # Find .gpkg file in the output dir
            gpkg_file = [f for f in os.listdir(job_output_dir)
                         if f.endswith('.gpkg')]
            if gpkg_file:
                gpkg_paths.append(os.path.join(job_output_dir, gpkg_file[0]))

    # Combine all .gpkg files into one
    combined_gdf = gpd.GeoDataFrame(pd.concat([gpd.read_file(path)
                                               for path in gpkg_paths],
                                              ignore_index=True))
    combined_gdf.to_file("run_output.gpkg", driver="GPKG")
    print("All jobs completed, combined GeoPackage saved as "
          "'run_output.gpkg'.")

if __name__ == "__main__":
    main()
