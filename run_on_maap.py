"""
Script for running GEDI waveform processing jobs on MAAP.

This script handles the end-to-end process of:
1. Searching for matching GEDI L1B, L2A, and L4A granules
2. Submitting processing jobs to MAAP DPS
3. Monitoring job status
4. Logging results

The script can filter granules by:
- Date range
- Geographic boundary
- Quality filters (configured in config.yaml file)

It provides progress monitoring and logging of:
- Job submission status
- Job completion status
- Success/failure counts
- Processing duration

To cancel processing early, press Ctrl-C twice (first time asks for confirmation).

Usage:
    python run_on_maap.py --username <maap_username> --tag <job_tag>
        --config <config_path> --hse <hse_path> --k_allom <k_allom_path>
        --algo_id <algorithm_id> --algo_version <version>
        [--boundary <boundary_path>] [--date_range <date_range>]
        [--job_limit <max_jobs>] [--check_interval <seconds>]
"""

import datetime
import logging
import os
import time
import warnings
from pathlib import Path
from typing import Dict, List, Set

import boto3
import click
import geopandas as gpd
from tqdm import tqdm
from geopandas import GeoDataFrame
from maap.Result import Granule

from dataclasses import dataclass

from maap import MAAP

maap = MAAP(maap_host="api.maap-project.org")


@dataclass
class RunConfig:
    """Container for all runtime configuration parameters"""
    username: str
    tag: str
    algo_id: str
    algo_version: str
    model_config: str
    hse: str
    k_allom: str
    boundary: str = None
    date_range: str = None
    job_limit: int = None
    check_interval: int = 120
    redo_tag: str = None
    force_redo: bool = False


# Logging utilities
def log_and_print(message: str):
    logging.info(message)
    click.echo(message)


# Granule and path utilities
def extract_key_from_granule(granule: Granule) -> str:
    """Extract matching base key string from granule UR"""
    ur = granule["Granule"]["GranuleUR"]
    ur = ur[ur.rfind("GEDI"):]  # Get meaningful part
    parts = ur.split("_")[2:5]  # Get the key segments
    return "_".join(parts)  # Join with underscores as string


def hash_granules(granules: List[Granule]) -> Dict[str, Granule]:
    """Create {base_key: granule} mapping with duplicate checking"""
    hashed = {}
    for granule in granules:
        key = extract_key_from_granule(granule)
        if key in hashed:
            raise ValueError(f"Duplicate base key {key} found in granules")
        hashed[key] = granule
    return hashed


def extract_s3_url_from_granule(granule: Granule) -> str:
    urls = granule["Granule"]["OnlineAccessURLs"]["OnlineAccessURL"]
    s3_urls = [url["URL"] for url in urls if url["URL"].startswith("s3")]

    if len(s3_urls) > 1:
        warnings.warn(f"Multiple S3 URLs found in granule: {s3_urls}")

    s3_url = s3_urls[0]

    return s3_url


def granules_match(g1: Granule, g2: Granule) -> bool:
    """Check if two granules match using their extracted keys"""
    try:
        key1 = extract_key_from_granule(g1)
        key2 = extract_key_from_granule(g2)
        return key1 == key2
    except ValueError as e:
        raise ValueError(f"Granule matching failed: {str(e)}")


def stripped_granule_name(granule: Granule) -> str:
    return granule["Granule"]["GranuleUR"].strip().split(".")[0]


def get_existing_keys(config: RunConfig) -> Set[str]:
    """Get set of processed output keys from previous run

    Note: Assumes that the output GeoPackages are named consistent with the
    keys specified in extract_key_from_granule and that the outputs are
    compressed, i.e. <key>.gpkg.bz2 (this is the current behavior of
    WaveformWriter).
    """
    s3 = boto3.client('s3')
    existing = set()

    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(
        Bucket="maap-ops-workspace",
        Prefix=(f"{config.username}/dps_output/{config.algo_id}/"
                "{config.algo_version}/{config.redo_tag}/")
    ):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.gpkg.bz2'):
                # Extract just the filename without path or extensions
                filename = Path(obj['Key']).name
                key = filename.split(".")[0].strip()
                existing.add(key)

    return existing


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




# Processing utilities
def validate_redo_tag(config: RunConfig) -> None:
    """Validate redo tag parameters and check for existing outputs"""
    if not config.force_redo and config.redo_tag == config.tag:
        raise ValueError(
            f"Cannot redo with same tag '{config.tag}' "
            "- use --force-redo to override"
        )

    # Verify S3 path exists
    s3 = boto3.client('s3')
    prefix = (f"{config.username}/dps_output/{config.algo_id}/"
              f"{config.algo_version}/{config.redo_tag}/")
    result = s3.list_objects_v2(
        Bucket="maap-ops-workspace",
        Prefix=prefix,
        MaxKeys=1  # Just check existence
    )
    if not result.get('KeyCount'):
        raise ValueError("No output directory found for "
                         f"redo tag '{redo_tag}'")


def get_collection_id(product: str) -> str:
    """Get collection ID for a GEDI product (l1b/l2a/l4a)"""
    host = "cmr.earthdata.nasa.gov"
    product_map = {
        "l1b": ("GEDI01_B", "002"),
        "l2a": ("GEDI02_A", "002"),
        "l4a": ("GEDI_L4A_AGB_Density_V2_1_2056", None)
    }
    short_name, version = product_map[product]
    params = {
        "short_name": short_name,
        "cmr_host": host,
        "cloud_hosted": "true"
    }
    if version:
        params["version"] = version
    return maap.searchCollection(**params)[0]["concept-id"]


def get_bounding_box(boundary: str) -> tuple:
    """
    Get bounding box of a shapefile or GeoPackage.

    Args:
        boundary: s3 path to the boundary shapefile or GeoPackage.

    Returns:
        Bounding box as a tuple (minx, miny, maxx, maxy).
    """
    boundary_path = s3_url_to_local_path(boundary)
    boundary_gdf: GeoDataFrame = gpd.read_file(boundary_path,
                                               driver="GPKG")
    bbox: tuple = boundary_gdf.total_bounds
    return bbox


def query_granules(product: str,
                   date_range: str = None,
                   boundary: str = None) -> Dict[str, List[Granule]]:
    """
    Query granules from CMR and filter by date range and boundary
    Returns: Dictionary of lists of granules for each product
    """

    # Get collection IDs using the lookup function
    collection_id = get_collection_id(product)

    # Set up search parameters for CMR granule query
    host = "cmr.earthdata.nasa.gov"  # Define host here
    max_results = 10000
    search_kwargs = {
        "concept_id": collection_id,
        "cmr_host": host,
        "limit": max_results,
    }

    if date_range:
        search_kwargs["temporal"] = date_range

    if boundary:
        boundary_bbox: tuple = get_bounding_box(boundary)
        boundary_bbox_str: str = ",".join(map(str, boundary_bbox))
        search_kwargs["bounding_box"] = boundary_bbox_str

    # Query CMR for granules separately per product to handle response limits
    log_and_print("Searching for granules.")
    click.echo("(This may take a few minutes.)")

    granules = maap.searchGranule(**search_kwargs)

    log_and_print(f"Found {len(granules)} {product} granules.")

    return granules

# Hash each product's granules separately
    hashed_granules = {
        product_key: hash_granules(gran_list)
        for product_key, gran_list in product_granules.items()
    }

    # Find subset of keys that occur in all 3 products
    common_keys = (
        set(hashed_granules["l1b"])
        .intersection(hashed_granules["l2a"])
        .intersection(hashed_granules["l4a"])
    )

    # Build matched granules list
    matched_granules: List[Dict[str, Granule]] = []
    for key in common_keys:
        matched_granules.append({
            "l1b": hashed_granules["l1b"][key],
            "l2a": hashed_granules["l2a"][key],
            "l4a": hashed_granules["l4a"][key]
        })

    # Validate that we found matches
    if not matched_granules:
        raise ValueError("No matching granules found"
                         "across all three products")

    log_and_print(f"Found {len(matched_granules)} matching "
                  "sets of granules.")


def match_granules(
        product_granules: Dict[str: List[Granule]]
) -> List[Dict[str, Granule]]:
    # Hash each product's granules separately
    hashed_granules = {
        product_key: hash_granules(gran_list)
        for product_key, gran_list in product_granules.items()
    }

    # Find subset of keys that occur in all 3 products
    common_keys = (
        set(hashed_granules["l1b"])
        .intersection(hashed_granules["l2a"])
        .intersection(hashed_granules["l4a"])
    )

    # Build matched granules list
    matched_granules: List[Dict[str, Granule]] = []
    for key in common_keys:
        matched_granules.append({
            "l1b": hashed_granules["l1b"][key],
            "l2a": hashed_granules["l2a"][key],
            "l4a": hashed_granules["l4a"][key]
        })

    # Validate that we found matches
    if not matched_granules:
        raise ValueError("No matching granules found"
                         "across all three products")

    log_and_print(f"Found {len(matched_granules)} matching "
                  "sets of granules.")

    return matched_granules


def exclude_processed_granules(
        matched_granules: List[Dict[str, Granule]],
        config: RunConfig
):
    exclude_keys = get_existing_keys(config)

    if exclude_keys:
        pre_count = len(matched_granules)
        exclude_set = set(exclude_keys)
        matched_granules = [
            matched
            for matched in matched_granules
            if extract_key_from_granule(matched["l1b"]) not in exclude_set
        ]
        excluded_count = pre_count - len(matched_granules)
        log_and_print(f"Excluded {excluded_count} granules "
                      "with existing outputs")
    else:
        log_and_print("No existing outputs found for redo tag"
                      " - processing all granules")


def prepare_job_kwargs(
        matched_granules: List[Dict[str, Granule]],
        config: RunConfig
):
    """Prepare job submission parameters for each triplet of granules."""

    job_kwargs_list = []
    if config.job_limit:
        n_jobs = min(len(matched_granules), config.job_limit)
    else:
        n_jobs = len(matched_granules)
    log_and_print(f"Submitting {n_jobs} " f"jobs.")

    job_kwargs_list = []
    for matched in matched_granules:
        job_kwargs = {
            "identifier": config.tag,
            "algo_id": config.algo_id,
            "version": config.algo_version,
            "username": config.username,
            "queue": "maap-dps-worker-16gb",
            "L1B": extract_s3_url_from_granule(matched["l1b"]),
            "L2A": extract_s3_url_from_granule(matched["l2a"]),
            "L4A": extract_s3_url_from_granule(matched["l4a"]),
            "config": config.model_config,  # Pass S3 URL directly
            "hse": config.hse,
            "k_allom": config.k_allom
        }

        if config.boundary:
            job_kwargs["boundary"] = config.boundary  # Pass S3 URL directly

        if config.date_range:
            job_kwargs["date_range"] = config.date_range

        job_kwargs_list.append(job_kwargs)

    return job_kwargs_list


@click.command()
@click.option("--username",
              "-u",
              type=str,
              required=True,
              help="MAAP username.")
@click.option("--tag", "-t", type=str, required=True, help="Job tag.")
@click.option(
    "--boundary",
    "-b",
    type=str,
    help=(
        "Path or URL to a shapefile or GeoPackage containing "
        "a boundary polygon. Note: should be accessible "
        "to MAAP DPS workers."
    ),
)
@click.option(
    "--date_range",
    "-d",
    type=str,
    help=(
        "Date range for granule search. "
        "See <https://cmr.earthdata.nasa.gov/search/site/"
        "docs/search/api.html#temporal-range-searches> "
        "for valid formats."
    ),
)
@click.option(
    "--config",
    "-c",
    type=str,
    required=True,
    help=("Path to the configuration YAML file. "
          "Filename must be 'config.yaml' or 'config.yml'."))
@click.option("--hse",
              type=str,
              required=True,
              help="Path to HSE raster file.")
@click.option("--k_allom",
              type=str,
              required=True,
              help="Path to k_allom raster file.")
@click.option("--algo_id",
              "-a",
              type=str,
              required=True,
              help="Algorithm ID to run.")
@click.option(
    "--algo_version",
    "-v",
    type=str,
    required=True,
    help="Algorithm version to run."
)
@click.option("--job_limit",
              "-j",
              type=int,
              help="Limit the number of jobs submitted.")
@click.option(
    "--check_interval",
    "-i",
    type=int,
    default=120,
    help="Time interval (in seconds) between job status checks.",
)
@click.option("--redo", "-r", type=str, help="Tag of previous run to exclude")
@click.option("--force-redo", is_flag=True, help="Allow redo with same tag")
@click.option("--exclude_path", "-e", type=str)
def main(
    username: str,
    tag: str,
    boundary: str,
    date_range: str,
    job_limit: int,
    check_interval: int,
    config: str,
    hse: str,
    k_allom: str,
    algo_id: str,
    algo_version: str,
    redo_tag: str,
    force_redo: bool,
    exclude_path: str,
):
    # Create configuration object
    config = RunConfig(
        username=username,
        tag=tag,
        algo_id=algo_id,
        algo_version=algo_version,
        config=config,
        hse=hse,
        k_allom=k_allom,
        boundary=boundary,
        date_range=date_range,
        job_limit=job_limit,
        check_interval=check_interval,
        redo_tag=redo_tag,
        force_redo=force_redo
    )

    start_time = datetime.datetime.now()

    # Set up output directory
    output_dir = Path(f"run_output_" f"{start_time.strftime('%Y%m%d_%H%M%S')}")
    os.makedirs(output_dir, exist_ok=False)

    # Set up log
    logging.basicConfig(
        filename=output_dir / "run.log",
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    log_and_print(f"Starting new model run at MAAP at {start_time}.")
    log_and_print(f"Boundary: {boundary}")
    log_and_print(f"Date Range: {date_range}")

    # Validate redo tag if specified
    if redo_tag:
        validate_redo_tag(config)

    # Read and log full model configuration
    model_config_path = s3_url_to_local_path(config)
    try:
        with open(model_config_path, "r") as config_file:
            full_model_config = config_file.read()
    except Exception as e:
        log_and_print("Error reading config file"
                      f"from {model_config_path}: {str(e)}")
        raise

    log_and_print(f"Configuration:\n{full_model_config}")

    # Query the CMR for granules
    product_granules: Dict[str, List[Granule]] = {}
    for product in ["l1b", "l2a", "l4a"]:
        granules = query_granules(product,
                                  date_range=date_range,
                                  boundary=boundary)
        product_granules[product] = granules

    matched_granules: List[Dict[str, Granule]] = (
        match_granules(product_granules)
    )

    # Filter out already-processed granules if redo tag is specified
    if config.redo_tag:
        matched_granules = exclude_processed_granules(matched_granules,
                                                      config)

    job_kwargs_list = prepare_job_kwargs(matched_granules, config)

    # Submit jobs in batches
    jobs = []
    job_batch_counter = 0
    job_batch_size = 50
    job_submit_delay = 2
    for job_kwargs in job_kwargs_list[:job_limit]:
        try:
            job = maap.submitJob(**job_kwargs)
            jobs.append(job)
            job_batch_counter += 1
        except Exception as e:
            log_and_print(f"Error submitting job: {e}")
            continue

        if job_batch_counter == job_batch_size:
            time.sleep(job_submit_delay)
            job_batch_counter = 0

    print(f"Submitted {len(jobs)} jobs.")

    job_ids = [job.id for job in jobs]

    # Write job IDs to a file in case processing is interrupted
    job_ids_file = output_dir / "job_ids.txt"
    with open(job_ids_file, "w") as f:
        for job_id in job_ids:
            f.write(f"{job_id}\n")
    log_and_print(f"Submitted job IDs written to {job_ids_file}")

    # Give the jobs time to start
    click.echo("Waiting for jobs to start...")
    time.sleep(10)

    # Initialize job monitoring
    job_manager = JobManager(job_ids, check_interval=config.check_interval)
    job_manager.monitor()

    # Log the succeeded and failed job IDs
    succeeded_job_ids = [
        job_id for job_id in job_ids if job_status_for(job_id) == "Succeeded"
    ]

    failed_job_ids = [
        job_id for job_id in job_ids if job_status_for(job_id) == "Failed"
    ]

    other_job_ids = [
        job_id
        for job_id in job_ids
        if job_status_for(job_id) not in ["Succeeded", "Failed"]
    ]

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
