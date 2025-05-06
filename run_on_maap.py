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
from pathlib import Path
from typing import Dict, List

import click
from maap.maap import MAAP
from maap.Result import Granule

from maap_utils.RunConfig import RunConfig
from maap_utils.JobManager import JobManager

from maap_utils.utils import (exclude_redo_granules,
                              match_granules,
                              query_granules,
                              prepare_job_kwargs,
                              s3_url_to_local_path,
                              validate_redo_tag)

maap = MAAP(maap_host="api.maap-project.org")


# CLI tool for running processing jobs on MAAP
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
@click.option("--redo_tag", "-r", type=str, help="Tag of previous run to exclude")
@click.option("--force-redo", is_flag=True, help="Allow redo with same tag")
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
):
    # Create configuration object
    run_config = RunConfig(
        username=username,
        tag=tag,
        algo_id=algo_id,
        algo_version=algo_version,
        model_config=config,
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

    # Set up logging with both file and console handlers
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # File handler with timestamps
    file_handler = logging.FileHandler(
        filename=output_dir / "run.log",
        mode="w"
    )
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))

    # Console handler without timestamps
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(message)s"))

    # Add both handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logging.info(f"Starting new model run at MAAP at {start_time}.")
    logging.info(f"Boundary: {boundary}")
    logging.info(f"Date Range: {date_range}")

    # validate redo tag if specified
    if redo_tag:
        validate_redo_tag(run_config)

    # Read and log full model configuration
    model_config_path = s3_url_to_local_path(run_config.model_config)
    try:
        with open(model_config_path, "r") as config_file:
            full_model_config = config_file.read()
    except Exception as e:
        logging.error("Error reading config file"
                      f"from {model_config_path}: {str(e)}")
        raise

    logging.info(f"Configuration:\n{full_model_config}")

    # Query the CMR for granules
    product_granules: Dict[str, List[Granule]] = {}
    for product in ["l1b", "l2a", "l4a"]:
        granules = query_granules(product,
                                  date_range=date_range,
                                  boundary=boundary,
                                  limit=run_config.job_limit)
        product_granules[product] = granules

    matched_granules: List[Dict[str, Granule]] = (
        match_granules(product_granules)
    )

    # Filter out already-processed granules if redo tag is specified
    if run_config.redo_tag:
        matched_granules = exclude_redo_granules(matched_granules,
                                                 run_config)

    job_kwargs_list = prepare_job_kwargs(matched_granules, run_config)

    # Initialize and submit jobs
    job_manager = JobManager(run_config,
                             job_kwargs_list,
                             check_interval=run_config.check_interval)
    job_manager.submit(output_dir)

    # Monitor job progress and show report
    job_manager.monitor()
    job_manager.report()
    end_time = datetime.datetime.now()

    logging.info(f"Model run completed at {end_time}.")


if __name__ == "__main__":
    main()
