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
import shutil
from pathlib import Path
from typing import Dict, List
import time
import signal

import click
from maap.maap import MAAP
from maap.Result import Granule

from maap_utils.RunConfig import RunConfig
from maap_utils.JobManager import JobManager

from maap_utils.utils import (
    exclude_redo_granules,
    match_granules,
    query_granules,
    prepare_job_kwargs,
    s3_url_to_local_path,
    validate_redo_tag,
)

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
    help=(
        "Path to the configuration YAML file. "
        "Filename must be 'config.yaml' or 'config.yml'."
    ),
)
@click.option("--hse", type=str, required=True, help="Path to HSE raster file.")
@click.option("--k_allom", type=str, required=True, help="Path to k_allom raster file.")
@click.option("--algo_id", "-a", type=str, required=True, help="Algorithm ID to run.")
@click.option(
    "--algo_version", "-v", type=str, required=True, help="Algorithm version to run."
)
@click.option("--job_limit", "-j", type=int, help="Limit the number of jobs submitted.")
@click.option("--redo-of", "-r", type=str, help="Tag of previous run to redo")
@click.option("--force-redo", is_flag=True, help="Allow redo with same tag")
@click.option(
    "--no-redo",
    is_flag=True,
    help="Disable automatic resubmission of failed jobs"
)
def main(
    username: str,
    tag: str,
    boundary: str,
    date_range: str,
    job_limit: int,
    config: str,
    hse: str,
    k_allom: str,
    algo_id: str,
    algo_version: str,
    redo_of: str,
    force_redo: bool,
    no_redo: bool,
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
        redo_of=redo_of,
        force_redo=force_redo,
    )

    start_time = datetime.datetime.now()

    # Set up output directory
    output_dir = Path(f"run_output_" f"{start_time.strftime('%Y%m%d_%H%M%S')}")
    os.makedirs(output_dir, exist_ok=False)

    # Set up logging with both file and console handlers
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # File handler with timestamps
    file_handler = logging.FileHandler(filename=output_dir / "run.log",
                                       mode="w")
    file_handler.setLevel(logging.INFO)

    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(message)s",
                          datefmt="%Y-%m-%d %H:%M:%S")
    )

    # Console handler without timestamps
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(message)s"))
    console_handler.setLevel(logging.INFO)

    # Add both handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logging.info("\n")
    logging.info("****************************************"
                 "****************************************")
    logging.info(f"Starting new model run at MAAP at {start_time}.")
    logging.info(f"Boundary: {boundary}")
    logging.info(f"Date Range: {date_range}")

    # validate redo tag if specified
    if redo_of:
        validate_redo_tag(run_config)

    # Copy config file into output directory for safekeeping
    model_config_path = s3_url_to_local_path(run_config.model_config)
    try:
        shutil.copy(model_config_path, output_dir / Path(model_config_path).name)
    except Exception as e:
        logging.error(f"Error copying config file from {model_config_path}: {str(e)}")
        raise

    # Query the CMR for granules
    product_granules: Dict[str, List[Granule]] = {}
    for product in ["l1b", "l2a", "l4a"]:
        granules = query_granules(
            product,
            date_range=date_range,
            boundary=boundary,
            limit=run_config.job_limit,
        )
        product_granules[product] = granules

    matched_granules: List[Dict[str, Granule]] = match_granules(product_granules)

    # Filter out already-processed granules if redo tag is specified
    if run_config.redo_of:
        matched_granules = exclude_redo_granules(matched_granules, run_config)

    job_kwargs_list = prepare_job_kwargs(matched_granules, run_config)

    # Initialize and submit jobs
    job_manager = JobManager(
        run_config,
        job_kwargs_list,
    )
    job_manager.submit(output_dir)

    # Handle monitoring and potential resubmissions
    def prompt_after_interrupt() -> str:
        """Prompt user after monitoring is interrupted by Ctrl-C."""
        timeout = 10
        if no_redo:
            print(f"Monitoring suspended. Waiting {timeout} seconds to resume or press Ctrl-C to exit.")
            try:
                time.sleep(timeout)
                return "resume"
            except KeyboardInterrupt:
                return "exit"
        else:
            print("\nMonitoring suspended. Enter 'r' to resubmit failed jobs, "
                  f"'x' to exit, or wait {timeout} seconds to resume.")
            try:
                # wait for user input with timeout
                signal.signal(signal.SIGALRM, lambda s, f: (_ for _ in ()).throw(TimeoutError()))
                signal.alarm(timeout)
                answer = input("> ").strip().lower()
                signal.alarm(0)
                if answer == "r":
                    return "resubmit"
                elif answer == "x":
                    return "exit"
                return "resume"
            except TimeoutError:
                return "resume"
            except KeyboardInterrupt:
                return "exit"

    def prompt_after_run() -> str:
        """Prompt user after jobs finish with some failures."""
        if no_redo:
            return "exit"
        else:
            answer = input(
                "\nAll jobs finished with some failures. Enter 'r' to retry failures, 'x' to exit.\n> "
            ).strip().lower()
            if answer == "r":
                return "resubmit"
            return "exit"

    # Main monitoring loop
    while True:
        run_status = job_manager.monitor()

        # monitoring exited because user intiated an interrupt via Ctrl-C
        # and some jobs are still pending
        if run_status == "interrupted":
            next_step = prompt_after_interrupt()
            if next_step == "resubmit":
                job_manager.resubmit_unsuccessful_jobs()
                continue
            elif next_step == "exit":
                break
            else:
                continue

        # monitoring exited because all jobs were successful
        elif run_status == "succeeded":
            break

        # monitoring exited because all jobs finished,
        # but some were not successful
        elif run_status == "partial":
            next_step = prompt_after_run()
            if next_step == "resubmit":
                job_manager.resubmit_unsuccessful_jobs()
                continue
            else:
                break

    job_manager.exit_gracefully()

    end_time = datetime.datetime.now()
    logging.info(f"Model run completed at {end_time}.")
    logging.info("****************************************"
                 "****************************************")


if __name__ == "__main__":
    main()
