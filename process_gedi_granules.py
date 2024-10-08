##########################################################################
# This script processes paired GEDI L1B and L2A granules (i.e. files)    #
# to calculate the Ni-Meister Biomass Index (NMBI) for each footprint in #
# the granules. The NMBI is a metric of above-ground biomass density.    #
##########################################################################

import logging
from datetime import datetime
from typing import Any, Dict, Union, Callable, List, Optional
from pathlib import Path
import warnings

import click
import h5py
import yaml

from nmbim import WaveformCollection, app_utils, processing_pipelines, filters

# Import modules for parallel processing if available
try:
    from multiprocessing import Pool
    import dill

    dill.settings["recurse"] = True
    MULTIPROCESSING_AVAILABLE = True
except ImportError:
    MULTIPROCESSING_AVAILABLE = False

def log_and_print(message: str) -> None:
    """Log a message and print it to the console."""
    logging.info(message)
    click.echo(message)

# Define function for processing a single beam.
# This function is used in both serial and parallel modes.
def process_beam(
    beam: str,
    l1b_path: str,
    l2a_path: str,
    output_path: str,
    processor_params: Dict[str, Dict[str, Any]],
    filters: Union[Dict[str, Optional[Callable]], bytes],
) -> None:
    # Unpickle the filters if necessary
    if isinstance(filters, bytes):
        filters = dill.loads(filters)

    click.echo(f"Loading waveforms for beam {beam}...")

    with h5py.File(l1b_path, "r") as l1b, h5py.File(l2a_path, "r") as l2a:
        waveforms = WaveformCollection(
            l1b,
            l2a,
            cache_beams=True,
            beams=[beam],
            filters=filters,
        )

        click.echo(f"{len(waveforms)} waveforms loaded for beam {beam}.")
        click.echo(f"Processing waveforms for beam {beam}...")
        app_utils.process_waveforms(waveforms, processor_params)
        click.echo(f"Waveforms for beam {beam} processed.")

        app_utils.write_waveforms(waveforms, output_path)
        click.echo(f"Waveforms for beam {beam} written to {output_path}.\n")


@click.command()
@click.argument("l1b_path", type=click.Path(exists=True))
@click.argument("l2a_path", type=click.Path(exists=True))
@click.argument("output_dir", type=click.Path(exists=True))
@click.option("--config", "-c", type=click.Path(exists=True),
              default="nmbim/filter_config.yaml",
              help="Path to the filter configuration YAML file.")
@click.option("--parallel", "-p", is_flag=True, help="Run in parallel mode.")
@click.option(
    "--n_workers",
    "-n",
    default=4,
    help="Number of workers for parallel mode."
)
@click.option("--boundary", type=click.Path(exists=True), help="Path to boundary file (e.g., .gpkg)")
@click.option("--date_range", help="Date range in format 'YYYY-MM-DDTHH:MM:SSZ,YYYY-MM-DDTHH:MM:SSZ'")
def main(l1b_path: str,
         l2a_path: str,
         output_dir: str,
         config: str,
         parallel: bool,
         n_workers: int,
         boundary: Optional[str],
         date_range: Optional[str]):
    """Process GEDI L1B and L2A granules to calculate the Ni-Meister Biomass
    Index (NMBI) for each footprint in the granules."""

    # Set up output directory and log file
    start_time: datetime = datetime.now()
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_name = Path(app_utils.build_output_filename(l1b_path, l2a_path))
    output_dir = Path(output_dir)
    output_path = (output_dir / output_name).with_suffix(".gpkg")

    logging.basicConfig(
        filename=f"{output_dir}/run.log",
        format="%(asctime)s - %(message)s",
        level=logging.INFO,
    )

    logging.info(f"Run started at "
                 f"{start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Set up the processing pipeline
    processor_params = processing_pipelines.biwf_pipeline
    beams = app_utils.get_beam_names()
    
    # Read the configuration
    with open(config, 'r') as config_file:
        full_config: Dict[str, Any] = yaml.safe_load(config_file)
    
    filter_config: Dict[str, Any] = full_config.get('filters', {})

    # Update configuration if boundary or date_range are provided
    if boundary:
        if 'spatial' in filter_config:
            if filter_config['spatial']:
                log_and_print("Warning: Overwriting existing spatial filter configuration.")
            else:
                log_and_print("Adding spatial filter configuration from boundary file supplied at runtime.")
        filter_config['spatial'] = {'file_path': boundary}
        log_and_print(f"Spatial filter applied with boundary file: {boundary}")

    if date_range:
        if 'temporal' in filter_config:
            if filter_config['temporal']:
                log_and_print("Warning: Overwriting existing temporal filter configuration.")
            else:
                log_and_print("Adding temporal filter configuration from date range supplied at runtime.")

        start, end = date_range.split(',')
        filter_config['temporal'] = {'time_start': start, 'time_end': end}
        log_and_print(f"Temporal filter applied with date range: {date_range}")

    # Generate filters
    my_filters: Dict[str, Optional[Callable]] = (
        filters.generate_filters(filters.get_filter_generators(), filter_config)
    )
    
    # Log applied filters
    applied_filters = [name for name, f in my_filters.items() if f is not None]
    if applied_filters:
        for filter_name in applied_filters:
            log_and_print(f"{filter_name.capitalize()} filter applied")
    else:
        log_and_print("No filters applied")

    # Create a backup of the original configuration
    backup_config = f"{config}.bak"
    with open(config, 'r') as original_file, open(backup_config, 'w') as backup_file:
        backup_file.write(original_file.read())

    # Update the full configuration and write it back to file
    full_config['filters'] = filter_config
    with open(config, 'w') as config_file:
        yaml.dump(full_config, config_file)
    
    # Log the updated configuration
    log_and_print("Updated configuration:")
    log_and_print(yaml.dump(full_config))
           
    if not MULTIPROCESSING_AVAILABLE and parallel:
        logging.warning(
            "Multiprocessing is not available on this system. "
            "Switching to serial mode."
        )
        parallel = False

    if parallel:
        pickled_filters = dill.dumps(my_filters)
        pool_size = n_workers
        pool_args_list = [
            (
                beam,
                l1b_path,
                l2a_path,
                output_path,
                processor_params,
                pickled_filters,
            )
            for beam in beams
        ]

        # Process the beams concurrently
        with Pool(pool_size) as pool:
            pool.starmap(process_beam, pool_args_list)

    else:
        for beam in beams:
            process_beam(
                beam,
                l1b_path,
                l2a_path,
                output_path,
                processor_params,
                my_filters,
            )

    click.echo(f"Output written to {output_path}")
    click.echo(f"Run complete.")

    # Log the run
    finish_time = datetime.now()
    logging.info(
        f"Run completed at {finish_time.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    logging.info(f"Run duration: {finish_time - start_time}")
    logging.info(
        f"Command line arguments: l1b_path={l1b_path}, "
        f"l2a_path={l2a_path}"
    )

    # Add newline to params to make log more readable
    f_processor_params = str(processor_params).replace(", ", ",\n")
    logging.info(f"Processor parameters: {f_processor_params}")


if __name__ == "__main__":
    main()
