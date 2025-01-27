##########################################################################
# This script processes triplets of GEDI L1B, L2A, L4A granules (i.e.    #
# files) locally to calculate the Ni-Meister Biomass Index (NMBI) for    #
# each footprint in the granules. Single-processing and multi_processing #
# modes are supported.                                                   #
##########################################################################

import logging
from datetime import datetime
from typing import Any, Dict, Union, Callable, List, Optional
from pathlib import Path

import click
import h5py
import yaml

from nmbim import WaveformCollection, ParameterLoader, ScalarSource, RasterSource, app_utils, filters, algorithms

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

def load_config(config_path: str) -> Dict[str, Any]:
    """Load and return the configuration from a YAML file."""
    try:
        with open(config_path, 'r') as config_file:
            return yaml.safe_load(config_file)
    except IOError as e:
        logging.error(f"Error opening config file: {e}")
        raise
    except yaml.YAMLError as e:
        logging.error(f"Error parsing YAML in config file: {e}")
        raise

# Define function for processing a single beam.
# This is the base processing function for both serial and parallel processing.
def process_beam(
    beam: str,
    l1b_path: str,
    l2a_path: str,
    l4a_path: str,
    hse_source: Union[str, float],
    k_allom_source: Union[str, float],
    output_path: str,
    processor_kwargs_dict: Dict[str, Dict[str, Any]],
    filters: Union[Dict[str, Optional[Callable]], bytes],
) -> None:
    # Unpickle the filters if necessary
    if isinstance(filters, bytes):
        filters = dill.loads(filters)

    click.echo(f"Loading waveforms for beam {beam}...")

    # Load the waveforms for the beam
    try:
        with h5py.File(l1b_path, "r") as l1b, h5py.File(l2a_path, "r") as l2a, h5py.File(l4a_path, "r") as l4a:
            waveforms = WaveformCollection(
                l1b,
                l2a,
                l4a,
                cache_beams=True,
                beams=[beam],
                filters=filters.values(),
            )
    except IOError as e:
        logging.error(f"Error opening HDF5 files: {e}")
        raise click.ClickException(f"Error opening HDF5 files: {e}")
    except Exception as e:
        logging.error(f"Error creating WaveformCollection: {e}")
        raise click.ClickException(f"Error creating WaveformCollection: {e}")
    
    click.echo(f"{len(waveforms)} waveforms loaded for beam {beam}.")

    # Parameterize the waveforms for the beam
    click.echo(f"Parameterizing waveforms for beam {beam}...")
    param_sources = {}
    
    # Handle parameter sources with explicit error propagation
    try:
        sources = {
            'hse': hse_source,
            'k_allom': k_allom_source
        }
        
        for param_name, source in sources.items():
            try:
                # Try to parse as number first
                scalar = float(source)
                param_sources[param_name] = ScalarSource(scalar)
            except ValueError:
                # If not a number, check if it's a valid raster path
                if source.lower().endswith(('.tif', '.tiff')):
                    param_sources[param_name] = RasterSource(source)
                else:
                    raise ValueError(
                        f"Invalid {param_name} source: {source}. "
                        "Must be numeric value or path to .tif/.tiff file"
                    )
    except Exception as e:
        logging.error(f"Error configuring parameter sources: {str(e)}")
        click.echo(f"Error: {str(e)}", err=True)
        raise click.ClickException(str(e))
    
    param_loader = ParameterLoader(
        sources=param_sources,
        waveforms=waveforms
    )

    param_loader.parameterize()
    
    # Process the waveforms for the beam
    click.echo(f"Processing waveforms for beam {beam}...")
    app_utils.process_waveforms(waveforms, processor_kwargs_dict)
    click.echo(f"Waveforms for beam {beam} processed.")

    app_utils.write_waveforms(waveforms, output_path)
    click.echo(f"Waveforms for beam {beam} written to {output_path}.\n")


@click.command()
@click.argument("l1b_path", type=click.Path(exists=True))
@click.argument("l2a_path", type=click.Path(exists=True))
@click.argument("l4a_path", type=click.Path(exists=True))
@click.argument("hse_source", type=click.UNPROCESSED)
@click.argument("k_allom_source", type=click.UNPROCESSED)
@click.argument("output_dir", type=click.Path(exists=True))
@click.option("--config", "-c", type=click.Path(exists=True),
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
         l4a_path: str,
         hse_source: Union[str, float],
         k_allom_source: Union[str, float],
         output_dir: str,
         config: str,
         parallel: bool,
         n_workers: int,
         boundary: Optional[str],
         date_range: Optional[str]) -> None:
    """Process GEDI L1B, L2A, and L4A granules to calculate the Ni-Meister Biomass
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


    #####################################
    # Configure the processing pipeline #
    #####################################
    # Read the configuration
    full_config: Dict[str, Any] = load_config(config)

    # Get the processor configuration
    processor_config = full_config.get('processing_pipeline', {})

    # replace the 'algorithm' key with the actual algorithm function
    processor_kwargs_dict = processor_config.copy()
    for proc_name, proc_config in processor_kwargs_dict.items():
        alg_name: str = proc_config['alg_fun']
        alg_fun: Callable = getattr(algorithms, alg_name)
        proc_config['alg_fun'] = alg_fun
    
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
        
    # Handle case where spatial or temporal filters are specified without parameters,
    # but no parameters are supplied at runtime either
    if not boundary and 'spatial' in filter_config and not filter_config['spatial']:
        filter_config.pop('spatial')
        log_and_print("Spatial filter removed because no boundary file was supplied.")

    if not date_range and 'temporal' in filter_config and not filter_config['temporal']:
        filter_config.pop('temporal')
        log_and_print("Temporal filter removed because no date range was supplied.")

    # Generate and log filters
    my_filters: Dict[str, Optional[Callable]] = filters.generate_filters(filter_config)
    
    # Log applied filters
    applied_filters = [name for name, f in my_filters.items() if f is not None]

    if applied_filters:
        for filter_name in applied_filters:
            log_and_print(f"{filter_name.capitalize()} filter applied")
    else:
        log_and_print("No filters applied")

    # Update the full configuration with runtime filters
    full_config['filters'] = filter_config

    # Log the updated configuration
    logging.info("Updated configuration:")
    logging.info(yaml.dump(full_config))

    ###############################
    # Run the processing pipeline #
    ###############################
    beams = app_utils.get_beam_names()
           
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
                l4a_path,
                hse_source,
                k_allom_source,
                output_path,
                processor_kwargs_dict,
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
                l4a_path,
                hse_source,
                k_allom_source,
                output_path,
                processor_kwargs_dict,
                my_filters,
            )

    click.echo(f"Output written to {output_path}")
    click.echo("Run complete.")

    # Log the run
    finish_time = datetime.now()
    logging.info(
        f"Run completed at {finish_time.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    logging.info(f"Run duration: {finish_time - start_time}")
    logging.info(
        f"Command line arguments: l1b_path={l1b_path}, "
        f"l2a_path={l2a_path}, "
        f"l4a_path={l4a_path}"
    )

if __name__ == "__main__":
    main()
