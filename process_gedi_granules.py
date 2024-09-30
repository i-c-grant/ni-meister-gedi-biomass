##########################################################################
# This script processes paired GEDI L1B and L2A granules (i.e. files)    #
# to calculate the Ni-Meister Biomass Index (NMBI) for each footprint in #
# the granules. The NMBI is a metric of above-ground biomass density.    #
##########################################################################

import logging
from datetime import datetime
from typing import Any, Dict, Union
from pathlib import Path
import warnings

import click
import h5py

from nmbim import WaveformCollection, app_utils, processing_pipelines, filters

# Import modules for parallel processing if available
try:
    from multiprocessing import Pool
    import dill

    dill.settings["recurse"] = True
    MULTIPROCESSING_AVAILABLE = True
except ImportError:
    MULTIPROCESSING_AVAILABLE = False

# Define function for processing a single beam.
# This function is used in both serial and parallel modes.
def process_beam(
    beam: str,
    l1b_path: str,
    l2a_path: str,
    output_path: str,
    processor_params: Dict[str, Dict[str, Any]],
    filters: Union[Dict[str, Any], bytes],
) -> None:
    # Unpickle the filters if necessary
    if type(filters) == bytes:
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
@click.option("--boundary", "-b", type=click.Path(exists=True),
              help=("Path to a Shapefile or GeoPackage containing "
                    "a boundary polygon. Must contain only one layer "
                    "and only polygon or multipolygon geometry."))
@click.option("--date_range", "-d", type=str,
              help=("Date range for filtering granules. Format for one "
                    "date is %Y-%m-%dT%H:%M:%SZ. 'date1, date2' provides a "
                    "range, 'date1,' provides a start date, and ',date2' "
                    "provides an end date. See NASA CMR search "
                    "documentation for more information: "
                    "https://cmr.earthdata.nasa.gov/search/site/docs/search/"
                    "api.html#temporal-range-searches"))
@click.option("--parallel", "-p", is_flag=True, help="Run in parallel mode.")
@click.option(
    "--n_workers",
    "-n",
    default=4,
    help="Number of workers for parallel mode."
)
def main(l1b_path: str,
         l2a_path: str,
         output_dir: str,
         boundary: str,
         date_range: str,
         parallel: bool,
         n_workers: int):
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
    my_filters = filters.define_filters()

    if boundary:
        my_filters.append(filters.generate_spatial_filter(boundary))

    if date_range:
        time_start, time_end = None, None
        dates = date_range.split(",")
        if len(dates) > 2:
            raise ValueError("Invalid date range. Please provide a single "
                             "date, a date range, or a start and end date.")
        if len(dates) == 1:
            warnings.warn("Only one date provided. This will be treated as "
                          "a start date. Using a leading or trailing comma "
                          "to specify how a single date should be handled.")
            dates.append(None)

        date_spec = "%Y-%m-%dT%H:%M:%SZ"
        if dates[0]:
            time_start: datetime = datetime.strptime(dates[0], date_spec)
        if dates[1]:
            time_end: datetime = datetime.strptime(dates[1], date_spec)

        if time_start and time_end and time_start > time_end:
            raise ValueError("The start date must be before the end date.")
            
        my_filters.append(filters.generate_temporal_filter(time_start,
                                                           time_end))
    
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

    logging.info(f"Output written to {output_path}")

    # Log the run
    finish_time = datetime.now()
    logging.info(
        f"Run completed at {finish_time.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    logging.info(f"Run duration: {finish_time - start_time}")
    logging.info(
        f"Command line arguments: l1b_path={l1b_path}, " f"l2a_path={l2a_path}"
    )
    # Add newline to params to make log more readable
    f_processor_params = str(processor_params).replace(", ", ",\n")
    logging.info(f"Processor parameters: {f_processor_params}")


if __name__ == "__main__":
    main()
