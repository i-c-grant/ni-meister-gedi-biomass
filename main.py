import logging
import os
from datetime import datetime
from typing import Any, Dict, Union

import click
import h5py

from nmbim import WaveformCollection, app_utils, processing_pipelines

# Import modules for parallel processing if available
try:
    from multiprocessing import Pool
    import dill
    dill.settings['recurse'] = True
    MULTIPROCESSING_AVAILABLE = True
except ImportError:
    MULTIPROCESSING_AVAILABLE = False

# Define function for processing a single beam.
# This function is used in both serial and parallel modes.
def process_beam(beam: str,
                 l1b_path: str,
                 l2a_path: str,
                 output_dir: str,
                 output_name: str,
                 processor_params: Dict[str, Dict[str, Any]],
                 filters: Union[Dict[str, Any], bytes]) -> None:

    # Unpickle the filters if necessary
    if type(filters) == bytes:
        filters = dill.loads(filters)

    click.echo(f"Loading waveforms for beam {beam}...")

    with h5py.File(l1b_path, 'r') as l1b, h5py.File(l2a_path, 'r') as l2a:
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

        app_utils.write_waveforms(waveforms, output_dir, output_name)
        click.echo(f"Waveforms for beam {beam} written to {output_dir}.\n")


@click.command()
@click.argument("l1b_path", type=click.Path(exists=True))
@click.argument("l2a_path", type=click.Path(exists=True))
@click.option("--parallel", "-p", is_flag=True, help="Run in parallel mode.")
@click.option("--n_workers", "-n", default=4, help="Number of workers for parallel mode.")
def main(l1b_path: str, l2a_path: str, parallel: bool, n_workers: int):
    # Create output directory with timestamp
    start_time = datetime.now()
    output_dir = f"results/output_{start_time.strftime('%Y%m%d_%H%M%S')}"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_name = f"test_run_{start_time.strftime('%Y%m%d_%H%M%S')}"

    # Set up the processing pipeline
    processor_params = processing_pipelines.biwf_pipeline
    beams = app_utils.get_beam_names()
    my_filters = app_utils.define_filters()

    if not MULTIPROCESSING_AVAILABLE and parallel:
        click.echo(
            "Multiprocessing is not available on this system. "
            "Running in serial mode."
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
                output_dir,
                output_name,
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
            process_beam(beam,
                         l1b_path,
                         l2a_path,
                         output_dir,
                         output_name,
                         processor_params,
                         my_filters)

    click.echo(f"Run completed.")

    # Log the run
    logging.basicConfig(filename=f"{output_dir}/run.log", level=logging.INFO)
    finish_time = datetime.now()
    logging.info(
        f"Run completed at {finish_time.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    logging.info(f"Run duration: {finish_time - start_time}")
    logging.info(
        f"Command line arguments: l1b_path={l1b_path}, " f"l2a_path={l2a_path}"
    )
    logging.info(f"Processor parameters: {processor_params}")


if __name__ == "__main__":
    main()
