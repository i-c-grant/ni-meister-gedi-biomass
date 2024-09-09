import argparse
from datetime import datetime
import cProfile
import os
from pathlib import Path

import h5py

import nmbim.algorithms as algorithms
import nmbim.processing_pipelines as processing_pipelines
from nmbim.CachedBeam import CachedBeam
from nmbim.Waveform import Waveform
from nmbim.WaveformProcessor import WaveformProcessor
from nmbim.WaveformWriter import WaveformWriter


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("l1b", help="Path to L1B file", type=str)
    parser.add_argument("l2a", help="Path to L2A file", type=str)
    parser.add_argument(
        "-n", "--number", help="Number of waveforms to process", type=int
    )
    parser.add_argument(
        "-c",
        "--cache",
        help="Cache beams in memory instead of accessing files directly",
        action="store_true"
    )

    args = parser.parse_args()

    l1b_path = args.l1b
    l2a_path = args.l2a

    if args.number:
        num_waveforms = args.number
    else:
        num_waveforms = None

    # Create output directory with timestamp
    output_dir = f"tests/output_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Define processor parameters
    processor_params = processing_pipelines.pipeline_biomass_index_simple

    with h5py.File(l1b_path, "r") as l1b, h5py.File(l2a_path, "r") as l2a:
        # Get beam names, excluding metadata group
        beams = [key for key in l1b.keys() if key != 'METADATA']

        for beam in beams:

            # Load beam data into memory if caching is enabled
            # Otherwise, access data directly from file
            if args.cache:
                l1b_beam = CachedBeam(l1b, beam)
                l2a_beam = CachedBeam(l2a, beam)
            else:
                l1b_beam = l1b[beam]
                l2a_beam = l2a[beam]

            # Limit number of waveforms to process if limit was specified
            if num_waveforms is not None:
                shot_numbers = l1b[beam]["shot_number"][:num_waveforms]

            else:
                shot_numbers = l1b[beam]["shot_number"][:]

            print(f"Processing {len(shot_numbers)} waveforms for beam {beam}")

            # Define within-beam waveform arguments that don't change between waveforms
            waveform_args = {"l1b_beam": l1b_beam, "l2a_beam": l2a_beam}

            # Create set of waveforms for this beam, ensuring no duplicates
            waveforms = set()
            for shot_number in shot_numbers:
                waveform_args["shot_number"] = shot_number
                waveforms.add(Waveform(**waveform_args))

            # Create pipeline of processors for this beam
            pipeline = []
            for proc_name in processor_params:
                p = WaveformProcessor(**processor_params[proc_name], waveforms = waveforms)
                pipeline.append(p)

            # Process waveforms, applying each processor in order
            for p in pipeline:
                p.process()
                
            # Write processed data to GeoPackage
            waveform_writer = WaveformWriter(path=f"{output_dir}/test.gpkg",
                                             cols={"biwf": "results/biomass_index",
                                                   "lat": "metadata/coords/lat",
                                                   "lon": "metadata/coords/lon"},
                                             append=True,
                                             waveforms=waveforms)
            waveform_writer.write()

if __name__ == "__main__":
    main()
