import argparse
from datetime import datetime
import os

from nmbim import (
    algorithms, processing_pipelines, Beam, Waveform,
    WaveformProcessor, WaveformCollection, WaveformWriter
)

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

    waveforms = WaveformCollection(l1b_path,
                                   l2a_path,
                                   limit=num_waveforms,
                                   cache_beams=args.cache,)
    
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
