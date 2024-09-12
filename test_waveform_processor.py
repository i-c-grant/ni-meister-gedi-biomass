import argparse
from datetime import datetime
import os
from typing import Dict
import logging

from nmbim import (
    algorithms, processing_pipelines, Beam, Waveform,
    WaveformProcessor, WaveformCollection, WaveformWriter, WaveformPlot
)

def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("l1b", help="Path to L1B file", type=str)
    parser.add_argument("l2a", help="Path to L2A file", type=str)
    # parser.add_argument(
        # "-n", "--number", help="Number of waveforms to process", type=int
    # )
    parser.add_argument(
        "-c",
        "--cache",
        help="Cache beams in memory instead of accessing files directly",
        action="store_true"
    )
    parser.add_argument(
        "-b",
        "--beamwise",
        help="Process waveforms one beam at a time to reduce memory usage",
        action="store_true",
    )

    return parser

def process_waveforms(waveforms: WaveformCollection,
                      processor_params: Dict[str, Dict],
                      output_dir: str,
                      output_name: str):

    # Create pipeline and process waveforms
    pipeline = []
    for proc_name in processor_params:
        p = WaveformProcessor(**processor_params[proc_name],
                              waveforms=waveforms)
        pipeline.append(p)
        
    for p in pipeline:
        p.process()

    # Write processed data
    waveform_writer = WaveformWriter(
        path=f"{output_dir}/{output_name}.gpkg",
        cols={"biwf": "results/biomass_index",
              "num_modes": "metadata/modes/num_modes",
              "quality_flag": "metadata/flags/quality",},
        append=True,
        waveforms=waveforms
    )
    waveform_writer.write()

def main():
    parser = create_parser()
    args = parser.parse_args()

    # Create output directory with timestamp
    now = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = f"tests/output_{now}"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_name = f"test_run_{now}"

    # Define processor parameters
    processor_params = processing_pipelines.pipeline_biomass_index_simple

    # Beam processing or whole file processing
    beams = ["BEAM0000",
             "BEAM0001",
             "BEAM0010",
             "BEAM0011",
             "BEAM0101",
             "BEAM0110",
             "BEAM1000",
             "BEAM1011"]

    # Quality control filters
    def flag_filter(wf: Waveform) -> bool:
        """Filter waveforms based on metadata or data quality."""
        if wf.get_data("metadata/flags/quality") == 1:
            return True
        else: 
            return False

    def modes_filter(wf: Waveform) -> bool:
        """Keep only waveforms with more than one mode."""
        if wf.get_data("metadata/modes/num_modes") > 1:
            return True
        else:
            return

    my_filters = [flag_filter]

    if args.beamwise:
        for beam in beams:
            waveforms = WaveformCollection(args.l1b,
                                           args.l2a,
                                           limit=args.number,
                                           cache_beams=args.cache,
                                           beams=[beam],
                                           filters=my_filters)
            process_waveforms(waveforms,
                              processor_params,
                              output_dir,
                              output_name)
    else:
        waveforms = WaveformCollection(args.l1b,
                                       args.l2a,
                                       limit=args.number,
                                       cache_beams=args.cache,
                                       filters=my_filters)
        process_waveforms(waveforms,
                          processor_params,
                          output_dir,
                          output_name)

    # Log the run
    logging.basicConfig(filename=f"{output_dir}/run.log", level=logging.INFO)
    logging.info(f"Run completed at {now}")
    # Store command line arguments
    logging.info(f"Command line arguments: {args}")
    # Store parameters
    logging.info(f"Processor parameters: {processor_params}")
    
if __name__ == "__main__":
    main()
