import argparse
from datetime import datetime
import os
import logging

from nmbim import (
    processing_pipelines,
    app_utils,
    WaveformCollection,
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
        action="store_true",
    )
    parser.add_argument(
        "-b",
        "--beamwise",
        help="Process waveforms one beam at a time to reduce memory usage",
        action="store_true",
    )

    return parser


def main():
    parser = create_parser()
    args = parser.parse_args()

    # Create output directory with timestamp
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"tests/output_{now}"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_name = f"test_run_{now}"

    # Define processor parameters
    processor_params = processing_pipelines.pipeline_test_veg_ground_sep

    # Beam processing or whole file processing
    beams = [
        "BEAM0000",
        "BEAM0001",
        "BEAM0010",
        "BEAM0011",
        "BEAM0101",
        "BEAM0110",
        "BEAM1000",
        "BEAM1011",
    ]

    my_filters = app_utils.define_filters()

    if args.beamwise:
        for beam in beams:
            waveforms = WaveformCollection(
                args.l1b,
                args.l2a,
                cache_beams=args.cache,
                beams=[beam],
                filters=my_filters,
            )

            app_utils.process_waveforms(waveforms, processor_params)

            # WaveformPlot(waveforms[0]).segmentation_plot()

            app_utils.write_waveforms(waveforms, output_dir, output_name)

    else:
        waveforms = WaveformCollection(
            args.l1b, args.l2a, cache_beams=args.cache, filters=my_filters
        )

        app_utils.process_waveforms(waveforms, processor_params)

        app_utils.write_waveforms(waveforms, output_dir, output_name)

    # Log the run
    logging.basicConfig(filename=f"{output_dir}/run.log", level=logging.INFO)
    logging.info(f"Run completed at {now}")
    # Store command line arguments
    logging.info(f"Command line arguments: {args}")
    # Store parameters
    logging.info(f"Processor parameters: {processor_params}")


if __name__ == "__main__":
    main()
