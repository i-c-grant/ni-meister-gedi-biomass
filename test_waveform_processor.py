from nmbim.Waveform import Waveform
from nmbim.WaveformProcessor import WaveformProcessor
import nmbim.algorithms as algorithms
from nmbim.CachedBeam import CachedBeam
import h5py
import argparse
import cProfile


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("l1b", help="Path to L1B file", type=str)
    parser.add_argument("l2a", help="Path to L2A file", type=str)
    parser.add_argument(
        "-n", "--number", help="Number of waveforms to process", type=int
    )
    parser.add_argument(
        "-c",
        '--cache',
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

    l1b = h5py.File(l1b_path, "r")
    l2a = h5py.File(l2a_path, "r")

    beams = list(l1b.keys())[:-1]  # Exclude "metadata" group

    for beam in beams:

        # Load beam data into memory
        l1b_beam = CachedBeam(l1b, beam)
        l2a_beam = CachedBeam(l2a, beam)

        if num_waveforms:
            shot_numbers = l1b[beam]["shot_number"][:num_waveforms]
        else:
            shot_numbers = l1b[beam]["shot_number"][:]

        print(f"Processing {len(shot_numbers)} waveforms for beam {beam}")

        # Set up processor using noise removal algorithm
        processor_args = {
            "alg_fun": algorithms.remove_noise,
            "input_map": {"wf": ["raw", "wf"], "mean_noise": ["raw", "mean_noise"]},
            "output_path": ["processed", "wf_noise_removed"],
            "params": {},
        }

        waveform_args = {"l1b_beam": l1b_beam, "l2a_beam": l2a_beam}

        processor = WaveformProcessor(**processor_args)

        for shot_number in shot_numbers:
            waveform_args["shot_number"] = shot_number
            waveform = Waveform(**waveform_args)
            processor.process(waveform)

    l1b.close()
    l2a.close()


if __name__ == "__main__":
    main()
