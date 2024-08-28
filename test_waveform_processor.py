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

    l1b = h5py.File(l1b_path, "r")
    l2a = h5py.File(l2a_path, "r")

    # Define processor parameters
    processor_params = {
        "noise": {"alg_fun" : algorithms.remove_noise,
                  "input_map" : {"wf": "raw/wf",
                               "mean_noise": "raw/mean_noise"},
                  "output_path" : "processed/wf_noise_removed",
                  "params" : {}},
        "normalize": {"alg_fun" : algorithms.normalize_waveform,
                      "input_map" : {"wf": "processed/wf_noise_removed"},
                      "output_path" : "processed/wf_noise_normalized",
                      "params" : {}},
        "smooth": {"alg_fun" : algorithms.smooth_waveform,
                   "input_map" : {"wf": "processed/wf_noise_normalized"},
                   "output_path" : "processed/wf_noise_normalized_smoothed",
                   "params" : {"sd": 5}},
        "per_height": {"alg_fun" : algorithms.calc_wf_per_height,
                       "input_map" : {"wf": "processed/wf_noise_normalized_smoothed"},
                       "output_path" : "processed/wf_per_height",
                       "params" : {"dz": .1}},
        "segment": {"alg_fun" : algorithms.separate_veg_ground,
                    "input_map" : {"wf": "processed/wf_per_height",
                                 "ht": "processed/ht",
                                 "rh": "raw/rh"},
                    "output_path" : "processed/wf_segmented",
                    "params" : {"veg_floor": 5}},
        "height": {"alg_fun" : algorithms.calc_height,
                   "input_map" : {"wf": "raw/wf",
                                "elev_top": "raw/elev/top",
                                "elev_bottom": "raw/elev/bottom",
                                "elev_ground": "raw/elev/ground"},
                   "output_path" : "processed/ht",
                   "params" : {}}
    }

    # Get beam names, excluding metadata group
    beams = list(l1b.keys())[:-1]

    for beam in beams:

        # Load beam data into memory if caching is enabled
        # Otherwise, access data directly from file
        if args.cache:
            l1b_beam = CachedBeam(l1b, beam)
            l2a_beam = CachedBeam(l2a, beam)
        else:
            l1b_beam = l1b[beam]
            l2a_beam = l2a[beam]

        if num_waveforms is not None:
            shot_numbers = l1b[beam]["shot_number"][:num_waveforms]
        else:
            shot_numbers = l1b[beam]["shot_number"][:]

        print(f"Processing {len(shot_numbers)} waveforms for beam {beam}")
        
        # Define waveform arguments that don't change between waveforms
        waveform_args = {"l1b_beam": l1b_beam, "l2a_beam": l2a_beam}

        # Create set to store waveforms, ensuring no duplicates
        waveforms = set()

        # Create waveforms
        for shot_number in shot_numbers:
            waveform_args["shot_number"] = shot_number
            waveforms.add(Waveform(**waveform_args))

        # Create processors
        proc_list = ["height",
                     "noise",
                     "normalize",
                     "smooth"]

        pipeline = []

        for proc_name in proc_list:
            p = WaveformProcessor(**processor_params[proc_name], waveforms = waveforms)
            pipeline.append(p)

        # Process waveforms, applying each processor in order
        for p in pipeline:
            p.process()

    l1b.close()
    l2a.close()


if __name__ == "__main__":
    main()
