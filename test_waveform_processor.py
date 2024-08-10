from nmbim.Waveform import Waveform
from nmbim.WaveformProcessor import WaveformProcessor
import nmbim.algorithms as algorithms
import h5py
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("l1b", help="Path to L1B file")
    parser.add_argument("l2a", help="Path to L2A file")
    args = parser.parse_args()
    
    l1b_path = args.l1b
    l2a_path = args.l2a

    l1b = h5py.File(l1b_path, "r")
    l2a = h5py.File(l2a_path, "r")

    beam = "BEAM0000"

    shot_numbers = l1b[beam]["shot_number"][:]

    waveform = Waveform(l1b = l1b, l2a = l2a, shot_number = shot_numbers[818], beam = beam)

    alg_fun = algorithms.remove_noise
    input_map = {"wf": ["raw", "wf"], "mean_noise": ["raw", "mean_noise"]}
    output_path = ["processed", "wf_noise_removed"]
    params = {}

    processor = WaveformProcessor(alg_fun = alg_fun,
                                  params = params,
                                  input_map = input_map,
                                  output_path = output_path)

    processor.process(waveform)

    l1b.close()
    l2a.close()

    print(f"Processed waveform {waveform.metadata['shot_number']}")
    print(f"Results: {waveform.processed['wf_noise_removed']}")
