import h5py
import numpy as np
from typing import Any, Dict, Union
import numpy.typing as npt

DSet = h5py.Dataset

class Waveform:
    """Fetches NMBIM-relevant data for one waveform from L1B and L2A files.

    Data in the Waveform class is raw and unprocessed. The class is intended
    to be used as a data container in the WaveformProcessor class.

    Attributes
    ----------
    metadata: dict[str, Any]
        Dictionary containing metadata for the waveform.

    raw: dict[str, Any]
        Dictionary containing unprocessed waveform data from L1B and L2A files.
        - wf: np.ndarray
            The laser return data (from rxwaveform in L1B file).

        - mean_noise: np.uint16
            The mean noise value.

        - elev: dict[str, np.uint16]
            Dictionary containing elevation data.

    processed: dict[str, Any]
        Dictionary containing processed waveform data. Initially empty.

    results: dict[str, Any]
        Dictionary containing the results of the NMBIM model.
    """

    def __init__(
        self, shot_number: int, beam: str, l1b_path: str, l2a_path: str
    ) -> None:
        """Initializes the Waveform object.

        Parameters
        ----------
        shot_number: int
            The unique shot number of the waveform.

        beam: str
            The beam name for the waveform (e.g. "BEAM0000").

        l1b_path: str
            The path to the L1B file.

        l2a_path: str
            The path to the L2A file.

        """
        l1b = h5py.File(l1b_path, "r")
        l2a = h5py.File(l2a_path, "r")

        l1b_beam: h5py.Group = Waveform._get_group(l1b, beam)

        # Store initial metadata
        self.metadata = {
            "shot_number": shot_number,
            "beam": beam,
            "l1b_path": l1b_path,
            "l2a_path": l2a_path,
        }

        # Get shot index for this waveform (requires initial metadata)
        self.metadata["shot_index"] = self._get_shot_index(l1b)
        if self.metadata["shot_index"] != self._get_shot_index(l2a):
            raise ValueError("Shot indicies in L1B and L2A files do not match")

        shot_index = self.metadata["shot_index"]

        # Store geolocation
        lats: DSet = Waveform._get_dataset(l1b, [beam, "geolocation", "latitude_bin0"])
        lon: DSet = Waveform._get_dataset(l1b, [beam, "geolocation", "longitude_bin0"])
        self.metadata["coords"] = {
            "lat": lats[shot_index],
            "lon": lon[shot_index],
        }

        # Initialize read-only representation of raw waveform data (see property and setter below)
        wf: DSet = self._get_waveform(l1b_file=l1b)
        mean_noise: float = Waveform._get_dataset(l1b, [beam, "noise_mean_corrected"])[shot_index]
        elev: Dict[str, Union[np.float32, np.float64]] = (
            self._get_elev(l1b_file=l1b, l2a_file=l2a)
        )
        self._raw = {
            "wf": wf,
            "mean_noise": mean_noise,
            "elev": elev,
        }

        self.processed = {}
        self.results = {}

        l1b.close()
        l2a.close()

    # Make raw data read-only
    @property
    def raw(self) -> dict[str, Any]:
        return self._raw

    @raw.setter
    def raw(self, value: dict[str, Any]) -> None:
        raise AttributeError("Raw waveform data cannot be modified.")

    def _get_shot_index(self, in_file: h5py.File) -> np.int64:
        # Find the index of this Waveform's shot within its beam group
        beam: str = self.metadata["beam"]
        shot_number: np.int64 = self.metadata["shot_number"]
        shot_nums: DSet = Waveform._get_dataset(in_file, [beam, "shot_number"])
        index = np.int64(np.where(shot_nums[:] == shot_number)[0][0])
        return index

    def _get_waveform(self, l1b_file: h5py.File) -> DSet:
        beam: str = self.metadata["beam"]
        shot_index: np.int64 = self.metadata["shot_index"]
        _get_dataset = Waveform._get_dataset
        # Extract the waveform data from the L1B file, converting to 0-based index
        start_idxs: DSet = _get_dataset(l1b_file, [beam, "rx_sample_start_index"])
        counts: DSet = _get_dataset(l1b_file, [beam, "rx_sample_count"])
        full_wf: DSet = _get_dataset(l1b_file, [beam, "rxwaveform"])
        wf_start = np.uint64(start_idxs[shot_index])
        wf_count = np.uint64(counts[shot_index])
        wf: DSet = full_wf[wf_start : wf_start + wf_count]
        return wf

    def _get_elev(
        self, l1b_file: h5py.File, l2a_file: h5py.File
    ) -> Dict[str, Union[np.float32, np.float64]]:
        beam = self.metadata["beam"]
        shot_index = self.metadata["shot_index"]

        # Extract elevation data and calculate height above ground.
        top = np.float64(Waveform._get_dataset(l2a_file, [beam, "elev_highestreturn"])[shot_index])
        bottom = np.float64(Waveform._get_dataset(l1b_file, [beam, "geolocation", "elevation_lastbin"])[shot_index])
        ground = np.float32(Waveform._get_dataset(l2a_file, [beam, "elev_lowestmode"])[shot_index])
        
        elev = {
            "top": top,
            "bottom": bottom,
            "ground": ground
        }

        return elev

    @staticmethod
    def _get_group(file: h5py.File, key: str) -> h5py.Group:
        return file[key] # type: ignore

    @staticmethod
    def _get_dataset(file: h5py.File, keys: list[str]) -> DSet:
        path: str = "/".join(keys)
        dataset = file[path] # type: ignore
        if not isinstance(dataset, h5py.Dataset):
            raise TypeError(f"Expected dataset in H5 file {file} at path {path}")
        else:
            return dataset
