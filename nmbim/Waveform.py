import h5py
import numpy as np
from typing import Any, Dict, Union, List

DSet = h5py.Dataset


class Waveform:
    """Fetches NMBIM-relevant data for one waveform from L1B and L2A files.
    Responsible for storing raw and processed data, as well as metadata.
    Not responsible for processing waveform data or opening/closing files.

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
        self, shot_number: int, beam: str, l1b: h5py.File, l2a: h5py.File
    ) -> None:
        """Initializes the Waveform object.

        Parameters
        ----------
        shot_number: int
            The unique shot number of the waveform.

        beam: str
            The beam name for the waveform (e.g. "BEAM0000").

        l1b: h5py.File
            Open HDF5 file object for the L1B file.

        l2a: h5py.File
            Open HDF5 file object for the L2A file.

        """
        self.l1b = l1b
        self.l2a = l2a

        # Store initial metadata
        self.metadata = {
            "shot_number": shot_number,
            "beam": beam,
            "l1b_path": self.l1b.filename,
            "l2a_path": self.l2a.filename,
        }

        # Get shot index for this waveform (requires initial metadata)
        self.metadata["shot_index"] = self._get_shot_index(l1b)
        if self.metadata["shot_index"] != self._get_shot_index(l2a):
            raise ValueError("Shot indicies in L1B and L2A files do not match")

        shot_index = self.metadata["shot_index"]

        # Store geolocation
        lats: DSet = self._get_dataset(
            "l1b", [beam, "geolocation", "latitude_bin0"]
        )
        lon: DSet = self._get_dataset(
            "l1b", [beam, "geolocation", "longitude_bin0"]
        )
        self.metadata["coords"] = {
            "lat": lats[shot_index],
            "lon": lon[shot_index],
        }

        # Initialize read-only waveform data (see property below)
        wf: DSet = self._get_waveform()
        mean_noise: float = self._get_dataset(
            "l1b", [beam, "noise_mean_corrected"]
        )[shot_index]
        elev: Dict[str, Union[np.float32, np.float64]] = self._get_elev()
        self._raw = {
            "wf": wf,
            "mean_noise": mean_noise,
            "elev": elev,
        }

        self.processed = {}
        self.results = {}

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
        shot_nums: DSet = self._get_dataset("l1b", [beam, "shot_number"])
        index = np.int64(np.where(shot_nums[:] == shot_number)[0][0])
        return index

    def _get_waveform(self) -> DSet:
        beam: str = self.metadata["beam"]
        shot_index: np.int64 = self.metadata["shot_index"]
        # Extract waveform data, converting to 0-based index
        start_idxs: DSet = self._get_dataset(
            "l1b", [beam, "rx_sample_start_index"]
        )
        counts: DSet = self._get_dataset("l1b", [beam, "rx_sample_count"])
        full_wf: DSet = self._get_dataset("l1b", [beam, "rxwaveform"])
        wf_start = np.uint64(start_idxs[shot_index])
        wf_count = np.uint64(counts[shot_index])
        wf: DSet = full_wf[wf_start: wf_start + wf_count]
        return wf

    def _get_elev(self) -> Dict[str, Union[np.float32, np.float64]]:
        beam = self.metadata["beam"]
        shot_index = self.metadata["shot_index"]

        # Extract elevation data and calculate height above ground.
        top = np.float64(
            self._get_dataset("l2a", [beam, "elev_highestreturn"])[shot_index]
        )
        bottom = np.float64(
            self._get_dataset(
                "l1b", [beam, "geolocation", "elevation_lastbin"]
            )[shot_index]
        )
        ground = np.float32(
            self._get_dataset("l2a", [beam, "elev_lowestmode"])[shot_index]
        )

        elev = {"top": top, "bottom": bottom, "ground": ground}

        return elev

    def _get_group(self, which_file: str, key: str) -> h5py.Group:
        if which_file not in ["l1b", "l2a"]:
            raise ValueError(
                f"File type must be 'l1b' or 'l2a', not {which_file}"
            )
        file: h5py.File = getattr(self, which_file)
        return file[key]  # type: ignore

    def _get_dataset(self, which_file: str, keys: List[str]) -> DSet:
        if which_file not in ["l1b", "l2a"]:
            raise ValueError(
                f"File type must be 'l1b' or 'l2a', not {which_file}"
            )
        file: h5py.File = getattr(self, which_file)
        path: str = "/".join(keys)
        dataset = file[path]  # type: ignore
        if not isinstance(dataset, h5py.Dataset):
            raise TypeError(
                f"Expected dataset in H5 file {file} at path {path}"
            )
        else:
            return dataset

    def get_data(self, keys: List[str]) -> Any:
        """Get data from the waveform object.

        Parameters
        ----------
        keys: List[str]
            List of keys indicating where to find data in the Waveform.

        Returns
        -------
        data: np.ndarray
            The data from the waveform.
        """
        data_dict = getattr(self, keys[0])
        for key in keys[1:]:
            obj = data_dict[key] # type: ignore
        return obj

    def save_data(self, data: Any, keys: List[str]) -> None:
        """Save data to the Waveform.

        Parameters
        ----------
        data: Any
            The data to save.

        keys: List[str]
            List of keys indicating where to save data in the Waveform.
        """
        data_dict = getattr(self, keys[0])
        for key in keys[1:-1]:
            data_dict = data_dict[key]
        data_dict[keys[-1]] = data
