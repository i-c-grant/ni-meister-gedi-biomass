import h5py
import numpy as np
from typing import Any, Dict, Union
import numpy.typing as npt


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
        shot_number : int
            The unique shot number of the waveform.

        beam : str
            The beam name for the waveform (e.g. "BEAM0000").

        l1b_path : str
            The path to the L1B file.

        l2a_path : str
            The path to the L2A file.

        """
        l1b = h5py.File(l1b_path, "r")
        l2a = h5py.File(l2a_path, "r")

        l1b_beam: h5py.Group = l1b[beam]  # type: ignore
        l2a_beam: h5py.Group = l2a[beam]  # type: ignore

        # Find the index of the shot number within the L1B and L2A files
        shot_index: np.int64 = self._get_shot_index(l1b_beam, shot_number)
        assert shot_index == self._get_shot_index(l2a_beam, shot_number)

        self._raw = {
            "wf": self._get_waveform(l1b_beam, shot_index),
            "mean_noise": l1b_beam["noise_mean_corrected"][shot_index],  # type: ignore
            "elev": self._get_elev(l1b_beam, l2a_beam, shot_index),
        }

        breakpoint()
        self.metadata = {
            "coords": {
                "lat": l1b_beam["geolocation"]["latitude_bin0"][shot_index],  # type: ignore
                "lon": l1b_beam["geolocation"]["longitude_bin0"][shot_index],  # type: ignore
            },
            "shot_number": shot_number,
            "shot_index": shot_index,
            "beam": beam,
            "l1b_path": l1b_path,
            "l2a_path": l2a_path,
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
        beam = self.metadata["beam"]
        shot_number = self.metadata["shot_number"]
        beam_group : h5py.Group = in_file[beam]  # type: ignore
        index = np.where(beam_group["shot_number"][:] == shot_number)[0][0]
        return index

    def _get_waveform(
        self, l1b_beam: h5py.Group, shot_index: np.int64
    ) -> npt.NDArray[np.float32]:
        # Extract the waveform data from the L1B file, converting to 0-based index
        wf_start: np.uint64 = (
            np.uint64(l1b_beam["rx_sample_start_index"][shot_index]) - 1
        )  # type: ignore
        wf_count: np.uint64 = np.uint64(l1b_beam["rx_sample_count"][shot_index])  # type: ignore
        wf: npt.NDArray[np.float32] = l1b_beam["rxwaveform"][wf_start : wf_start + wf_count]  # type: ignore

        return wf

    def _get_elev(
        self, l1b_beam: h5py.Group, l2a_beam: h5py.Group, shot_index: np.int64
    ) -> Dict[str, Union[np.float32, np.float64]]:
        # Extract elevation data and calculate height above ground.
        elev = {
            "top": np.float64(l1b_beam["geolocation"]["elevation_bin0"][shot_index]),  # type: ignore
            "bottom": np.float64(
                l1b_beam["geolocation"]["elevation_lastbin"][shot_index]
            ),  # type: ignore
            "ground": np.float32(l2a_beam["elev_lowestmode"][shot_index]),  # type: ignore
        }

        return elev

