import h5py
import numpy as np


class Waveform:
    """Fetches NMBIM-relevant data for one waveform from L1B and L2A files.

    Data in the Waveform class is raw and unprocessed. The class is intended
    to be used as a data container in the WaveformProcessor class.

    Attributes
    ----------
    wf : dict[str, np.ndarray]
        A dictionary containing the waveform data and height above ground.
    rh : np.ndarray
        The relative height data from L2A.
    mean_noise : float
        The mean noise for the waveform.
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

        # Find the index of the shot number within the L1B and L2A files
        shot_index = np.where(l1b[beam]["shot_number"][:] == shot_number)[0][0]
        assert shot_index == (
            np.where(l2a[beam]["shot_number"][:] == shot_number)[0][0]
        )

        # Extract the waveform, height, and relative height data
        self.wf = self._get_waveform(l1b, beam, shot_index)
        self.mean_noise = l1b[beam]["noise_mean_corrected"][shot_index]
        self.height = self._get_height(l1b, l2a, beam, shot_index)
        self.rh = l2a[beam]["rh"][shot_index]

        # Extract the gelocation data
        self.coords = {}
        self.coords["lat"] = l1b[beam]["geolocation"]["latitude_bin0"][shot_index]
        self.coords["lon"] = l1b[beam]["geolocation"]["longitude_bin0"][shot_index]

        l1b.close()
        l2a.close()

    def _get_waveform(
        self, l1b: h5py.File, beam: str, shot_index: int
    ) -> dict[str, np.ndarray]:
        # Extract the waveform data from the L1B file

        wf_start = np.uint64(l1b[beam]["rx_sample_start_index"][shot_index] - 1)
        wf_count = np.uint64(l1b[beam]["rx_sample_count"][shot_index])

        wf = l1b[beam]["rxwaveform"][wf_start : wf_start + wf_count]

        return wf

    def _get_height(
        self, l1b: h5py.File, l2a: h5py.File, beam: str, shot_index: int
    ) -> np.ndarray:
        # Extract elevation data and calculate height above ground

        elev = {
            "start": l1b[beam]["geolocation"]["elevation_bin0"][shot_index],
            "end": l1b[beam]["geolocation"]["elevation_lastbin"][shot_index],
            "ground": l2a[beam]["elev_lowestmode"][shot_index],
        }

        height = np.linspace(elev["start"], elev["end"], len(self.wf)) - elev["ground"]

        return height
