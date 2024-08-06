import h5py
import numpy as np

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
        Dictionary containing processed waveform data (e.g. height above ground). Initially empty.

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

        # Find the index of the shot number within the L1B and L2A files
        shot_index = np.where(l1b[beam]["shot_number"][:] == shot_number)[0][0]
        assert shot_index == (
            np.where(l2a[beam]["shot_number"][:] == shot_number)[0][0]
        )

        self.raw = {
            "wf": self._get_waveform(l1b, beam, shot_index),
            "mean_noise": l1b[beam]["noise_mean_corrected"][shot_index],
            "elev": self._get_elev(l1b, l2a, beam, shot_index)
        }

        self.metadata = {
            "coords": {
                "lat": l1b[beam]["geolocation"]["latitude_bin0"][shot_index],
                "lon": l1b[beam]["geolocation"]["longitude_bin0"][shot_index],
            },
            "shot_number": shot_number,
            "shot_index": shot_index,
            "beam": beam,
            "l1b_path": l1b_path,
            "l2a_path": l2a_path
        }

        self.processed = {}
        self.results = {}

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

    def _get_elev(
        self, l1b: h5py.File, l2a: h5py.File, beam: str, shot_index: int
    ) -> dict[str, np.uint16]:
        """Extracts elevation data and calculates height above ground."""
        elev = {
            "start": l1b[beam]["geolocation"]["elevation_bin0"][shot_index],
            "end": l1b[beam]["geolocation"]["elevation_lastbin"][shot_index],
            "ground": l2a[beam]["elev_lowestmode"][shot_index],
        }
        return elev
