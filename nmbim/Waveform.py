import h5py
import numpy as np
from numpy.typing import ArrayLike
from typing import Any, Dict, Union, List
from nmbim.Beam import Beam

class Waveform:
    """Fetches NMBIM-relevant data for one waveform from files or cached data.
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

        - elev: Dict[str, Union[np.float32, np.float64]]
            Dictionary containing elevation data.

    processed: dict[str, Any]
        Dictionary containing processed waveform data. Initially empty.

    results: dict[str, Any]
        Dictionary containing the results of the NMBIM model.
    """

    
    def __init__(
            self,
            shot_number: int,
            l1b_beam: Beam,
            l2a_beam: Beam,
    ) -> None:
        """Initializes the Waveform object.

        Parameters
        ----------
        shot_number: int
            The unique shot number of the waveform.

        l1b_beam: Beam
            The L1B beam data.

        l2a_beam: Beam
            The L2A beam data.
        """

        self.l1b_beam: Beam = l1b_beam
        self.l2a_beam: Beam = l2a_beam

        beam_name = l1b_beam.get_beam_name()
        
        # Store initial metadata
        self.metadata = {
            "shot_number": shot_number,
            "beam": beam_name,
        }

        # Get shot index for this waveform (requires initial metadata)
        # This is the index of the shot within its files,
        # not the unique shot number
        self.metadata["shot_index"] = self._get_shot_index()
        
        shot_index = self.metadata["shot_index"]

        # Store geolocation
        lats: ArrayLike = self.l1b_beam.extract_dataset("geolocation/latitude_bin0")
        lons: ArrayLike = self.l1b_beam.extract_dataset("geolocation/longitude_bin0")

        self.metadata["coords"] = {
            "lat": lats[shot_index],
            "lon": lons[shot_index],
        }

        # Get quality flag and number of modes from L2A beam
        self.metadata["flags"] = {
            "quality": self.l2a_beam.extract_dataset("quality_flag")[shot_index],
            "surface": self.l2a_beam.extract_dataset("surface_flag")[shot_index],
        }

        self.metadata["landcover"] = {
            "modis_nonvegetated": self.l2a_beam.extract_dataset("land_cover_data/modis_nonvegetated")[shot_index],
            "modis_treecover": self.l2a_beam.extract_dataset("land_cover_data/modis_treecover")[shot_index],
            "landsat_treecover": self.l2a_beam.extract_dataset("land_cover_data/landsat_treecover")[shot_index],
        }

        self.metadata["modes"] = {
            "num_modes": self.l2a_beam.extract_dataset("num_detectedmodes")[shot_index],
        }

        # Initialize read-only waveform data (see property below)
        wf: ArrayLike = self._get_waveform()
        mean_noise = self.l1b_beam.extract_dataset("noise_mean_corrected")[shot_index]
        elev: Dict[str, Union[np.float32, np.float64]] = self._get_elev()
        rh: ArrayLike = self.l2a_beam.extract_dataset("rh")

        self._raw = {
            "wf": wf,
            "mean_noise": mean_noise,
            "elev": elev,
            "rh": rh,
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

    def _get_shot_index(self) -> np.int64:
        # Find the index of this Waveform's shot within its beam group
        shot_number: np.int64 = self.metadata["shot_number"]
        shot_nums_l1b: ArrayLike = self.l1b_beam.extract_dataset("shot_number")
        shot_nums_l2a: ArrayLike = self.l2a_beam.extract_dataset("shot_number")
        index_l1b: np.int64 = np.where(shot_nums_l1b == shot_number)[0][0]
        index_l2a: np.int64 = np.where(shot_nums_l2a == shot_number)[0][0]

        # Ensure shot numbers match between L1B and L2A
        if index_l1b != index_l2a:
            raise ValueError(
                f"File mismatch: L1B shot index {index_l1b} != L2A shot index {index_l2a}"
            )
        else:
            index = index_l1b

        return index

    def _get_waveform(self) -> ArrayLike:
        shot_index: np.int64 = self.metadata["shot_index"]
        # Extract waveform data, converting to 0-based index
        start_idxs: ArrayLike = self.l1b_beam.extract_dataset("rx_sample_start_index")
        wf_start: int = np.int64(start_idxs[shot_index])
        counts: ArrayLike = self.l1b_beam.extract_dataset("rx_sample_count")
        full_wf: ArrayLike = self.l1b_beam.extract_dataset("rxwaveform")
        wf_count = counts[shot_index]
        wf: ArrayLike = full_wf[wf_start: wf_start + wf_count]
        return wf

    def _get_elev(self) -> Dict[str, Union[np.float32, np.float64]]:
        shot_index = self.metadata["shot_index"]
        # Elevation of top of waveform return window
        top = np.float64(
            self.l2a_beam.extract_dataset("elev_highestreturn")[shot_index]
        )
        # Elevation of bottom of waveform return window
        bottom = np.float64(
            self.l1b_beam.extract_dataset("geolocation/elevation_lastbin")[shot_index]
        )
        # Elevation of ground, set to elevation of lowest detected waveform mode
        ground = np.float32(
            self.l2a_beam.extract_dataset("elev_lowestmode")[shot_index]
        )

        elev = {"top": top, "bottom": bottom, "ground": ground}

        return elev

    def get_data(self, path: str) -> Any:
        """
        Get data from the Waveform object.

        Parameters
        ----------
        path: str
            The path to the data in the Waveform object, e.g. "raw/wf".

        Returns
        -------
        data: Any
            The data from the waveform.
        """
        keys = path.split("/")

        if not hasattr(self, keys[0]):
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{keys[0]}'"
            )

        # Get top-level dictionary
        data = getattr(self, keys[0])

        # Traverse dictionary to get data
        for key in keys[1:]:
            data = data[key] # type: ignore

        return data

    def save_data(self, data: Any, path: str) -> None:
        """Save data to the Waveform.

        Parameters
        ----------
        data: Any
            The data to save.

        path: str
            Path to the data in the Waveform object, e.g. "processed/heights".
        """

        keys = path.split("/")
        if not hasattr(self, keys[0]):
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{keys[0]}'"
            )

        data_dict = getattr(self, keys[0])
        for key in keys[1:-1]:
            # Create nested dicts to the save path if they don't exist
            if key not in data_dict:
                data_dict[key] = {}
            data_dict = data_dict[key]
        data_dict[keys[-1]] = data

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Waveform):
            return False
        else:
            return self.metadata == other.metadata

    def __hash__(self) -> int:
        beam = self.metadata["beam"]
        shot_number = self.metadata["shot_number"]
        return hash((beam, shot_number))
