import h5py
import numpy as np
from typing import Any, Dict, Union, List
from nmbim.CachedBeam import CachedBeam

DSet = h5py.Dataset
InputBeam = Union[h5py.Group, CachedBeam]

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

        - elev: dict[str, np.uint16]
            Dictionary containing elevation data.

    processed: dict[str, Any]
        Dictionary containing processed waveform data. Initially empty.

    results: dict[str, Any]
        Dictionary containing the results of the NMBIM model.
    """

    
    def __init__(
            self,
            shot_number: int,
            l1b_beam: InputBeam,
            l2a_beam: InputBeam,
    ) -> None:
        """Initializes the Waveform object.

        Parameters
        ----------
        shot_number: int
            The unique shot number of the waveform.

        beam: str
            The beam name for the waveform (e.g. "BEAM0000").

        l1b_beam: InputBeam
            The L1B beam data.

        l2a_beam: InputBeam
            The L2A beam data.

        """
        self.l1b_beam: InputBeam = l1b_beam
        self.l2a_beam: InputBeam = l2a_beam

        # Get beam name from CachedBeam object or h5py.Group
        if isinstance(l1b_beam, CachedBeam):
            beam = l1b_beam.get_beam_name()
        elif isinstance(l1b_beam, h5py.Group):
            beam = l1b_beam.name.split("/")[-1]

        # Store initial metadata
        self.metadata = {
            "shot_number": shot_number,
            "beam": beam,
        }

        # Get shot index for this waveform (requires initial metadata)
        self.metadata["shot_index"] = self._get_shot_index()
        
        shot_index = self.metadata["shot_index"]

        # Store geolocation
        lats: DSet = self._get_dataset(
            "l1b", ["geolocation", "latitude_bin0"]
        )
        lon: DSet = self._get_dataset(
            "l1b", ["geolocation", "longitude_bin0"]
        )
        self.metadata["coords"] = {
            "lat": lats[shot_index],
            "lon": lon[shot_index],
        }

        # Initialize read-only waveform data (see property below)
        wf: DSet = self._get_waveform()
        mean_noise: float = self._get_dataset(
            "l1b", ["noise_mean_corrected"]
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

    def _get_shot_index(self) -> np.int64:
        # Find the index of this Waveform's shot within its beam group
        shot_number: np.int64 = self.metadata["shot_number"]
        shot_nums_l1b: DSet = self._get_dataset("l1b", ["shot_number"])
        shot_nums_l2a: DSet = self._get_dataset("l2a", ["shot_number"])
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

    def _get_waveform(self) -> DSet:
        shot_index: np.int64 = self.metadata["shot_index"]
        # Extract waveform data, converting to 0-based index
        start_idxs: DSet = self._get_dataset("l1b", ["rx_sample_start_index"])
        counts: DSet = self._get_dataset("l1b", ["rx_sample_count"])
        full_wf: DSet = self._get_dataset("l1b", ["rxwaveform"])
        wf_start = np.uint64(start_idxs[shot_index])
        wf_count = np.uint64(counts[shot_index])
        wf: DSet = full_wf[wf_start: wf_start + wf_count]
        return wf

    def _get_elev(self) -> Dict[str, Union[np.float32, np.float64]]:
        shot_index = self.metadata["shot_index"]

        # Extract elevation data and calculate height above ground.
        top = np.float64(
            self._get_dataset("l2a", ["elev_highestreturn"])[shot_index]
        )
        bottom = np.float64(
            self._get_dataset(
                "l1b", ["geolocation", "elevation_lastbin"]
            )[shot_index]
        )
        ground = np.float32(
            self._get_dataset("l2a", ["elev_lowestmode"])[shot_index]
        )

        elev = {"top": top, "bottom": bottom, "ground": ground}

        return elev

    def _get_dataset(self, which_product: str, keys: List[str]) -> Union[DSet, np.ndarray]:
        # Get dataset from L1B or L2A input beam.

        # Returns h5py.Dataset if beam is h5py.Group (lazy loading)
        # or numpy.ndarray if beam is CachedBeam (already loaded)
        
        if which_product == "l1b":
            beam: InputBeam = self.l1b_beam
        elif which_product == "l2a":
            beam: InputBeam = self.l2a_beam
        else:
            raise ValueError(f"Data product must be 'l1b' or 'l2a', got {which_product}")

        # Traverse nested dictionary to get dataset
        dataset: Any = beam
        for key in keys:
            dataset = dataset[key]

        if not isinstance(dataset, (DSet, np.ndarray)):
            raise TypeError(
                f"Expected h5py.Dataset or numpy array in {which_product} at {keys}, got {type(dataset)}"
            )
        else:
            return dataset

    def get_data(self, keys: List[str]) -> Any:
        """Get data from the Waveform object.

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
