import h5py
import numpy as np
from typing import Dict, Union

# Recursive type alias for nested dictionary of numpy arrays
BeamData = Dict[str, Union[np.ndarray, "BeamData"]]


class CachedBeam:
    """
    Loads a beam h5py.Group from an L1B and L2A file and caches it in memory.

    Attributes
    ----------
    file: h5py.File
        The file containing the beam data.

    file_path: str
        The path to the file containing the beam data.

    beam: str
        The name of the beam to cache.

    data: BeamData
        The cached beam data. Values are either numpy arrays or nested dictionaries.
    """

    def __init__(self, file: h5py.File, beam: str) -> None:
        """Initializes CachedBeam by loading the beam data from the file.

        Parameters
        ----------
        file: h5py.File
            The file containing the beam data.

        beam: str
            The name of the beam to cache.
        """
        self.file = file
        self.file_path = file.filename
        self.beam = beam
        self.group: h5py.Group = self.file[self.beam]

        if not isinstance(self.group, h5py.Group):
            raise ValueError(f"Beam {beam} not found in file {self.file_path}")

        # Load beam data into memory
        self.data: BeamData = CachedBeam._load_group(self.group)

    @staticmethod
    def _load_group(group: h5py.Group) -> BeamData:
        """Recursively loads nested group data into a dictionary."""
        data = {}
        for key in group:
            if isinstance(group[key], h5py.Group):
                # Recursively load sub-group into nested dictionary
                data[key] = CachedBeam._load_group(group[key])
            elif isinstance(group[key], h5py.Dataset):
                # Load dataset into dictionary as numpy array
                data[key] = group[key][()]
            else:
                raise TypeError(f"Expected group or dataset, got {type(group[key])}")
        return data

    def __repr__(self) -> str:
        return f"CachedBeam({self.file_path}, {self.beam})"

    def __str__(self) -> str:
        return f"CachedBeam({self.file_path}, {self.beam})"

    def __getitem__(self, key: str) -> Union[np.ndarray, "BeamData"]:
        return self.data[key]

    def __setitem__(self, key: str, value: Union[np.ndarray, "BeamData"]) -> None:
        raise NotImplementedError("CachedBeam is read-only")

    def items(self) -> BeamData:
        return self.data.items()

    def keys(self) -> BeamData:
        return self.data.keys()
