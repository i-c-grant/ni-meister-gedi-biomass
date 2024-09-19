from dataclasses import dataclass
from typing import Dict, Union

import h5py
import numpy as np
from numpy.typing import ArrayLike

# Recursive type alias for nested dictionary of numpy arrays
BeamData = Dict[str, Union[np.ndarray, "BeamData"]]


@dataclass
class Beam:
    """
    Loads a beam h5py.Group from an L1B and L2A file and caches it in memory.

    Attributes
    ----------
    file: h5py.File
        The file containing the beam data.

    beam: str
        The name of the beam.

    cache: bool
        Whether to cache the beam data in memory.
    """

    file: h5py.File
    beam: str
    cache: bool = False

    def __post_init__(self) -> None:
        self._path = self.file.filename
        self._group = self.file[self.beam]

        # Load beam into memory if caching is enabled;
        # otherwise, access data directly from h5py.Group
        if self.cache:
            self.data = Beam._load_group(self._group)
        else:
            self.data = self._group

    @staticmethod
    def _load_group(group: h5py.Group) -> BeamData:
        """Recursively loads nested group data into a dictionary."""
        data = {}
        for key in group:
            if isinstance(group[key], h5py.Group):
                # Recursively load sub-group into nested dictionary
                data[key] = Beam._load_group(group[key])
            elif isinstance(group[key], h5py.Dataset):
                # Load dataset into dictionary as numpy array
                data[key] = group[key][()]
            else:
                raise TypeError(
                    f"Expected group or dataset, got {type(group[key])}"
                )
        return data

    def extract_dataset(self, path: str) -> ArrayLike:
        """Extracts a full dataset from the beam data at the given path."""
        keys = path.split("/")
        data = self.data
        for key in keys:
            data = data[key]
        if not isinstance(data, (np.ndarray, h5py.Dataset)):
            raise TypeError(f"Expected ArrayLike, got {type(data)}")
        return data

    def extract_value(self, path: str, index: int) -> float:
        """Extracts a value from the beam data at the given path and index."""
        data = self.extract_dataset(path)
        return data[index]

    def get_beam_name(self) -> str:
        """Returns the name of the beam."""
        return self.beam

    def get_path(self) -> str:
        """Returns the path to the file containing the beam data."""
        return self._path

    def where_shot(self, shot_number: int) -> int:
        """Returns the index of the shot number in the beam data."""
        # Ensure same data type for comparison
        shot_numbers = self.extract_dataset("shot_number")
        shot_number = np.asarray(shot_number, dtype=shot_numbers.dtype)
        return np.where(shot_numbers == shot_number)[0][0]

    def __repr__(self) -> str:
        return (f"Beam(file={self._path}, beam={self.beam}, "
                f"cache={self.cache})")
