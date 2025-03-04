import h5py
import numpy as np
from typing import Dict, Union, Any, Optional
from numpy.typing import ArrayLike

# Recursive type alias for nested dictionary of numpy arrays
LVISData = Dict[str, Union[np.ndarray, "LVISData"]]

class LVISCacheL1:
    _cache = {}

    @classmethod
    def load(cls, filepath: str) -> LVISData:
        """Load the LVIS L1 HDF5 file and cache it for future calls.
        
        Returns a nested dictionary structure of the HDF5 file contents.
        """
        if filepath in cls._cache:
            return cls._cache[filepath]

        with h5py.File(filepath, "r") as f:
            # Recursively load the HDF5 file into a nested dictionary
            data = cls._load_group(f)
        
        cls._cache[filepath] = data
        return data
    
    @classmethod
    def _load_group(cls, group: h5py.Group) -> LVISData:
        """Recursively loads nested group data into a dictionary."""
        data = {}
        for key in group:
            if isinstance(group[key], h5py.Group):
                # Recursively load sub-group into nested dictionary
                data[key] = cls._load_group(group[key])
            elif isinstance(group[key], h5py.Dataset):
                # Load dataset into dictionary as numpy array
                data[key] = group[key][()]
            else:
                raise TypeError(
                    f"Expected group or dataset, got {type(group[key])}"
                )
        return data
        
    def __init__(self, filepath: str):
        """Initialize with the path to the L1B HDF5 file.
        
        Args:
            filepath: Path to the L1B HDF5 file
        """
        self.filepath = filepath
        # Ensure the file is loaded into the cache
        self.load(filepath)
        
    def extract_dataset(self, path: str) -> ArrayLike:
        """Extract a full dataset from the cached data.
        
        Args:
            path: Path to the dataset, with components separated by '/'
            
        Returns:
            The entire dataset for the specified path
        """
        data = self.__class__.load(self.filepath)
        keys = path.split("/")
        for key in keys:
            data = data[key]
        if not isinstance(data, np.ndarray):
            raise TypeError(f"Expected numpy array, got {type(data)}")
        return data
        
    def extract_value(self, path: str, index: int) -> float:
        """Extract a single value from the cached data.
        
        Args:
            path: Path to the dataset, with components separated by '/'
            index: Index of the value to extract
            
        Returns:
            The value at the specified path and index
        """
        dataset = self.extract_dataset(path)
        return dataset[index]
        
    def where_shot(self, shot_number: int) -> int:
        """Find the index of a shot number in the cached data.
        
        Args:
            shot_number: Shot number to find
            
        Returns:
            Index of the shot number, or raises ValueError if not found
        """
        shot_numbers = self.extract_dataset("SHOTNUMBER")
        indices = np.where(shot_numbers == shot_number)[0]
        if len(indices) == 0:
            raise ValueError(f"Shot number {shot_number} not found in {self.filepath}")
        return indices[0]
