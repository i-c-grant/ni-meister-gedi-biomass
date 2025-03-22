from copy import deepcopy
from typing import Any, Dict, Literal, Optional, Set
from datetime import datetime

import numpy as np
from numpy.typing import ArrayLike
from shapely.geometry import Point

from nmbim.NestedDict import NestedDict
from nmbim.LVISCacheL1 import LVISCacheL1
from nmbim.LVISCacheL2 import LVISCacheL2


class LVISWaveform:
    """Stores raw and processed waveform data for one LVIS footprint.

    Requires a shot number and cache objects for L1 and L2 data.
    The cache objects handle efficient loading and caching of data
    from the L1B HDF5 and L2 TXT files.

    Data is stored in a nested dictionary structure with string paths,
    organized in three categories: raw, processed, and metadata.

    Methods:
    - get_paths(): Returns a set of terminal paths in the LVISWaveform object.
    - get_data(path: str): Returns the data stored at the given path.
    - save_data(data: Any, path: str): Saves data to the given path
    """

    def __init__(
        self,
        l1_cache: LVISCacheL1,
        l2_cache: LVISCacheL2,
        shot_number: Optional[int] = None,
        cache_index: Optional[int] = None,
        immutable: bool = False,
    ) -> None:
        """Initialize with either shot_number or cache_index."""
        if (shot_number is None) == (cache_index is None):
            raise ValueError("Must provide exactly one of shot_number or cache_index")

        if cache_index is not None:
            # Validate index and get shot number
            try:
                shot_number = l1_cache.get_shot_number(cache_index)
                l2_shot_number = l2_cache.get_shot_number(cache_index)
            except IndexError:
                raise ValueError(f"Invalid cache_index: {cache_index}")

            if shot_number != l2_shot_number:
                raise ValueError(
                    f"L1/L2 shot number mismatch at index {cache_index}: "
                    f"L1={shot_number}, L2={l2_shot_number}"
                )
            shot_index = cache_index
        else:
            # Original lookup by shot_number
            shot_index = l1_cache.where_shot(shot_number)
        """Initializes the LVISWaveform object with a shot number and cache objects.
        
        Uses provided cache objects to efficiently load data.
        
        Args:
            shot_number: The shot number to extract
            l1_cache: LVISCacheL1 object to use for L1B data
            l2_cache: LVISCacheL2 object to use for L2 data
            immutable: If True, data will be deepcopied when retrieved
        """
        # Initialize data dictionary
        self.immutable: bool = immutable
        self._data: NestedDict = NestedDict()

        # Store shot number
        self.save_data(data=shot_number, path="metadata/shot_number")
        
        # Flag to track if L2 data was loaded
        self._l2_loaded = False

        # Find the index of the shot in the L1 data
        shot_index = l1_cache.where_shot(shot_number)
        self.save_data(data=shot_index, path="metadata/shot_index")
        
        # Store LFID (LVIS file identification)
        lfid = l1_cache.extract_value("LFID", shot_index)
        self.save_data(data=lfid, path="metadata/lfid")

        # Store coordinates
        lon0 = l1_cache.extract_value("LON0", shot_index)
        lat0 = l1_cache.extract_value("LAT0", shot_index)
        z0 = l1_cache.extract_value("Z0", shot_index)
        
        lon1023 = l1_cache.extract_value("LON1023", shot_index)
        lat1023 = l1_cache.extract_value("LAT1023", shot_index)
        z1023 = l1_cache.extract_value("Z1023", shot_index)
        
        # Store top coordinates (highest sample of the waveform)
        self.save_data(
            data={"lon": lon0, "lat": lat0, "z": z0},
            path="metadata/coords_top",
        )
        
        # Store bottom coordinates (lowest sample of the waveform)
        self.save_data(
            data={"lon": lon1023, "lat": lat1023, "z": z1023},
            path="metadata/coords_bottom",
        )
        
        # Store center coordinates (average of top and bottom)
        center_lon = (lon0 + lon1023) / 2
        center_lat = (lat0 + lat1023) / 2
        center_z = (z0 + z1023) / 2
        
        self.save_data(
            data={"lon": center_lon, "lat": center_lat, "z": center_z},
            path="metadata/coords",
        )

        # Store point geometry (using center coordinates)
        self.save_data(
            data=Point(center_lon, center_lat),
            path="metadata/point_geom",
        )
        
        # Simply store TIME as a number
        time_seconds = l1_cache.extract_value("TIME", shot_index)
        self.save_data(data=time_seconds, path="metadata/time")

        # Store beam geometry
        self.save_data(data=l1_cache.extract_value("AZIMUTH", shot_index), path="metadata/azimuth")
        self.save_data(data=l1_cache.extract_value("INCIDENTANGLE", shot_index), path="metadata/incident_angle")
        self.save_data(data=l1_cache.extract_value("RANGE", shot_index), path="metadata/range")

        # Store raw waveform data
        # Transmitted waveform (128 bins)
        tx_wave = l1_cache.extract_value("TXWAVE", shot_index)
        self.save_data(data=tx_wave, path="raw/tx_wave")
        
        # Return waveform (1024 bins)
        rx_wave = l1_cache.extract_value("RXWAVE", shot_index)
        self.save_data(data=rx_wave, path="raw/rx_wave")
        
        # Store signal mean noise level
        self.save_data(
            data=l1_cache.extract_value("SIGMEAN", shot_index),
            path="raw/mean_noise",
        )
        
        # Load L2 data using the cache
        self._load_l2_data(l2_cache, lfid, shot_number)
        
    def _load_l2_data(self, l2_cache: LVISCacheL2, lfid: int, shot_number: int) -> None:
        """Load L2 data using the provided LVISCacheL2 object.
        
        The L2 data is identified by the combination of LFID and shot number.
        """
        try:
            # Store the selected RH metrics
            rh_metrics = {}
            for rh_key in ["RH50", "RH75", "RH90", "RH95", "RH100"]:
                rh_metrics[rh_key] = l2_cache.extract_value(lfid, shot_number, rh_key)
            
            self.save_data(data=rh_metrics, path="raw/rh")
            
            # Store ground elevation (zg)
            ground_elev = l2_cache.extract_value(lfid, shot_number, "ZG")
            if ground_elev is not None:
                self.save_data(data=ground_elev, path="raw/elev/ground")
            
            # Store highest elevation (zh)
            highest_elev = l2_cache.extract_value(lfid, shot_number, "ZH")
            if highest_elev is not None:
                self.save_data(data=highest_elev, path="raw/elev/highest")
            
            # Store top elevation (zt)
            top_elev = l2_cache.extract_value(lfid, shot_number, "ZT")
            if top_elev is not None:
                self.save_data(data=top_elev, path="raw/elev/top")
            
            # Mark L2 data as loaded if we got at least one value
            if any(v is not None for v in rh_metrics.values()) or ground_elev is not None or highest_elev is not None or top_elev is not None:
                self._l2_loaded = True
            else:
                print(f"Warning: No L2 data found for LFID {lfid}, shot number {shot_number}")
            
        except Exception as e:
            print(f"Error loading L2 data: {e}")

    def get_paths(self) -> Set[str]:
        """Returns a set of terminal paths in the LVISWaveform object."""
        return self._data.get_paths()

    def get_data(self, path: str) -> Any:
        """Returns the data stored at the given path."""
        data = self._data.get_data(path)
        if self.immutable:
            data = deepcopy(data)
        return data

    def save_data(self, data: Any, path: str) -> None:
        """Saves data to the given path in the LVISWaveform object. If a
        dictionary is passed, its keys will be saved as sub-entries
        under the specified path."""
        valid_first_keys = ["raw", "processed", "results", "metadata"]
        if path.split("/")[0] not in valid_first_keys:
            raise ValueError(
                "Invalid path provided. Must start with 'raw',"
                "'processed', 'results', or 'metadata'"
            )

        self._data.save_data(data, path, overwrite=False)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, LVISWaveform):
            return False

        my_shot_number = self.get_data("metadata/shot_number")
        other_shot_number = other.get_data("metadata/shot_number")
        my_lfid = self.get_data("metadata/lfid")
        other_lfid = other.get_data("metadata/lfid")

        is_equal = (
            my_shot_number == other_shot_number and my_lfid == other_lfid
        )

        return is_equal

    def __hash__(self) -> int:
        lfid = self.get_data("metadata/lfid")
        shot_number = self.get_data("metadata/shot_number")
        return hash((lfid, shot_number))

    def __str__(self) -> str:
        return f"LVISWaveform {self.get_data('metadata/shot_number')}"

    def __repr__(self) -> str:
        l2_status = "L2 loaded" if self._l2_loaded else "L2 not loaded"
        rep = (
            f"LVISWaveform(shot_number={self.get_data('metadata/shot_number')}, "
            f"LFID: {self.get_data('metadata/lfid')}, "
            f"Coords: {self.get_data('metadata/coords')}, "
            f"{l2_status}, "
            f"Paths: {sorted(self.get_paths())}"
        )
        return rep
