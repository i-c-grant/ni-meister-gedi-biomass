from copy import deepcopy
from typing import Any, Dict, Literal, Optional, Set
from datetime import datetime, timedelta

import h5py
from numpy.typing import ArrayLike
from shapely.geometry import Point

from nmbim.Beam import Beam
from nmbim.NestedDict import NestedDict


class Waveform:
    """Stores raw and processed waveform data for one GEDI footprint.

    Requires either 1) a shot number and h5py file objects for the L1B
    and L2A files or 2) Beam objects for the beam groups in those files.
    Provide Beam objects with caching enabled for fast batch
    processing. Provide file paths to lazily look up single Waveforms.

    Data is stored in a nested dictionary structure with string paths,
    organized in four categories: raw, processed, results, and metadata.

    Methods:
    - get_paths(): Returns a set of terminal paths in the Waveform object.
    - get_data(path: str): Returns the data stored at the given path.
    - save_data(data: Any, path: str): Saves data to the given path
    """

    def __init__(
        self,
        shot_number: int,
        l1b_beam: Optional[Beam] = None,
        l2a_beam: Optional[Beam] = None,
        l1b: Optional[h5py.File] = None,
        l2a: Optional[h5py.File] = None,
        immutable: bool = True,
    ) -> None:
        """Initializes the Waveform object. In addition to a shot number,
        requires either two Beam objects or two h5py file objects.

        If immutable is True, data will be deepcopied when retrieved.
        """
        signature = Waveform._validate_signature(init_args=locals().copy())

        # Initialize data dictionary
        self.immutable: bool = immutable
        self._data: NestedDict = NestedDict()

        # Store shot number
        self.save_data(data=shot_number, path="metadata/shot_number")

        if signature == "beams":
            # Store beams
            self.l1b_beam = l1b_beam
            self.l2a_beam = l2a_beam

            # Store beam name
            l1b_beam_name = l1b_beam.get_beam_name()
            l2a_beam_name = l2a_beam.get_beam_name()
            if l1b_beam_name != l2a_beam_name:
                raise ValueError(
                    f"Beam mismatch: L1B beam {l1b_beam_name} "
                    f"!= L2A beam {l2a_beam_name}"
                )
            self.save_data(data=l1b_beam_name, path="metadata/beam")

            # Get file paths and store
            self.save_data(data=l1b_beam.get_path(), path="metadata/l1b_path")
            self.save_data(data=l2a_beam.get_path(), path="metadata/l2a_path")

        if signature == "files":
            # Geta and store file paths
            l1b_path = l1b.filename
            l2a_path = l2a.filename
            self.save_data(data=l1b_path, path="metadata/l1b_path")
            self.save_data(data=l2a_path, path="metadata/l2a_path")

            # Store beam name
            beam_name_l1b = Waveform._which_beam(shot_number, file=l1b)
            beam_name_l2a = Waveform._which_beam(shot_number, file=l2a)
            if beam_name_l1b != beam_name_l2a:
                raise ValueError(
                    f"Beam mismatch: L1B beam {beam_name_l1b} "
                    f"!= L2A beam {beam_name_l2a}"
                )
            beam_name = beam_name_l1b
            self.save_data(data=beam_name, path="metadata/beam")

            # Create beams and store
            self.l1b_beam = Beam(file=l1b, beam=beam_name, cache=False)
            self.l2a_beam = Beam(file=l2a, beam=beam_name, cache=False)

        # Store shot index
        l1b_index = self.l1b_beam.where_shot(shot_number)
        l2a_index = self.l2a_beam.where_shot(shot_number)
        if l1b_index == l2a_index:
            shot_index = l1b_index
            self.save_data(data=shot_index, path="metadata/shot_index")
        else:
            raise ValueError(
                f"File mismatch: L1B shot index {l1b_index} != L2A "
                f"shot index {l2a_index}"
            )

        # Store coordinates
        lat = self.l1b_beam.extract_value(
            "geolocation/latitude_bin0", shot_index
        )

        lon = self.l1b_beam.extract_value(
            "geolocation/longitude_bin0", shot_index
        )

        self.save_data(
            data={"lat": lat, "lon": lon},
            path="metadata/coords",
        )

        # Store point geometry
        self.save_data(
            data=Point(lon, lat),
            path="metadata/point_geom",
        )
        
        # Extract and store GPS time
        # Note: master_time_epoch is an offset from GPS epoch
        # (1980-01-06) and delta_time is the shot's time since
        # master_time_epoch in seconds.
        master_time_epoch = self.l1b_beam.extract_value(
            "ancillary/master_time_epoch", 0
        )
        delta_time = self.l1b_beam.extract_value(
            "geolocation/delta_time", shot_index
        )
        gps_timedelta = master_time_epoch + delta_time
        self.save_data(data=gps_timedelta, path="metadata/gps_timedelta")

        # Store quality flags
        qual_flag = self.l2a_beam.extract_value("quality_flag", shot_index)
        surf_flag = self.l2a_beam.extract_value("surface_flag", shot_index)
        flag_dict = {"quality": qual_flag, "surface": surf_flag}
        self.save_data(data=flag_dict, path="metadata/flags")

        # Store number of detected modes
        self.save_data(
            data={
                "num_modes": self.l2a_beam.extract_value(
                    "num_detectedmodes", shot_index
                )
            },
            path="metadata/modes",
        )

        # Store land cover data
        self.save_data(
            data={
                "modis_nonvegetated": self.l2a_beam.extract_value(
                    "land_cover_data/modis_nonvegetated", shot_index
                ),
                "modis_treecover": self.l2a_beam.extract_value(
                    "land_cover_data/modis_treecover", shot_index
                ),
                "landsat_treecover": self.l2a_beam.extract_value(
                    "land_cover_data/landsat_treecover", shot_index
                ),
            },
            path="metadata/landcover",
        )

        # Store raw waveform data from L1B beam by subsetting the waveform
        # (subtract 1 from start index to convert to 0-based indexing)
        all_wfs: ArrayLike = self.l1b_beam.extract_dataset("rxwaveform")
        wf_start: int = (
            self.l1b_beam.extract_value("rx_sample_start_index", shot_index)
            - 1
        )
        wf_len: int = self.l1b_beam.extract_value(
            "rx_sample_count", shot_index
        )
        wf = all_wfs[wf_start : wf_start + wf_len]
        self.save_data(data=wf, path="raw/wf")

        # Store mean noise value
        self.save_data(
            data=self.l1b_beam.extract_value(
                "noise_mean_corrected", shot_index
            ),
            path="raw/mean_noise",
        )

        # Store relative height
        self.save_data(
            data=self.l2a_beam.extract_value("rh", shot_index), path="raw/rh"
        )

        # Store elevation
        self.save_data(
            data={
                "top": self.l1b_beam.extract_value(
                    "geolocation/elevation_bin0", shot_index
                ),
                "bottom": self.l1b_beam.extract_value(
                    "geolocation/elevation_lastbin", shot_index
                ),
                "ground": self.l2a_beam.extract_value(
                    "elev_lowestmode", shot_index
                ),
            },
            path="raw/elev",
        )

    @staticmethod
    def _validate_signature(
        init_args: Dict[str, Any],
    ) -> Literal["beams", "files"]:
        # Verify that the initialization signature is valid
        # (either two Beam objects or two file paths)
        # and return the signature type

        # Define helper functions to check for None values
        def any_none(*args: Any) -> bool:
            return any(arg is None for arg in args)

        def all_none(*args: Any) -> bool:
            return all(arg is None for arg in args)

        l1b_beam = init_args["l1b_beam"]
        l2a_beam = init_args["l2a_beam"]
        l1b = init_args["l1b"]
        l2a = init_args["l2a"]

        # Two signatures are valid (both must include a shot number):
        # 1) "beams": two Beam objects and no h5py files
        if not any_none(l1b_beam, l2a_beam) and all_none(l1b, l2a):
            # Type check for Beam objects
            if isinstance(l1b_beam, Beam) and isinstance(l2a_beam, Beam):
                signature = "beams"
            else:
                raise TypeError(
                    "Type mismatch: l1b_beam and l2a_beam must be Beam objects."
                )

        # or 2) "files": two h5py files and no Beam objects
        elif all_none(l1b_beam, l2a_beam) and not any_none(l1b, l2a):
            # Type check for h5py files
            if isinstance(l1b, h5py.File) and isinstance(l2a, h5py.File):
                signature = "files"
            else:
                raise TypeError(
                    "Type mismatch: l1b and l2a must be h5py.File objects."
                )
        # Otherwise, the signature is invalid
        else:
            raise ValueError(
                "Invalid Waveform initialization signature. "
                "Provide either 1) two Beam objects or 2) two file paths, "
                "but not a mix of both."
            )
        return signature

    def get_paths(self) -> Set[str]:
        """Returns a set of terminal paths in the Waveform object."""
        return self._data.get_paths()

    def get_data(self, path: str) -> Any:
        """Returns the data stored at the given path."""
        data = self._data.get_data(path)
        if self.immutable:
            data = deepcopy(data)
        return data

    def save_data(self, data: Any, path: str) -> None:
        """Saves data to the given path in the Waveform object. If a
        dictionary is passed, its keys will be saved as sub-entries
        under the specified path."""
        valid_first_keys = ["raw", "processed", "results", "metadata"]
        if path.split("/")[0] not in valid_first_keys:
            raise ValueError(
                "Invalid path provided. Must start with 'raw',"
                "'processed', 'results', or 'metadata'"
            )

        self._data.save_data(data, path, overwrite=False)

    def get_time(self) -> datetime:
        """Returns a datetime representing the GPS time of the waveform."""
        gps_epoch = datetime(1980, 1, 6)
        time_delta = timedelta(
            seconds=self.get_data("metadata/gps_timedelta"))
        gps_time = gps_epoch + time_delta
        return gps_time
        
    @staticmethod
    def _which_beam(shot_number: int, file: h5py.File) -> Optional[str]:
        """Determine which beam a waveform belongs to"""
        for key in file.keys():
            if key != "METADATA":
                beam = key
                shot_numbers = file[beam]["shot_number"]
                if shot_number in shot_numbers:
                    return beam
        return None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Waveform):
            return False

        my_shot_number = self.get_data("metadata/shot_number")
        other_shot_number = other.get_data("metadata/shot_number")
        my_beam = self.get_data("metadata/beam")
        other_beam = other.get_data("metadata/beam")

        is_equal = (
            my_shot_number == other_shot_number and my_beam == other_beam
        )

        return is_equal

    def __hash__(self) -> int:
        beam = self.get_data("metadata/beam")
        shot_number = self.get_data("metadata/shot_number")
        return hash((beam, shot_number))

    def __str__(self) -> str:
        return f"Waveform {self.get_data('metadata/shot_number')}"

    def __repr__(self) -> str:
        rep = (
            f"Waveform(shot_number={self.get_data('metadata/shot_number')}, "
            f"Beam: {self.get_data('metadata/beam')}, "
            f"Coords: {self.get_data('metadata/coords')}, "
            f"Paths: {sorted(self.get_paths())}"
        )
        return rep
