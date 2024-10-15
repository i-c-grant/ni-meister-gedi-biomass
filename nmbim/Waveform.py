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
    L2A, and L4A files or 2) Beam objects for the beam groups in those files.
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
        l4a_beam: Optional[Beam] = None,
        l1b: Optional[h5py.File] = None,
        l2a: Optional[h5py.File] = None,
        l4a: Optional[h5py.File] = None,
        immutable: bool = True,
    ) -> None:
        """Initializes the Waveform object. In addition to a shot number,
        requires either three Beam objects or three h5py file objects.

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
            self.l4a_beam = l4a_beam

            # Store beam name
            l1b_beam_name = l1b_beam.get_beam_name()
            l2a_beam_name = l2a_beam.get_beam_name()
            l4a_beam_name = l4a_beam.get_beam_name()
            if l1b_beam_name != l2a_beam_name or l1b_beam_name != l4a_beam_name:
                raise ValueError(
                    f"Beam mismatch: L1B beam {l1b_beam_name}, "
                    f"L2A beam {l2a_beam_name}, "
                    f"L4A beam {l4a_beam_name}"
                )
            self.save_data(data=l1b_beam_name, path="metadata/beam")

            # Get file paths and store
            self.save_data(data=l1b_beam.get_path(), path="metadata/l1b_path")
            self.save_data(data=l2a_beam.get_path(), path="metadata/l2a_path")
            self.save_data(data=l4a_beam.get_path(), path="metadata/l4a_path")

        if signature == "files":
            # Get and store file paths
            l1b_path = l1b.filename
            l2a_path = l2a.filename
            l4a_path = l4a.filename
            self.save_data(data=l1b_path, path="metadata/l1b_path")
            self.save_data(data=l2a_path, path="metadata/l2a_path")
            self.save_data(data=l4a_path, path="metadata/l4a_path")

            # Store beam name
            beam_name_l1b = Waveform._which_beam(shot_number, file=l1b)
            beam_name_l2a = Waveform._which_beam(shot_number, file=l2a)
            beam_name_l4a = Waveform._which_beam(shot_number, file=l4a)
            if beam_name_l1b != beam_name_l2a or beam_name_l1b != beam_name_l4a:
                raise ValueError(
                    f"Beam mismatch: L1B beam {beam_name_l1b}, "
                    f"L2A beam {beam_name_l2a}, "
                    f"L4A beam {beam_name_l4a}"
                )
            beam_name = beam_name_l1b
            self.save_data(data=beam_name, path="metadata/beam")

            # Create beams and store
            self.l1b_beam = Beam(file=l1b, beam=beam_name, cache=False)
            self.l2a_beam = Beam(file=l2a, beam=beam_name, cache=False)
            self.l4a_beam = Beam(file=l4a, beam=beam_name, cache=False)

        # Store shot index
        l1b_index = self.l1b_beam.where_shot(shot_number)
        l2a_index = self.l2a_beam.where_shot(shot_number)
        l4a_index = self.l4a_beam.where_shot(shot_number)
        if l1b_index == l2a_index == l4a_index:
            shot_index = l1b_index
            self.save_data(data=shot_index, path="metadata/shot_index")
        else:
            raise ValueError(
                f"File mismatch: L1B shot index {l1b_index}, "
                f"L2A shot index {l2a_index}, "
                f"L4A shot index {l4a_index}"
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
        
        # Extract and store GPS time for the waveform
        # Define the GPS epoch and calculate the GEDI epoch
        gps_epoch: datetime = datetime(1980, 1, 6)
        gedi_epoch_offset: int = self.l1b_beam.extract_value(
            "ancillary/master_time_epoch", 0
        )
        gedi_epoch: datetime = gps_epoch + timedelta(seconds=gedi_epoch_offset)

        # Calculate the waveform time
        wf_timedelta: int = self.l1b_beam.extract_value(
            "geolocation/delta_time", shot_index
        )
        wf_time: datetime = gedi_epoch + timedelta(seconds=wf_timedelta)
        self.save_data(data=wf_time, path="metadata/time")

        # Store quality flags
        l2_qual_flag = self.l2a_beam.extract_value("quality_flag",
                                                   shot_index)

        l4_qual_flag = self.l4a_beam.extract_value("l4_quality_flag",
                                                   shot_index)

        surf_flag = self.l2a_beam.extract_value("surface_flag",
                                                shot_index)
        
        degrade_flag = self.l4a_beam.extract_value("degrade_flag",
                                                   shot_index)

        leaf_off_flag = self.l4a_beam.extract_value(
            "land_cover_data/leaf_off_flag",
            shot_index)

        flag_dict = {
            "l2_quality": l2_qual_flag,
            "l4_quality": l4_qual_flag,
            "degrade": degrade_flag,
            "surface": surf_flag,
        }

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
                "urban_proportion": self.l4a_beam.extract_value(
                    "land_cover_data/urban_proportion", shot_index
                ),
                "pft": self.l4a_beam.extract_value(
                    "land_cover_data/pft_class", shot_index
                ),
                "region": self.l4a_beam.extract_value(
                    "land_cover_data/region_class", shot_index
                )
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

        # Store sensitivity
        self.save_data(
            data=self.l4a_beam.extract_value("sensitivity", shot_index),
            path="raw/sensitivity",
        )

        # Store L4A data
        self.save_data(
            data={
                "agbd": self.l4a_beam.extract_value("agbd", shot_index),
                "agbd_se": self.l4a_beam.extract_value("agbd_se", shot_index),
            },
            path="raw/l4a",
        )

    @staticmethod
    def _validate_signature(
        init_args: Dict[str, Any],
    ) -> Literal["beams", "files"]:
        # Verify that the initialization signature is valid
        # (either three Beam objects or three file paths)
        # and return the signature type

        # Define helper functions to check for None values
        def any_none(*args: Any) -> bool:
            return any(arg is None for arg in args)

        def all_none(*args: Any) -> bool:
            return all(arg is None for arg in args)

        l1b_beam = init_args["l1b_beam"]
        l2a_beam = init_args["l2a_beam"]
        l4a_beam = init_args["l4a_beam"]
        l1b = init_args["l1b"]
        l2a = init_args["l2a"]
        l4a = init_args["l4a"]

        # Two signatures are valid (both must include a shot number):
        # 1) "beams": three Beam objects and no h5py files
        if not any_none(l1b_beam, l2a_beam, l4a_beam) and all_none(l1b, l2a, l4a):
            # Type check for Beam objects
            if isinstance(l1b_beam, Beam) and isinstance(l2a_beam, Beam) and isinstance(l4a_beam, Beam):
                signature = "beams"
            else:
                raise TypeError(
                    "Type mismatch: l1b_beam, l2a_beam, and l4a_beam must be Beam objects."
                )

        # or 2) "files": three h5py files and no Beam objects
        elif all_none(l1b_beam, l2a_beam, l4a_beam) and not any_none(l1b, l2a, l4a):
            # Type check for h5py files
            if isinstance(l1b, h5py.File) and isinstance(l2a, h5py.File) and isinstance(l4a, h5py.File):
                signature = "files"
            else:
                raise TypeError(
                    "Type mismatch: l1b, l2a, and l4a must be h5py.File objects."
                )
        # Otherwise, the signature is invalid
        else:
            raise ValueError(
                "Invalid Waveform initialization signature. "
                "Provide either 1) three Beam objects or 2) three file paths, "
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
