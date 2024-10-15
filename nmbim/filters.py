######################################################################
# This module defines filters that determine which waveforms are     #
# processed. A filter is a function that takes a Waveform object as  #
# input and returns a boolean value. The filters can be combined to  #
# create a list of filters that are applied to each waveform. A      #
# WaveformCollection gets such a list of filters and uses it to      #
# determine which waveforms to include.                              #
######################################################################

import warnings
from datetime import datetime
from typing import Callable, Optional, Tuple, Dict, Any
import os

import geopandas as gpd
import numpy as np

from nmbim.Waveform import Waveform

DateInterval = Tuple[Optional[datetime], Optional[datetime]]


def parse_date_range(date_range: str) -> DateInterval:
    """Parse a date range string into a start and end date."""
    time_start, time_end = None, None
    dates = date_range.split(",")
    if len(dates) > 2:
        raise ValueError(
            "Invalid date range. Please provide a single "
            "date, a date range, or a start and end date."
        )
    if len(dates) == 1:
        warnings.warn(
            "Only one date provided. This will be treated as "
            "a start date. Using a leading or trailing comma "
            "to specify how a single date should be handled."
        )
        dates.append(None)

    date_spec = "%Y-%m-%dT%H:%M:%SZ"
    if dates[0]:
        time_start: datetime = datetime.strptime(dates[0], date_spec)
    if dates[1]:
        time_end: datetime = datetime.strptime(dates[1], date_spec)

    if time_start and time_end and time_start > time_end:
        raise ValueError("The start date must be before the end date.")

    return time_start, time_end


# Filter generators
def generate_temporal_filter(
    time_start: Optional[str], time_end: Optional[str]
) -> Callable:
    """Generate a temporal filter based on start, end time, or both."""
    date_spec = "%Y-%m-%dT%H:%M:%SZ"
    start = datetime.strptime(time_start, date_spec) if time_start else None
    end = datetime.strptime(time_end, date_spec) if time_end else None

    def temporal_filter(wf: "Waveform") -> bool:
        wf_time = wf.get_data("metadata/time")
        after_start = start is None or wf_time >= start
        before_end = end is None or wf_time <= end
        return after_start and before_end

    return temporal_filter


def generate_flag_filter() -> Callable:
    """Generate a filter based on metadata or data quality."""

    def flag_filter(wf: Waveform) -> bool:
        return wf.get_data("metadata/flags/l4_quality") == 1

    return flag_filter


def generate_modes_filter(min_modes) -> Callable:
    """Generate a filter to keep only waveforms with more than one mode."""

    def modes_filter(wf: Waveform) -> bool:
        return wf.get_data("metadata/modes/num_modes") >= min_modes

    return modes_filter


def generate_landcover_filter(min_treecover) -> Callable:
    """Generate a filter to keep only waveforms with more than 50% tree cover."""

    def landcover_filter(wf: Waveform) -> bool:
        return wf.get_data("metadata/landcover/modis_treecover") >= min_treecover

    return landcover_filter


def generate_spatial_filter(
    file_path: str, waveform_crs: str = "EPSG:4326"
) -> Callable:
    """Generate a spatial filter based on a polygon layer."""
    file_path = os.path.realpath(file_path)
    poly_gdf = gpd.read_file(file_path)

    if poly_gdf is None:
        raise ValueError(f"The polygon file at {file_path} could not be read.")

    if not poly_gdf.geom_type.isin(["Polygon", "MultiPolygon"]).all():
        raise ValueError(
            "The file contains non-polygon geometries. Ensure all geometries are polygons."
        )

    poly_crs = poly_gdf.crs
    if poly_crs is None:
        raise ValueError("The polygon file does not have a CRS specified.")

    def spatial_filter(wf: "Waveform") -> bool:
        wf_point = wf.get_data("metadata/point_geom")
        point_gdf = gpd.GeoSeries([wf_point], crs=waveform_crs)
        point_gdf = point_gdf.to_crs(poly_crs)
        return poly_gdf.contains(point_gdf.iloc[0]).any()

    return spatial_filter

def generate_plausible_ground_filter(window_start: float, window_end: float) -> Callable:
    """Generate a filter to keep only waveforms with a plausible
    ground return.

    Plausible ground returns are defined as lowest modes within a
    specified relative distance window between the top and bottom of
    the waveform. This filters out waveforms with lowest modes that
    are too close to the top or bottom of the waveform to plausibly
    represent ground returns. """

    if not 0 <= window_start < window_end <= 1:
        raise ValueError("Window start and end must be between 0 and 1, with start < end.")

    def plausible_ground_filter(wf: Waveform) -> bool:
        ground: float = wf.get_data("raw/elev/ground")
        top: float = wf.get_data("raw/elev/top")
        bottom: float = wf.get_data("raw/elev/bottom")

        if ground is None or top is None or bottom is None:
            return False

        wf_height = top - bottom
        
        ground_relative_height = (ground - bottom) / wf_height
        return window_start < ground_relative_height < window_end

    return plausible_ground_filter

def generate_ground_to_top_filter(min_height: float) -> Callable:
    """
    Generate a filter to keep only waveforms where the difference between
    the ground and top of the waveform is greater than a specified minimum height.

    Parameters
    ----------
    min_height : float
        The minimum height difference between ground and top of the waveform.

    Returns
    -------
    Callable
        A filter function that takes a Waveform object and returns a boolean.
    """
    def ground_to_top_filter(wf: Waveform) -> bool:
        ground: float = wf.get_data("raw/elev/ground")
        top: float = wf.get_data("raw/elev/top")

        if ground is None or top is None:
            return False

        return (top - ground) > min_height

    return ground_to_top_filter

def get_filter_generators() -> Dict[str, Callable]:
    """Get a dictionary of filter generators."""
    return {
        "temporal": generate_temporal_filter,
        "flag": generate_flag_filter,
        "modes": generate_modes_filter,
        "landcover": generate_landcover_filter,
        "spatial": generate_spatial_filter,
        "plausible_ground": generate_plausible_ground_filter,
        "ground_to_top": generate_ground_to_top_filter,
    }


def generate_filters(filter_config: Dict[str, Dict[str, Any]]) -> Dict[str, Optional[Callable]]:
    """Generate a dictionary of filters based on a configuration dictionary."""
    generators = get_filter_generators()
    filters = {}

    for filter_name, filter_kwargs in filter_config.items():
        if filter_name in generators:
            if isinstance(filter_kwargs, dict):
                filters[filter_name] = generators[filter_name](**filter_kwargs)
            elif filter_kwargs is None:
                filters[filter_name] = None
            else:
                raise ValueError(
                    f"Invalid configuration for filter '{filter_name}'. "
                    f"Expected a dictionary or None, got {type(filter_kwargs)}"
                )
        else:
            filters[filter_name] = None
            warnings.warn(
                f"Filter '{filter_name}' specified in configuration "
                f"but no matching generator was defined. This filter "
                f"will be ignored."
            )

    return filters

def get_registered_filters() -> Dict[str, Dict[str, Any]]:
    """
    Get a dictionary of all registered filters and their configurations.
    
    Returns:
        Dict[str, Dict[str, Any]]: The dictionary of registered filters and their configurations.
    """
    return {name: {} for name in get_filter_generators().keys()}
