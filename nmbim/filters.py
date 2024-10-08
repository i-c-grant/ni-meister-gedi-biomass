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
        return wf.get_data("metadata/flags/quality") == 1

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

def generate_ht_window_filter(window_start: float, window_end: float) -> Callable:
    """Generate a filter based on the position of the first positive value in the ht array."""
    if not 0 <= window_start < window_end <= 1:
        raise ValueError("Window start and end must be between 0 and 1, with start < end.")

    def ht_window_filter(wf: "Waveform") -> bool:
        ht_array = wf.get_data("raw/ht")
        first_positive_index = np.argmax(ht_array > 0)
        array_length = len(ht_array)
        relative_position = first_positive_index / array_length
        return window_start <= relative_position <= window_end

    return ht_window_filter

def get_filter_generators() -> Dict[str, Callable]:
    """Get a dictionary of filter generators."""
    return {
        "temporal": generate_temporal_filter,
        "flag": generate_flag_filter,
        "modes": generate_modes_filter,
        "landcover": generate_landcover_filter,
        "spatial": generate_spatial_filter,
        "ht_window": generate_ht_window_filter,
    }


def generate_filters(
    generators: Dict[str, Callable], config: Dict[str, Dict[str, Any]]
) -> Dict[str, Optional[Callable]]:
    """Generate a dictionary of filters based on a configuration dictionary."""
    filters = {}

    for filter_name, filter_config in config.get('filters', {}).items():
        if filter_name in generators:
            if isinstance(filter_config, dict):
                filters[filter_name] = generators[filter_name](**filter_config)
            elif filter_config is None:
                filters[filter_name] = None
            else:
                raise ValueError(
                    f"Invalid configuration for filter '{filter_name}'. "
                    f"Expected a dictionary or None, got {type(filter_config)}"
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
