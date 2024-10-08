######################################################################
# This module defines filters that determine which waveforms are     #
# processed. A filter is a function that takes a Waveform object as  #
# input and returns a boolean value. The filters can be combined to  #
# create a list of filters that are applied to each waveform. A      #
# WaveformCollection gets such a list of filters and uses it to      #
# determine which waveforms to include.                              #
######################################################################

from datetime import datetime
from typing import Callable, List, Optional, Tuple
import os

import geopandas as gpd
from shapely.geometry import MultiPolygon, Point, Polygon

from nmbim.Waveform import Waveform

DateInterval = Tuple[Optional[datetime], Optional[datetime]]
def parse_date_range(date_range: str) -> DateInterval:
    """Parse a date range string into a start and end date."""
    time_start, time_end = None, None
    dates = date_range.split(",")
    if len(dates) > 2:
        raise ValueError("Invalid date range. Please provide a single "
                         "date, a date range, or a start and end date.")
    if len(dates) == 1:
        warnings.warn("Only one date provided. This will be treated as "
                      "a start date. Using a leading or trailing comma "
                      "to specify how a single date should be handled.")
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
def generate_temporal_filter(time_start: Optional[datetime], 
                             time_end: Optional[datetime]) -> Callable:
    """Generate a temporal filter based on start, end time, or both."""

    def temporal_filter(wf: 'Waveform') -> bool:
        # Extract waveform time
        wf_time = wf.get_data('metadata/time')
        
        # Check if the waveform time is within the specified time range
        after_start, before_end = True, True
        if time_start and wf_time < time_start:
            after_start = False

        if time_end and wf_time > time_end:
            before_end = False

        return after_start and before_end

    return temporal_filter


# Quality control filters
def generate_flag_filter() -> Callable:
    """Generate a filter based on metadata or data quality."""
    def flag_filter(wf: Waveform) -> bool:
        if wf.get_data("metadata/flags/quality") == 1:
            return True
        else:
            return False
    return flag_filter

def generate_modes_filter() -> Callable:
    """Generate a filter to keep only waveforms with more than one mode."""
    def modes_filter(wf: Waveform) -> bool:
        if wf.get_data("metadata/modes/num_modes") > 0:
            return True
        else:
            return False
    return modes_filter

def generate_landcover_filter() -> Callable:
    """Generate a filter to keep only waveforms with more than 50% tree cover."""
    def landcover_filter(wf: Waveform) -> bool:
        if wf.get_data("metadata/landcover/modis_treecover") > 10:
            return True
        else:
            return False
    return landcover_filter

def generate_spatial_filter(file_path: str, 
                            waveform_crs: str = "EPSG:4326") -> Callable:
    """Generate a spatial filter based on a polygon layer.

    Acceptable formats are GeoPackage and Shapefile. File must contain
    only polygons and are assumed to contain a single layer."""

    # Resolve the file path
    file_path = os.path.realpath(file_path)

    # Read the polygons from the file (only the first layer)
    poly_gdf = gpd.read_file(file_path)

    if poly_gdf is None:
        raise ValueError("The polygon file at {file_path} "
                         "could not be read.")
    
    # Ensure the geometry type is Polygon or MultiPolygon
    if not poly_gdf.geom_type.isin(["Polygon", "MultiPolygon"]).all():
        raise ValueError("The file contains non-polygon geometries. "
                         "Ensure all geometries are polygons.")
   
    # Get the CRS of the polygon file and check if it is specified
    poly_crs = poly_gdf.crs
    if poly_crs is None:
        raise ValueError("The polygon file does not have a CRS specified.")

    # Define the spatial filter
    def spatial_filter(wf: 'Waveform') -> bool:
        wf_point = wf.get_data("metadata/point_geom")
        point_gdf = gpd.GeoSeries([wf_point], crs=waveform_crs)
        point_gdf = point_gdf.to_crs(poly_crs)
        return poly_gdf.contains(point_gdf.iloc[0]).any()

    return spatial_filter


def define_filters(poly_file: Optional[str] = None,
                   time_start: Optional[datetime] = None,
                   time_end: Optional[datetime] = None) -> List[Callable]:
    """Define filters that determine which waveforms are processed"""

    # Invariant filters
    filters = [generate_flag_filter(),
               generate_modes_filter(),
               generate_landcover_filter()]

    # Dynamic filters
    if poly_file:
        spatial_filter = generate_spatial_filter(poly_file)
        filters.append(spatial_filter)

    if time_start and time_end:
        temporal_filter = generate_temporal_filter(time_start, time_end)
        filters.append(temporal_filter)

    return filters
