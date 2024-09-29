######################################################################
# This module defines filters that determine which waveforms are     #
# processed. A filter is a function that takes a Waveform object as  #
# input and returns a boolean value. The filters can be combined to  #
# create a list of filters that are applied to each waveform. A      #
# WaveformCollection gets such a list of filters and uses it to      #
# determine which waveforms to include.                              #
######################################################################

from datetime import datetime
from typing import Callable, List, Optional
import os

import geopandas as gpd
from osgeo import ogr
from shapely.geometry import MultiPolygon, Point, Polygon

from nmbim.Waveform import Waveform


# Quality control filters
def flag_filter(wf: Waveform) -> bool:
    """Filter waveforms based on metadata or data quality."""
    if wf.get_data("metadata/flags/quality") == 1:
        return True
    else:
        return False

def modes_filter(wf: Waveform) -> bool:
    """Keep only waveforms with more than one mode."""
    if wf.get_data("metadata/modes/num_modes") > 0:
        return True
    else:
        return False

def landcover_filter(wf: Waveform) -> bool:
    """Keep only waveforms with more than 50% tree cover."""
    if wf.get_data("metadata/landcover/modis_treecover") > 10:
        return True
    else:
        return False

def generate_spatial_filter(file_path: str, 
                            waveform_crs: str = "EPSG:4326") -> Callable:
    """Generate a spatial filter based on a polygon from a GeoPackage or 
       Shapefile. File must contain only polygons and one layer."""

    # Resolve the file path
    file_path = os.path.realpath(file_path)

    # Check layer count (for formats with multiple layers like GPKG)
    with ogr.Open(file_path) as boundary_data:
        n_layers = boundary_data.GetLayerCount()
        if n_layers > 1:
            raise ValueError(f"The boundary file contains multiple layers: ",
                             f"{layers}. Please provide a file with a ",
                             f"single layer.")
    
    # Read the polygons from the file (only the first layer)
    poly_gdf = gpd.read_file(file_path)
    
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
        point_gdf = gpd.GeoSeries([wf.as_point()], crs=waveform_crs)
        point_gdf = point_gdf.to_crs(poly_crs)
        return poly_gdf.contains(point_gdf.iloc[0]).any()

    return spatial_filter

def generate_temporal_filter(time_start: datetime,
                         time_end: datetime) -> Callable:
    """Generate a time filter based on a start and end time."""
    
    def temporal_filter(wf: 'Waveform') -> bool:
        # Extract waveform time
        # wf_time = datetime.strptime(wf.get_data("metadata/datetime"),
                                    # "%Y-%m-%dT%H:%M:%S")
        
        # Check if the waveform time is within the specified time range
        if time_start <= wf_time <= time_end:
            return True
        return False

    return temporal_filter

def define_filters(poly_file: Optional[str] = None,
                   time_start: Optional[datetime] = None,
                   time_end: Optional[datetime] = None) -> List[Callable]:
    """Define filters that determine which waveforms are processed"""

    # Invariant filters
    filters = [flag_filter,
               modes_filter,
               landcover_filter]

    # Dynamic filters
    if poly_file:
        spatial_filter = generate_spatial_filter(poly_file)
        filters.append(spatial_filter)

    if time_start and time_end:
        temporal_filter = generate_temporal_filter(time_start, time_end)
        filters.append(temporal_filter)

    return filters
