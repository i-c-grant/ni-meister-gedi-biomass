"""
Utility functions for creating test rasters for GEDI parameter testing.
"""

import numpy as np
import rasterio
from pathlib import Path
from typing import Optional

def create_global_raster(
    output_path: Path,
    value: float,
    resolution: float = 0.5,
    dtype: str = "float32",
    nodata: Optional[float] = None
) -> None:
    """Create a global raster with a single value.
    
    Args:
        output_path: Path to save the raster
        value: Value to fill the raster with
        resolution: Resolution in degrees
        dtype: Data type for the raster
        nodata: No data value (optional)
    """
    # Always create a raster with one cell
    height = 1
    width = 1
    
    # Create transform
    transform = rasterio.transform.from_bounds(
        -180.0,  # left
        -90.0,   # bottom
        180.0,   # right
        90.0,    # top
        width,
        height
    )
    
    # Create data array filled with the value
    data = np.full((height, width), value, dtype=dtype)
    
    # Create directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write to file
    with rasterio.open(
        output_path,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=1,
        dtype=dtype,
        crs="EPSG:4326",
        transform=transform,
        nodata=nodata
    ) as dst:
        dst.write(data, 1)
    
    print(f"Created global raster at {output_path} with value {value}")
    return rasterio.open(output_path, "r")

def halve_raster(raster_path: Path, output_path: Path) -> None:
    """Halve the raster horizontally and save the left half to the specified output path.
    
    Args:
        raster_path: Path to the source raster file.
        output_path: The path where the halved raster will be saved.
    """
    import rasterio
    import numpy as np
    with rasterio.open(raster_path) as src:
        profile = src.profile
        data = src.read(1)
        height, width = data.shape
        half_width = width // 2
        halved_data = data[:, :half_width]
        # Update profile for new width
        profile.update({"width": half_width})
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(output_path, "w", **profile) as dst:
        dst.write(halved_data, 1)
    print(f"Created halved raster from {raster_path} at {output_path}")

def create_boundary_raster(
    output_path: Path,
    value: float,
    resolution: float,
    boundary_path: Path,
    dtype: str = "float32",
    nodata: Optional[float] = None
) -> None:
    """Create a raster based on a geographic boundary provided in a geopackage file.
    
    The resulting raster is filled with the given value across the extent of the boundary.
    
    Args:
        output_path: Path where the raster will be saved.
        value: The value to fill the raster.
        resolution: The resolution (cell size) in the units of the boundary's CRS.
        boundary_path: Path to the geopackage file containing the boundary geometry.
        dtype: The data type of the raster array.
        nodata: The no-data value for the raster.
    """
    import geopandas as gpd
    import rasterio
    import numpy as np
    boundary = gpd.read_file(boundary_path)
    minx, miny, maxx, maxy = boundary.total_bounds
    width = int((maxx - minx) / resolution)
    height = int((maxy - miny) / resolution)
    if width <= 0 or height <= 0:
         raise ValueError(f"Calculated raster dimensions are invalid (width={width}, height={height}). Check boundary file and resolution.")
    transform = rasterio.transform.from_bounds(minx, miny, maxx, maxy, width, height)
    data = np.full((height, width), value, dtype=dtype)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        output_path,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=1,
        dtype=dtype,
        crs=boundary.crs.to_string() if boundary.crs else "EPSG:4326",
        transform=transform,
        nodata=nodata
    ) as dst:
        dst.write(data, 1)
    print(f"Created boundary raster at {output_path} with value {value}")

def create_random_na_raster(input_path: Path, output_path: Path, na_probability: float = 0.1) -> None:
    """Create a new raster with random no-data values inserted.

    Reads the input raster and randomly sets cells to no-data with the specified probability,
    then saves the result to output_path.
    
    Args:
        input_path: Path of the source raster file.
        output_path: Path to save the modified raster with random NAs.
        na_probability: Probability of setting a cell to no-data.
    """
    import rasterio
    import numpy as np
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(input_path) as src:
        profile = src.profile
        data = src.read(1)
    mask = np.random.rand(*data.shape) < na_probability
    data = data.astype('float32')
    nodata_val = profile.get('nodata')
    if nodata_val is None:
         nodata_val = -9999
         profile['nodata'] = nodata_val
    data[mask] = nodata_val
    with rasterio.open(output_path, 'w', **profile) as dst:
        dst.write(data, 1)
    print(f"Created NA raster at {output_path} from {input_path}")
