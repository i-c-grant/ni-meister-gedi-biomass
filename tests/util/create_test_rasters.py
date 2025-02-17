import numpy as np
import rasterio
from pathlib import Path
from typing import Optional, Tuple
from shapely.geometry import Polygon

def create_global_raster(
    output_path: Path,
    value: float,
    dtype: str = "float32",
    nodata: Optional[float] = None
) -> None:
    """Create a single-cell global coverage raster with given value.
    
    Args:
        output_path: Path to save the raster
        value: Value to fill the raster with
        dtype: Data type for the raster
        nodata: No data value (optional)
    """
    with rasterio.open(
        output_path,
        "w",
        driver="GTiff",
        height=1,
        width=1,
        count=1,
        dtype=dtype,
        crs="EPSG:4326",
        transform=rasterio.Affine(360.0, 0.0, -180.0, 0.0, -180.0, 90.0),
        nodata=nodata
    ) as dst:
        dst.write(np.array([[value]]), 1)

def create_polygon_raster(
    output_path: Path,
    polygon: Polygon,
    value: float,
    resolution: Tuple[float, float] = (1.0, 1.0),
    dtype: str = "float32",
    nodata: Optional[float] = None
) -> None:
    """Create a raster covering a polygon with given resolution and value.
    
    Args:
        output_path: Path to save the raster
        polygon: Shapely polygon defining the area
        value: Value to fill the raster with
        resolution: Tuple of (x_resolution, y_resolution) in degrees
        dtype: Data type for the raster
        nodata: No data value (optional)
    """
    bounds = polygon.bounds
    width = int((bounds[2] - bounds[0]) / resolution[0])
    height = int((bounds[3] - bounds[1]) / resolution[1])
    
    transform = rasterio.transform.from_origin(
        west=bounds[0],
        north=bounds[3],
        xsize=resolution[0],
        ysize=resolution[1]
    )
    
    # Create mask from polygon
    data = np.full((int(height), int(width)), value, dtype=dtype)
    
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

def add_random_nodata(
    raster_path: Path,
    proportion: float,
    nodata_value: float = np.nan
) -> None:
    """Randomly set proportion of cells to nodata.
    
    Args:
        raster_path: Path to existing raster
        proportion: Proportion of cells to set to nodata (0.0-1.0)
        nodata_value: Value to use for nodata
    """
    with rasterio.open(raster_path, 'r+') as dst:
        data = dst.read(1)
        mask = np.random.random(data.shape) < proportion
        data[mask] = nodata_value
        dst.write(data, 1)

if __name__ == "__main__":
    # Create in tests/input directory
    output_dir = Path(__file__).parent/"input"
    output_dir.mkdir(exist_ok=True)
    
    # Create all test rasters
    nan_raster_path = output_dir/"nan_raster.tif"
    create_global_raster(nan_raster_path, np.nan, dtype="float64")
    print(f"Created NaN raster at: {nan_raster_path}")
    
    hse_raster_path = output_dir/"hse.tif"
    create_global_raster(hse_raster_path, 1.0)
    print(f"Created HSE raster at: {hse_raster_path}")
    
    k_allom_raster_path = output_dir/"k_allom.tif"
    create_global_raster(k_allom_raster_path, 2.0)
    print(f"Created K_allom raster at: {k_allom_raster_path}")
    
    # Example usage of new functions:
    from shapely.geometry import box
    
    # Create a polygon raster
    polygon = box(-10, -10, 10, 10)  # 20x20 degree box
    poly_raster = output_dir/"polygon_raster.tif"
    create_polygon_raster(poly_raster, polygon, 5.0, resolution=(0.5, 0.5))
    print(f"Created polygon raster at: {poly_raster}")
    
    # Add random nodata
    add_random_nodata(poly_raster, 0.2)  # 20% nodata
    print(f"Added random nodata to polygon raster")
