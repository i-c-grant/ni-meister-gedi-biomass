import numpy as np
import rasterio
from pathlib import Path

def create_nan_raster(output_path: Path):
    """Create a test raster filled with NaN values."""
    with rasterio.open(
        output_path,
        "w",
        driver="GTiff",
        height=1,
        width=1,
        count=1,
        dtype="float64",
        crs="EPSG:4326",
        transform=rasterio.Affine(360.0, 0.0, -180.0, 0.0, -180.0, 90.0),
        nodata=np.nan
    ) as dst:
        dst.write(np.array([[np.nan]]), 1)

def create_hse_raster(output_path: Path):
    """Create a test raster filled with 1.0 values."""
    with rasterio.open(
        output_path,
        "w",
        driver="GTiff",
        height=1,
        width=1,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=rasterio.Affine(360.0, 0.0, -180.0, 0.0, -180.0, 90.0),
        nodata=np.nan
    ) as dst:
        dst.write(np.array([[1.0]]), 1)

def create_k_allom_raster(output_path: Path):
    """Create a test raster filled with 2.0 values."""
    with rasterio.open(
        output_path,
        "w",
        driver="GTiff",
        height=1,
        width=1,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=rasterio.Affine(360.0, 0.0, -180.0, 0.0, -180.0, 90.0),
        nodata=np.nan
    ) as dst:
        dst.write(np.array([[2.0]]), 1)

if __name__ == "__main__":
    # Create in tests/input directory
    output_dir = Path(__file__).parent/"input"
    output_dir.mkdir(exist_ok=True)
    
    # Create all test rasters
    nan_raster_path = output_dir/"nan_raster.tif"
    create_nan_raster(nan_raster_path)
    print(f"Created NaN raster at: {nan_raster_path}")
    
    hse_raster_path = output_dir/"hse.tif"
    create_hse_raster(hse_raster_path)
    print(f"Created HSE raster at: {hse_raster_path}")
    
    k_allom_raster_path = output_dir/"k_allom.tif"
    create_k_allom_raster(k_allom_raster_path)
    print(f"Created K_allom raster at: {k_allom_raster_path}")
