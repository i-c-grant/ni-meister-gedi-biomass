#!/usr/bin/env python3
"""
Script to create test rasters for GEDI parameter testing.
This creates fixed test assets that can be used by pytest.
"""

import argparse
from pathlib import Path
from typing import Optional
from raster_utils import create_global_raster, halve_raster, create_boundary_raster, create_random_na_raster
import rasterio


def main():
    """Create test rasters for GEDI parameter testing."""
    parser = argparse.ArgumentParser(description="Create test rasters for GEDI parameter testing")
    parser.add_argument("--output-dir", type=str, default="tests/data/rasters",
                        help="Directory to save test rasters")
    parser.add_argument("--resolution", type=float, default=0.005,
                        help="Resolution in degrees")
    args = parser.parse_args()
    
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create test rasters
    create_global_raster(
        output_dir / "hse_global.tif",
        value=1.0,
        resolution=args.resolution
    )
    
    create_global_raster(
        output_dir / "k_allom_global.tif",
        value=2.0,
        resolution=args.resolution
    )
    
    
    # Create boundary-based rasters
    boundary_path = Path("/home/ian/projects/ni-meister-gedi-biomass-global/tests/data/boundaries/test_boundary.gpkg")
    
    create_boundary_raster(
        output_dir / "hse_boundary.tif",
        value=1.0,
        resolution=args.resolution,
        boundary_path=boundary_path
    )
    
    create_boundary_raster(
        output_dir / "k_allom_boundary.tif",
        value=2.0,
        resolution=args.resolution,
        boundary_path=boundary_path
    )
    
    halve_raster(output_dir / "hse_boundary.tif", output_dir / "hse_half.tif")
    halve_raster(output_dir / "k_allom_boundary.tif", output_dir / "k_allom_half.tif")
    
    # Create NA versions from the boundary rasters
    create_random_na_raster(
        output_dir / "hse_boundary.tif",
        output_dir / "hse_boundary_na.tif",
        na_probability=0.5
    )
    
    create_random_na_raster(
        output_dir / "k_allom_boundary.tif",
        output_dir / "k_allom_boundary_na.tif",
        na_probability=0.5
    )
    
    print(f"Created all test rasters in {output_dir}")
    print("To use these rasters in tests, make sure they're in the tests/data/rasters directory")

if __name__ == "__main__":
    main()
