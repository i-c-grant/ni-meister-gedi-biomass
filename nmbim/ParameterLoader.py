from dataclasses import dataclass
from typing import Dict, List

import rasterio
from rasterio.io import DatasetReader
from rasterio.errors import CRSError
from rasterio.crs import CRS
from shapely.geometry import Point

from nmbim.WaveformCollection import WaveformCollection


@dataclass
class ParameterLoader:
    """
    A class to load parameter values from rasters for GEDI waveforms.

    Attributes
    ----------
    param_rasters : Dict[str, DatasetReader]
        Dictionary of parameter name to rasterio dataset reader
    waveforms : WaveformCollection
        Collection of waveforms to parameterize
    """
    raster_paths: Dict[str, str]
    waveforms: WaveformCollection

    def __post_init__(self):
        """Open raster files and verify CRS"""
        self.param_rasters = {
            param_name: rasterio.open(raster_path)
            for param_name, raster_path in self.raster_paths.items()
        }

        # Check that all rasters are in EPSG:4326
        wgs84 = CRS.from_epsg(4326)
        if len(self.param_rasters) > 0:
            for name, raster in self.param_rasters.items():
                if not raster.crs.equals(wgs84):
                    raise CRSError(
                        f"CRS mismatch: {name} has CRS {raster.crs}, "
                        f"expected EPSG:4326 (WGS 84)"
                    )


    def get_points(self) -> List[Point]:
        """Get list of point geometries from all waveforms in collection.

        Returns
        -------
        List[Point]
            List of shapely Points representing waveform locations
        """
        return [wf.get_data("metadata/point_geom") for wf in self.waveforms]


    def parameterize(self) -> None:
        """Sample parameter values at waveform locations and save to waveforms.
        
        For each parameter raster, samples values at all waveform locations
        and saves the values to the corresponding waveforms under
        'metadata/parameters/{param_name}'.
        """
        points = self.get_points()
        
        for param_name, raster in self.param_rasters.items():
            # Sample raster values at all points
            values = [value[0] for value in raster.sample(
                [(p.x, p.y) for p in points]
            )]
            
            # Save values to waveforms
            for wf, value in zip(self.waveforms, values):
                wf.save_data(
                    data=value,
                    path=f"metadata/parameters/{param_name}"
                )

        # Close all rasters (each instance only parameterizes once)
        self.close_rasters()

    def close_rasters(self):
        """Close all parameter rasters."""
        for raster in self.param_rasters.values():
            raster.close()
