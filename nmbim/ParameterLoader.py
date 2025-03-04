from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Union, Optional
import math

import rasterio
from rasterio.io import DatasetReader
from rasterio.errors import CRSError
from rasterio.crs import CRS
from shapely.geometry import Point

from nmbim.WaveformCollection import WaveformCollection
from nmbim.Waveform import Waveform


class ParameterSource(ABC):
    """Interface for parameter value sources."""
    
    @abstractmethod
    def get_value(self, waveform: 'Waveform') -> float:
        pass

    @abstractmethod
    def validate(self) -> None:
        """Validate source configuration."""
        pass


class RasterSource(ParameterSource):
    """Parameter values from geospatial raster."""
    
    def __init__(self, path: str):
        self.path = Path(path)
        
    def validate(self):
        """Verify raster exists and uses WGS84 CRS."""
        if not self.path.exists():
            raise FileNotFoundError(f"Raster {self.path} not found")
        with rasterio.open(self.path) as src:
            if src.crs.to_epsg() != 4326:
                raise CRSError("Invalid raster CRS (expected EPSG:4326)")


    def get_value(self, waveform: 'Waveform') -> Optional[float]:
        """Sample raster at waveform location.
        
        Returns
        ------
        Optional[float]
        The raster value at the waveform's location, or None if no valid data exists
        """
        point = waveform.get_data("metadata/point_geom")
        with rasterio.open(self.path) as src:
            nodata = src.nodata  # Assume single band
            value = next(src.sample([(point.x, point.y)]))[0]
        if nodata is not None and value == nodata:
            return None
        if isinstance(value, float) and math.isnan(value):
            return None
        return value


class ScalarSource(ParameterSource):
    """Constant value for all waveforms."""
    
    def __init__(self, value: Union[str, float, int]):
        try:
            self.value = float(value)
        except ValueError:
            raise TypeError(f"Invalid scalar value: {value}")

    def validate(self) -> None:
        pass  # Validation handled in constructor

    def get_value(self, waveform: 'Waveform') -> float:
        return self.value


@dataclass
class ParameterLoader:
    """
    Coordinates loading multiple parameters for one WaveformCollection.
    
    Attributes
    ----------
    sources : Dict[str, ParameterSource]
        Mapping of parameter names to value sources
    waveforms : WaveformCollection
        Collection of waveforms to parameterize
    """
    sources: Dict[str, ParameterSource]
    waveforms: WaveformCollection

    def __post_init__(self):
        """Validate all sources before use."""
        for source in self.sources.values():
            source.validate()

    def parameterize(self) -> None:
        """Write parameters to all waveforms."""
        for param_name, source in self.sources.items():
            for wf in self.waveforms:
                value = source.get_value(wf)
                wf.save_data(
                    data=value,
                    path=f"metadata/parameters/{param_name}",
                )
