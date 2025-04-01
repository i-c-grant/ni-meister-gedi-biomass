import csv
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Union, List
import warnings
from datetime import datetime

import geopandas as gpd
import numpy as np
from shapely.geometry import Point

import json
import pandas as pd
import logging
logger = logging.getLogger(__name__)

from nmbim.Waveform import Waveform


@dataclass
class WaveformWriter:
    """
    Class for writing data to a CSV file from a collection of Waveforms.

    Attributes
    ----------

    path: str
        The path to the CSV file to write to

    cols: Dict[str, str]
        A dictionary mapping column names to the paths of the data in the waveform

    append: bool
        Whether to append to the file if it already exists.

    waveforms: Iterable[Waveform]
        The waveforms to write to the CSV file.
    """

    path: Union[str, Path]
    cols: Dict[str, str]
    append: bool
    waveforms: Iterable[Waveform]

    # Data dictionary for the current waveform
    _waveform_data: Dict[str, Any] = field(
        default_factory=dict, init=False, repr=False
    )
    # Iterator over the waveforms
    # Number of rows to write for the current waveform
    _n_rows: int = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if isinstance(self.path, str):
            self.path = Path(self.path)
        self._file_type = self.path.suffix.lstrip(".")
        if self._file_type not in ["csv", "gpkg"]:
            raise ValueError(f"Unsupported file type {self._file_type}")



    def _validate_row_lengths(self) -> None:
        # Validate types and lengths of columns in loaded data
        for col_name, col_data in self._waveform_data.items():
            col_len = len(col_data)
            if col_len != self._n_rows:
                raise ValueError(
                    f"All columns must have the same length; "
                    f"column {col_name} has length {col_len}, "
                    f"but the first column has length {self._n_rows}"
                )

    def _gather_rows(self) -> List[dict]:
        """Collect all rows with common fields for both CSV and GeoPackage by directly iterating over self.waveforms."""
        rows = []
        for wf in self.waveforms:
            current_data = {}
            for col_name, col_path in self.cols.items():
                col_data = wf.get_data(col_path)
                single_val_types = (int, float, str, np.floating, np.integer, datetime)
                if isinstance(col_data, single_val_types):
                    col_data = [col_data]
                elif not isinstance(col_data, (list, np.ndarray)):
                    raise TypeError(f"Unwritable data type {type(col_data)} in column {col_name}")
                current_data[col_name] = col_data
            n_rows = len(list(current_data.values())[0])
            shot_number = str(wf.get_data("metadata/shot_number"))
            beam = wf.get_data("metadata/beam")
            lon = wf.get_data("metadata/coords/lon")
            lat = wf.get_data("metadata/coords/lat")
            base_data = {
                "shot_number": shot_number,
                "beam": beam,
                "lon": lon,
                "lat": lat,
            }
            if self._file_type == "gpkg":
                for col_name, col_data in current_data.items():
                    if len(col_data) != n_rows:
                        raise ValueError(f"All columns must have the same length; column {col_name} has length {len(col_data)}, but expected {n_rows}")
                for i in range(n_rows):
                    row = base_data.copy()
                    for key, val in current_data.items():
                        row[key] = val[i]
                    row["geometry"] = Point(lon, lat)
                    rows.append(row)
            else:
                ser_data = {}
                for key, val in current_data.items():
                    ser_data[key] = json.dumps(val) if isinstance(val, (list, np.ndarray)) else val
                row = base_data.copy()
                row.update(ser_data)
                rows.append(row)
        return rows
    
    def write(self) -> None:
        """Write the waveforms to file using gathered rows."""
        num_wfs = len(list(self.waveforms)) if not hasattr(self.waveforms, "__len__") else len(self.waveforms)
        logger.info(f"Beginning writing of {num_wfs} waveform(s) to {self.path} as {self._file_type}")
        if len(self.waveforms) == 0:
            warnings.warn(f"No waveforms provided to write in {self}", UserWarning)
            return

        rows = self._gather_rows()

        # Create appropriate dataframe and write to file
        if self._file_type == "csv":
            df = pd.DataFrame(rows)
            # Write CSV with headers only if not appending
            df.to_csv(
                self.path, 
                mode="a" if self.append else "w", 
                header=not self.path.exists() or not self.append,
                index=False
            )
        elif self._file_type == "gpkg":
            # Convert rows to GeoDataFrame with geometry
            gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")
            gdf.to_file(
                self.path, 
                driver="GPKG", 
                mode="a" if self.append else "w"
            )
            
        logger.info(f"Write operation completed for {self.path}")

    def __repr__(self) -> str:
        return f"WaveformWriter(path={self.path!r}, cols={self.cols!r}, append={self.append})"

    def __str__(self) -> str:
        return f"WaveformWriter writing to {self.path} with columns {list(self.cols.keys())}"
