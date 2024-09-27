import csv
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Union
import warnings

import geopandas as gpd
import numpy as np
from shapely.geometry import Point

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
    _waveform_iter: Iterable[Waveform] = field(
        default=None, init=False, repr=False
    )
    # Number of rows to write for the current waveform
    _n_rows: int = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if isinstance(self.path, str):
            self.path = Path(self.path)
        self._file_type = self.path.suffix.lstrip(".")
        if self._file_type not in ["csv", "gpkg"]:
            raise ValueError(f"Unsupported file type {self._file_type}")

        self._waveform_iter = iter(self.waveforms)

    def _get_next(self):
        # Get the next waveform from the iterator
        try:
            wf = next(self._waveform_iter)
        except StopIteration:
            wf = None
        return wf

    def _load_next_waveform(self) -> Optional[Waveform]:
        # Load data to write from next waveform in waveforms
        waveform = self._get_next()

        if waveform is not None:
            self._waveform_data.clear()
            for col_name, col_path in self.cols.items():
                # Depending on requested column, data might be a single value
                # (e.g. biomass index) or an array of values (e.g. raw waveform).
                # Both are okay as long as all columns requested are of the same length,
                # which is checked in WaveformWriter._validate_row_lengths.
                col_data = waveform.get_data(col_path)
                if isinstance(
                    col_data, (int, float, str, np.floating, np.integer)
                ):
                    col_data = [
                        col_data
                    ]  # Cast single values to list for length validation
                elif not isinstance(col_data, (list, np.ndarray)):
                    raise TypeError(
                        f"Unwritable data type {type(col_data)} in "
                        f"column {col_name}"
                    )
                self._waveform_data[col_name] = col_data

            # Update the number of rows based on the first column provided
            self._n_rows = len(list(self._waveform_data.values())[0])

        return waveform

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

    def _to_csv(self) -> None:
        """
        Write specified columns of the waveform to a CSV file.

        Parameters
        ----------
        waveform: Waveform
            The waveform object containing the data to be written.
        """

        # Write to the file
        with open(
            self.path, "a" if self.append else "w", newline=""
        ) as csv_file:
            writer = csv.writer(csv_file)

            # Load data from first waveform and store reference to it
            wf = self._load_next_waveform()

            # Write header if file is empty
            if csv_file.tell() == 0:
                header = ["shot_number", "beam"] + list(
                    self._waveform_data.keys()
                )
                writer.writerow(header)

            # Write data for each waveform
            while wf is not None:
                self._validate_row_lengths()

                # Get metadata for current waveform
                shot_number: str = str(wf.get_data("metadata/shot_number"))
                beam: str = wf.get_data("metadata/beam")

                # Construct and write data rows for current waveform
                wf_data: dict = self._waveform_data
                for i in range(self._n_rows):
                    row = [shot_number, beam] + [
                        col_data[i] for col_data in wf_data.values()
                    ]
                    writer.writerow(row)

                # Load data from the next waveform
                wf = self._load_next_waveform()

    def _to_gpkg(self) -> None:
        rows = []
        wf = self._load_next_waveform()

        while wf is not None:
            self._validate_row_lengths()
            shot_number = str(wf.get_data("metadata/shot_number"))
            beam = wf.get_data("metadata/beam")
            lon = wf.get_data("metadata/coords/lon")
            lat = wf.get_data("metadata/coords/lat")

            for i in range(self._n_rows):
                geometry = Point(lon, lat)
                row = {
                    "shot_number": shot_number,
                    "beam": beam,
                    **{
                        col_name: self._waveform_data[col_name][i]
                        for col_name in self._waveform_data
                    },
                    "geometry": geometry,
                }

                rows.append(row)

            wf = self._load_next_waveform()

        gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")

        gdf.to_file(self.path, driver="GPKG", mode="a" if self.append else "w")

    def write(self) -> None:
        """Write the waveforms to the file if there are any."""
        if len(self.waveforms) > 0:
            if self._file_type == "csv":
                self._to_csv()
            elif self._file_type == "gpkg":
                self._to_gpkg()
        else:
            warnings.warn(f"No waveforms provided to write in {self}",
                          UserWarning)

    def __repr__(self) -> str:
        return f"WaveformWriter(path={self.path!r}, cols={self.cols!r}, append={self.append})"

    def __str__(self) -> str:
        return f"WaveformWriter writing to {self.path} with columns {list(self.cols.keys())}"
