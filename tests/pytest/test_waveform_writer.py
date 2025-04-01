import csv
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest

from nmbim.WaveformWriter import WaveformWriter

# Dummy Waveform class implementing the minimal get_data interface
class DummyWaveform:
    def __init__(self, shot_number, data):
        # data is a dict mapping data paths to values
        self._data = {"metadata/shot_number": shot_number}
        self._data.update(data)
    
    def get_data(self, path):
        # Return stored value or None
        return self._data.get(path)

# Prepare a dummy list of waveforms with consistent data lengths.
@pytest.fixture
def dummy_waveforms():
    # Each waveform returns scalars for single-row output.
    wf1 = DummyWaveform(
        shot_number="1001",
        data={
            "data/col1": 10,
            "data/col2": 20,
            # For gpkg, extra metadata needed:
            "metadata/beam": "BEAM0000",
            "metadata/coords/lon": -100.0,
            "metadata/coords/lat": 40.0,
        }
    )
    wf2 = DummyWaveform(
        shot_number="1002",
        data={
            "data/col1": 30,
            "data/col2": 40,
            "metadata/beam": "BEAM0001",
            "metadata/coords/lon": -99.5,
            "metadata/coords/lat": 39.5,
        }
    )
    return [wf1, wf2]

# Test CSV output functionality
def test_waveform_writer_csv(tmp_path, dummy_waveforms):
    # Configure output columns mapping: keys become CSV header columns.
    cols = {"col1": "data/col1", "col2": "data/col2"}
    output_file = tmp_path / "test_output.csv"

    writer = WaveformWriter(path=str(output_file), cols=cols, append=False, waveforms=dummy_waveforms)
    writer.write()

    # Open the written CSV and verify its content.
    with open(output_file, "r", newline="") as csvfile:
        reader = csv.reader(csvfile)
        rows = list(reader)

    # Expected header: "shot_number" then the keys of cols (order preserved)
    expected_header = ["shot_number", "beam", "lon", "lat"] + list(cols.keys())
    assert rows[0] == expected_header

    # There should be one data row per waveform.
    assert len(rows) == 1 + len(dummy_waveforms)

    # Validate each data row.
    import json
    for wf, row in zip(dummy_waveforms, rows[1:]):
        shot_num = wf.get_data("metadata/shot_number")
        col1 = wf.get_data("data/col1")
        col2 = wf.get_data("data/col2")
        assert row[0] == str(shot_num)
        assert row[1] == str(wf.get_data("metadata/beam"))
        assert row[2] == str(wf.get_data("metadata/coords/lon"))
        assert row[3] == str(wf.get_data("metadata/coords/lat"))
        # For scalar values, they were wrapped as [value] and then serialized to JSON.
        assert row[4] == json.dumps([col1])
        assert row[5] == json.dumps([col2])

# Test GeoPackage (GPKG) output functionality
def test_waveform_writer_gpkg(tmp_path, dummy_waveforms):
    # For gpkg, the _waveform_data extraction will use the same cols mapping.
    # We still need the extra metadata: "metadata/coords/lon", "metadata/coords/lat", and "metadata/beam".
    cols = {"col1": "data/col1", "col2": "data/col2"}
    output_file = tmp_path / "test_output.gpkg"

    writer = WaveformWriter(path=str(output_file), cols=cols, append=False, waveforms=dummy_waveforms)
    writer.write()

    # Use geopandas to read the gpkg; confirm number of features and presence of geometry column.
    gdf = gpd.read_file(str(output_file), driver="GPKG")
    # There should be as many rows as waveforms
    assert len(gdf) == len(dummy_waveforms)
    # Check required geometry and extra metadata columns from writer
    expected_cols = {"shot_number", "beam", "geometry"} | set(cols.keys())
    assert expected_cols.issubset(set(gdf.columns))
    
    # Verify geometry column is valid (Point with proper lon, lat from each waveform)
    for wf, geom in zip(dummy_waveforms, gdf["geometry"]):
        lon = wf.get_data("metadata/coords/lon")
        lat = wf.get_data("metadata/coords/lat")
        # The geometry should be a Point with given coordinates.
        assert np.isclose(geom.x, lon)
        assert np.isclose(geom.y, lat)

# Test CSV output with array-like fields (multiple rows per waveform)
def test_waveform_writer_csv_array_fields(tmp_path):
    # Create a dummy waveform returning array-like fields.
    wf = DummyWaveform(
        shot_number="2001",
        data={
            "data/array1": [1, 2, 3],
            "data/array2": [4, 5, 6],
            # Required metadata for gpkg (even though this is CSV, we include them for consistency)
            "metadata/beam": "BEAM0000",
            "metadata/coords/lon": -100.0,
            "metadata/coords/lat": 40.0,
        }
    )
    # Mapping: both columns refer to array outputs.
    cols = {"array1": "data/array1", "array2": "data/array2"}
    output_file = tmp_path / "test_output_array.csv"

    writer = WaveformWriter(path=str(output_file), cols=cols, append=False, waveforms=[wf])
    writer.write()

    # Read the CSV content
    with open(output_file, "r", newline="") as csvfile:
        reader = csv.reader(csvfile)
        rows = list(reader)

    # The header should include new automatic columns (beam, lon, lat) + specified columns
    expected_header = ["shot_number", "beam", "lon", "lat", "array1", "array2"]
    assert rows[0] == expected_header
    # Expect only one data row per waveform with JSON-serialized array-like fields.
    assert len(rows) == 2

    # Validate that the row contains JSON serialized array-like fields and automatic columns
    import json
    expected_row = [
        "2001", 
        "BEAM0000",  # beam
        "-100.0",     # lon
        "40.0",       # lat
        json.dumps(wf.get_data("data/array1")), 
        json.dumps(wf.get_data("data/array2"))
    ]
    assert rows[1] == expected_row


# Test GeoPackage (GPKG) output with array-like fields.
def test_waveform_writer_gpkg_array_fields(tmp_path):
    wf = DummyWaveform(
        shot_number="2001",
        data={
            "data/array1": [7, 8, 9],
            "data/array2": [10, 11, 12],
            "metadata/beam": "BEAM0000",
            "metadata/coords/lon": -100.5,
            "metadata/coords/lat": 40.5,
        }
    )
    cols = {"array1": "data/array1", "array2": "data/array2"}
    output_file = tmp_path / "test_output_array.gpkg"

    writer = WaveformWriter(path=str(output_file), cols=cols, append=False, waveforms=[wf])
    writer.write()

    # Read the written GeoPackage using geopandas.
    gdf = gpd.read_file(str(output_file), driver="GPKG")
    # Expect as many rows as array elements (3)
    assert len(gdf) == 3
    # The required columns should be present.
    expected_cols = {"shot_number", "beam", "geometry", "array1", "array2"}
    assert expected_cols.issubset(set(gdf.columns))

    # Check geometry for each row matches the provided coordinates.
    for geom in gdf["geometry"]:
        # Use np.isclose on the coordinate values.
        assert np.isclose(geom.x, -100.5)
        assert np.isclose(geom.y, 40.5)

    # Verify that the array columns are written correctly
    # Note: The writer writes one row per element in the arrays.
    for i, (_, row) in enumerate(gdf.iterrows()):
        assert str(row["shot_number"]) == "2001"
        assert row["array1"] == 7 + i
        assert row["array2"] == 10 + i
