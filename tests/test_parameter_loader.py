import pytest
import sys
import numpy as np
import rasterio
import h5py
from pathlib import Path
from rasterio.errors import CRSError
from typing import Union

FloatLike = Union[float, np.floating]

# Add package root to Python path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

from nmbim import ParameterLoader, RasterSource, ScalarSource, WaveformCollection
from nmbim.Waveform import Waveform

@pytest.fixture
def hse_raster():
    return Path(__file__).parent.parent/"tests"/"input"/"hse.tif"

@pytest.fixture
def k_allom_raster():
    return Path(__file__).parent.parent/"tests"/"input"/"k_allom.tif"

@pytest.fixture
def nan_raster():
    return Path(__file__).parent/"input"/"nan_raster.tif"

@pytest.fixture
def sample_waveform(tmp_path):
    # Setup test data paths from the user's input directory
    test_data_dir = Path(__file__).parent.parent/"tests"/"input"
    l1b = test_data_dir/"GEDI01_B_2021151223415_O13976_02_T00676_02_005_02_V002.h5"
    l2a = test_data_dir/"GEDI02_A_2021151223415_O13976_02_T00676_02_003_02_V002.h5"
    l4a = test_data_dir/"GEDI04_A_2021151223415_O13976_02_T00676_02_002_02_V002.h5"
    
    with h5py.File(l1b, "r") as f1, h5py.File(l2a, "r") as f2, h5py.File(l4a, "r") as f3:
        # Get first beam and first shot number
        beam_name = [k for k in f1.keys() if k.startswith("BEAM")][0]
        shot_number = f1[beam_name]["shot_number"][0]
        
        # Create a real waveform using production data
        waveform = Waveform(
            shot_number=shot_number,
            l1b=f1,
            l2a=f2,
            l4a=f3
        )
    return waveform

@pytest.mark.requires_data
def test_raster_parameter_loading(sample_waveform, hse_raster, k_allom_raster):
    """Test loading parameters from raster sources"""
    test_data_dir = Path(__file__).parent.parent/"tests"/"input"
    
    # Create ParameterLoader with test rasters
    loader = ParameterLoader(
        sources={
            "hse": RasterSource(hse_raster),
            "k_allom": RasterSource(k_allom_raster)
        },
        waveforms=WaveformCollection.from_waveforms([sample_waveform])
    )
    
    loader.parameterize()
    
    # Verify parameters were added
    wf = sample_waveform
    assert "metadata/parameters/hse" in wf.get_paths()
    assert "metadata/parameters/k_allom" in wf.get_paths()
    
    # Verify raster sampling worked
    hse_value = wf.get_data("metadata/parameters/hse")
    assert isinstance(hse_value, FloatLike)
    assert 0 < hse_value < 100  # Based on test data characteristics

@pytest.mark.parametrize("scalar_value", [42.0, "3.14", -5])
def test_scalar_parameter_loading(sample_waveform, scalar_value):
    """Test loading scalar parameters with various valid inputs"""
    loader = ParameterLoader(
        sources={"test_param": ScalarSource(scalar_value)},
        waveforms=WaveformCollection.from_waveforms([sample_waveform])
    )
    
    loader.parameterize()
    
    # Verify scalar parameter
    stored_value = sample_waveform.get_data("metadata/parameters/test_param")
    assert stored_value == float(scalar_value)

def test_invalid_scalar_raises_error(sample_waveform):
    """Test invalid scalar values raise proper error"""
    with pytest.raises(TypeError) as excinfo:
        ScalarSource("not_a_number")
    
    assert "Invalid scalar value" in str(excinfo.value)

@pytest.mark.requires_data 
def test_missing_raster_raises_error(sample_waveform):
    """Test missing raster file validation"""
    with pytest.raises(FileNotFoundError):
        # Need to call validate() to trigger the file check
        RasterSource("nonexistent_file.tif").validate()

@pytest.mark.requires_data
def test_missing_raster_value_raises_error(sample_waveform, nan_raster):
    """Test missing raster values raise proper error"""    
    # Create ParameterLoader with the NaN raster
    loader = ParameterLoader(
        sources={"hse": RasterSource(nan_raster)},
        waveforms=WaveformCollection.from_waveforms([sample_waveform])
    )
    
    # Test parameterize() raises the error
    with pytest.raises(ValueError) as excinfo:
        loader.parameterize()
    
    assert "No raster value found at location" in str(excinfo.value)

def test_wrong_crs_raster_raises_error(tmp_path):
    """Test CRS validation for raster sources"""
    # Create temp raster with wrong CRS
    bad_raster = tmp_path/"bad_crs.tif"
    with rasterio.open(
        bad_raster,
        "w",
        driver="GTiff",
        height=100,
        width=100,
        count=1,
        dtype="float32",
        crs="EPSG:3857",  # Wrong CRS!
        transform=rasterio.Affine.identity()
    ):
        pass
    
    with pytest.raises(CRSError) as excinfo:
        RasterSource(bad_raster).validate()
    
    assert "expected EPSG:4326" in str(excinfo.value)

def test_mixed_parameter_sources(sample_waveform, hse_raster):
    """Test mixing raster and scalar parameter sources"""
    test_data_dir = Path(__file__).parent.parent/"tests"/"input"
    
    loader = ParameterLoader(
        sources={
            "hse": RasterSource(hse_raster),
            "k_allom": ScalarSource(0.5)
        },
        waveforms=WaveformCollection.from_waveforms([sample_waveform])
    )
    
    loader.parameterize()
    
    wf = sample_waveform
    assert isinstance(wf.get_data("metadata/parameters/hse"), FloatLike)
    assert wf.get_data("metadata/parameters/k_allom") == 0.5

def test_empty_waveform_collection(hse_raster):
    """Test parameterizing an empty waveform collection"""
    loader = ParameterLoader(
        sources={"hse": RasterSource(hse_raster)},
        waveforms=WaveformCollection.from_waveforms([])
    )
    # Should complete without errors
    loader.parameterize()


def test_corrupted_raster_file(tmp_path):
    """Test handling of corrupted/invalid raster files"""
    bad_raster = tmp_path/"corrupted.tif"
    bad_raster.write_text("Not a real TIFF file")
    
    with pytest.raises(rasterio.RasterioIOError):
        RasterSource(bad_raster).validate()
