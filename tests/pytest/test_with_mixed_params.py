"""
Tests for parameter loading with different raster configurations.

Before running these tests, you need to generate the test rasters:
    python tests/util/create_test_rasters.py

This will create the necessary raster files in tests/data/rasters.
"""

import pytest
import time
from datetime import datetime
from pathlib import Path
import h5py
from click.testing import CliRunner
from process_gedi_granules import main as process_gedi_granules
import yaml
import geopandas as gpd
import numbers

MAX_WAVEFORMS = 100000

@pytest.fixture(scope="session")
def test_data_dir():
    """Fixture for the test data directory"""
    return Path(__file__).parent.parent/"data"

@pytest.fixture(scope="session")
def l1b_file(test_data_dir):
    """Fixture for L1B GEDI file"""
    path = test_data_dir/"gedi"/"GEDI01_B_2021151223415_O13976_02_T00676_02_005_02_V002.h5"
    return h5py.File(path, "r")

@pytest.fixture(scope="session")
def l2a_file(test_data_dir):
    """Fixture for L2A GEDI file"""
    path = test_data_dir/"gedi"/"GEDI02_A_2021151223415_O13976_02_T00676_02_003_02_V002.h5"
    return h5py.File(path, "r")

@pytest.fixture(scope="session")
def l4a_file(test_data_dir):
    """Fixture for L4A GEDI file"""
    path = test_data_dir/"gedi"/"GEDI04_A_2021151223415_O13976_02_T00676_02_002_02_V002.h5"
    return h5py.File(path, "r")

@pytest.fixture(scope="session")
def boundary_file(test_data_dir):
    """Fixture for boundary file"""
    return test_data_dir/"boundaries"/"test_boundary.gpkg"

@pytest.fixture(scope="session")
def config_file(test_data_dir):
    """Fixture for config file"""
    return test_data_dir/"config"/"config.yaml"

@pytest.fixture(scope="session")
def raster_data_dir(test_data_dir):
    """Fixture for the raster data directory"""
    path = test_data_dir / "rasters"
    return path

@pytest.fixture(scope="session")
def hse_raster(raster_data_dir):
    """Fixture for HSE raster with global coverage"""
    path = raster_data_dir / "hse_global.tif"
    return path

@pytest.fixture(scope="session")
def hse_raster_half(raster_data_dir):
    """Fixture for HSE raster with half coverage"""
    path = raster_data_dir / "hse_half.tif"
    return path

@pytest.fixture(scope="session")
def k_allom_raster(raster_data_dir):
    """Fixture for K_allom raster with global coverage"""
    path = raster_data_dir / "k_allom_global.tif"
    return path

@pytest.fixture(scope="session")
def k_allom_raster_half(raster_data_dir):
    """Fixture for K_allom raster with half coverage"""
    path = raster_data_dir / "k_allom_half.tif"
    return path

@pytest.fixture(scope="session")
def hse_raster_na(raster_data_dir):
    """Fixture for HSE raster with NA values"""
    path = raster_data_dir / "hse_boundary_na.tif"
    return path

@pytest.fixture(scope="session")
def k_allom_raster_na(raster_data_dir):
    """Fixture for K_allom raster with NA values"""
    path = raster_data_dir / "k_allom_boundary_na.tif"
    return path

@pytest.fixture
def default_hse():
    """Fixture for default HSE value"""
    return 5.0

@pytest.fixture
def default_k_allom():
    """Fixture for default K_allom value"""
    return 10.0

@pytest.fixture
def output_dir(request):
    """Fixture for creating a timestamped output directory with a label."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    label = request.node.name  # Use the test function name as the label
    path = Path("tests/output") / f"{label}_{timestamp}"
    path.mkdir(parents=True, exist_ok=True)
    return path

@pytest.fixture(autouse=True)
def wait_after_test():
    """Wait 10 seconds after each test to allow processes to clean up."""
    yield  # This runs the test
    print(f"\nWaiting 10 seconds after test to allow processes to clean up...")
    time.sleep(10)  # Wait 10 seconds after the test

def test_full_coverage_raster(l1b_file, l2a_file, l4a_file, hse_raster, k_allom_raster, default_hse, default_k_allom, config_file, output_dir):
    """Test parameter loading with full coverage raster"""
    # Debug: Print the HSE raster path and check if it exists
    print(f"\nHSE raster path: {hse_raster}")
    print(f"HSE raster exists: {Path(hse_raster).exists()}")
    
    # Debug: Check the raster values
    import rasterio
    try:
        with rasterio.open(hse_raster) as src:
            data = src.read(1)
            print(f"HSE raster values: min={data.min()}, max={data.max()}, unique={set(data.flatten())}")
            print(f"Raster bounds: {src.bounds}")
    except Exception as e:
        print(f"Error reading raster: {e}")
    
    runner = CliRunner()

    result = runner.invoke(process_gedi_granules, [
        str(l1b_file.filename),
        str(l2a_file.filename),
        str(l4a_file.filename),
        str(output_dir),  # output_dir
        "--default-hse", str(default_hse),  # default-hse
        "--default-k-allom", str(default_k_allom),  # default-k-allom
        "--config", str(config_file),
        "--hse-path", str(hse_raster),
        "--k-allom-path", str(k_allom_raster),
        "--max-waveforms", MAX_WAVEFORMS,
        "--n_workers", "8"
    ])
    print(f"Command exit code: {result.exit_code}")
    if result.exception:
        print(f"Exception: {result.exception}")
    assert result.exit_code == 0
    
    # Verify all waveforms got HSE values from raster
    output_gpkg = next(output_dir.glob("*.gpkg"))
    gdf = gpd.read_file(output_gpkg)
    
    assert not gdf.empty
    assert "hse" in gdf.columns
    assert "k_allom" in gdf.columns
    
    # Should have HSE from raster (1.0) and K_allom from raster (2.0)
    assert set(gdf["hse"].values) == {1.0}
    assert set(gdf["k_allom"].values) == {2.0}
    
    # Verify data types
    assert all(isinstance(v, numbers.Real) for v in gdf["hse"].values)
    assert all(isinstance(v, numbers.Real) for v in gdf["k_allom"].values)

def test_na_rasters(l1b_file, l2a_file, l4a_file, hse_raster_na, k_allom_raster_na, default_hse, default_k_allom, config_file, output_dir):
    """Test parameter loading with NA rasters"""
    runner = CliRunner()
    result = runner.invoke(process_gedi_granules, [
        str(l1b_file.filename),
        str(l2a_file.filename),
        str(l4a_file.filename),
        str(output_dir),
        "--default-hse", str(default_hse),
        "--default-k-allom", str(default_k_allom),
        "--config", str(config_file),
        "--hse-path", str(hse_raster_na),
        "--k-allom-path", str(k_allom_raster_na),
        "--max-waveforms", MAX_WAVEFORMS,
        "--n_workers", "8"
    ])
    print(f"Command exit code: {result.exit_code}")
    if result.exception:
        print(f"Exception: {result.exception}")
    assert result.exit_code == 0

    output_gpkg = next(output_dir.glob("*.gpkg"))
    gdf = gpd.read_file(output_gpkg)

    # Check that the raster values from NA rasters are either the raster value or the default.
    assert "hse" in gdf.columns
    assert "k_allom" in gdf.columns
    assert set(gdf["hse"].values).issubset({1.0, default_hse})
    assert set(gdf["k_allom"].values).issubset({2.0, default_k_allom})

    assert all(isinstance(v, float) for v in gdf["hse"].values)
    assert all(isinstance(v, float) for v in gdf["k_allom"].values)

def test_partial_coverage_raster(l1b_file, l2a_file, l4a_file, k_allom_raster_half, default_hse, default_k_allom, config_file, output_dir):
    """Test parameter loading with partial coverage raster"""
    # Debug: Print the K_allom raster path and check if it exists
    print(f"\nK_allom half raster path: {k_allom_raster_half}")
    print(f"K_allom half raster exists: {Path(k_allom_raster_half).exists()}")
    
    runner = CliRunner()
    result = runner.invoke(process_gedi_granules, [
        str(l1b_file.filename),
        str(l2a_file.filename),
        str(l4a_file.filename),
        str(output_dir),  # output_dir
        "--default-hse", str(default_hse),  # default-hse
        "--default-k-allom", str(default_k_allom),  # default-k_allom
        "--config", str(config_file),
        "--hse-path", "",
        "--k-allom-path", str(k_allom_raster_half),
        "--max-waveforms", MAX_WAVEFORMS,
        "--n_workers", "8"
    ])
    print(f"Command exit code: {result.exit_code}")
    if result.exception:
        print(f"Exception: {result.exception}")
    assert result.exit_code == 0
    
    # Verify some waveforms got K_allom from raster, others used default
    output_gpkg = next(output_dir.glob("*.gpkg"))
    gdf = gpd.read_file(output_gpkg)
    
    assert not gdf.empty
    assert "hse" in gdf.columns
    assert "k_allom" in gdf.columns
    
    # Should have default HSE (5.0) and mix of K_allom from raster (2.0) and default (10.0)
    assert set(gdf["hse"].values) == {5.0}
    assert set(gdf["k_allom"].values) == {2.0, 10.0}
    
    # Verify data types
    assert all(isinstance(v, numbers.Real) for v in gdf["hse"].values)
    assert all(isinstance(v, numbers.Real) for v in gdf["k_allom"].values)

def test_mixed_parameters(l1b_file, l2a_file, l4a_file, hse_raster, k_allom_raster_half, default_hse, default_k_allom, config_file, output_dir):
    """Test parameter loading with both rasters"""
    runner = CliRunner()
    result = runner.invoke(process_gedi_granules, [
        str(l1b_file.filename),
        str(l2a_file.filename),
        str(l4a_file.filename),
        str(output_dir),  # output_dir
        "--default-hse", str(default_hse),  # default-hse
        "--default-k-allom", str(default_k_allom),  # default-k_allom
        "--config", str(config_file),
        "--hse-path", str(hse_raster),
        "--k-allom-path", str(k_allom_raster_half),
        "--max-waveforms", MAX_WAVEFORMS,
        "--n_workers", "8"
    ])
    print(f"Command exit code: {result.exit_code}")
    if result.exception:
        print(f"Exception: {result.exception}")
    assert result.exit_code == 0
    
    # Verify waveforms got:
    # - HSE from full coverage raster
    # - K_allom from partial coverage raster where available, else default
    output_gpkg = next(output_dir.glob("*.gpkg"))
    gdf = gpd.read_file(output_gpkg)
    
    assert not gdf.empty
    assert "hse" in gdf.columns
    assert "k_allom" in gdf.columns
    
    # Should have HSE from raster (1.0) and mix of K_allom from raster (2.0) and default (10.0)
    assert set(gdf["hse"].values) == {1.0}
    assert set(gdf["k_allom"].values) == {2.0, 10.0}
    
    # Verify data types
    assert all(isinstance(v, numbers.Real) for v in gdf["hse"].values)
    assert all(isinstance(v, numbers.Real) for v in gdf["k_allom"].values)

def test_default_parameters_only(l1b_file, l2a_file, l4a_file, default_hse, default_k_allom, config_file, output_dir):
    """Test parameter loading with only default values"""
    runner = CliRunner()
    result = runner.invoke(process_gedi_granules, [
        str(l1b_file.filename),
        str(l2a_file.filename),
        str(l4a_file.filename),
        str(output_dir),  # output_dir
        "--default-hse", str(default_hse),  # default-hse
        "--default-k-allom", str(default_k_allom),  # default-k_allom
        "--config", str(config_file),
        "--hse-path", "",
        "--k-allom-path", "",
        "--max-waveforms", MAX_WAVEFORMS,
        "--n_workers", "8"
    ])
    print(f"Command exit code: {result.exit_code}")
    if result.exception:
        print(f"Exception: {result.exception}")
    assert result.exit_code == 0
    
    # Verify all waveforms used default parameter values
    output_gpkg = next(output_dir.glob("*.gpkg"))
    gdf = gpd.read_file(output_gpkg)
    
    assert not gdf.empty
    assert "hse" in gdf.columns
    assert "k_allom" in gdf.columns
    
    # Should have default HSE (5.0) and default K_allom (10.0)
    assert set(gdf["hse"].values) == {5.0}
    assert set(gdf["k_allom"].values) == {10.0}
    
    # Verify data types
    assert all(isinstance(v, numbers.Real) for v in gdf["hse"].values)
    assert all(isinstance(v, numbers.Real) for v in gdf["k_allom"].values)
