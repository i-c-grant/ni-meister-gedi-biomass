import pytest
from pathlib import Path
import h5py
import yaml

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
    path = test_data_dir/"config"/"config.yaml"
    with open(path) as f:
        return yaml.safe_load(f)

@pytest.fixture(scope="session")
def hse_raster(test_data_dir):
    """Fixture for HSE raster"""
    return test_data_dir/"allom"/"hse.tif"

@pytest.fixture(scope="session")
def k_allom_raster(test_data_dir):
    """Fixture for K_allom raster"""
    return test_data_dir/"allom"/"k_allom.tif"

@pytest.fixture(scope="session")
def nan_raster(test_data_dir):
    """Fixture for NaN raster"""
    return test_data_dir/"allom"/"nan_raster.tif"

@pytest.fixture
def default_hse():
    """Fixture for default HSE value"""
    return 1.0

@pytest.fixture
def default_k_allom():
    """Fixture for default K_allom value"""
    return 2.0
