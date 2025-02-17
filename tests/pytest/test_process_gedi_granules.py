import pytest
from click.testing import CliRunner
from pathlib import Path
import yaml
import sys

# Add package root to Python path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

from process_gedi_granules import main

@pytest.fixture
def runner():
    return CliRunner()

@pytest.fixture
def test_data_dir():
    return Path(__file__).parent.parent/"tests"/"input"

@pytest.fixture
def test_paths(test_data_dir):
    """Fixture for test file paths"""
    return {
        "l1b": test_data_dir/"GEDI01_B_2021151223415_O13976_02_T00676_02_005_02_V002.h5",
        "l2a": test_data_dir/"GEDI02_A_2021151223415_O13976_02_T00676_02_003_02_V002.h5", 
        "l4a": test_data_dir/"GEDI04_A_2021151223415_O13976_02_T00676_02_002_02_V002.h5",
        "hse_raster": test_data_dir/"hse.tif",
        "k_allom_raster": test_data_dir/"k_allom.tif",
        "config": test_data_dir/"config.yaml"
    }

@pytest.mark.slow
def test_cli_with_scalar_params(runner, test_paths, tmp_path):
    """Test CLI with scalar parameter values"""
    
    result = runner.invoke(main, [
        str(test_paths["l1b"]), str(test_paths["l2a"]), str(test_paths["l4a"]),
        "42.0", "0.5",  # Scalar values for hse and k_allom
        str(tmp_path),
        "--config", test_paths["config"]
    ])
    
    assert result.exit_code == 0
    assert "Run complete" in result.output

@pytest.mark.slow
def test_cli_with_raster_params(runner, test_paths, tmp_path):
    """Test CLI with raster parameter files"""
    
    result = runner.invoke(main, [
        str(test_paths["l1b"]), str(test_paths["l2a"]), str(test_paths["l4a"]),
        str(test_paths["hse_raster"]), str(test_paths["k_allom_raster"]),
        str(tmp_path),
        "--config", test_paths["config"]
    ])
    
    assert result.exit_code == 0
    assert "Run complete" in result.output

def test_cli_invalid_parameter(runner, test_paths, tmp_path):
    """Test CLI with invalid parameter value"""
    
    result = runner.invoke(main, [
        str(test_paths["l1b"]), str(test_paths["l2a"]), str(test_paths["l4a"]),
        "not_a_number", "0.5",  # Invalid hse value
        str(tmp_path),
        "--config", test_paths["config"]
    ])
    
    assert result.exit_code != 0
    assert "Invalid hse source" in result.output

def test_cli_missing_file(runner, test_paths, tmp_path):
    """Test CLI with missing input file"""
    
    result = runner.invoke(main, [
        str(test_paths["l1b"]), str(test_paths["l2a"]), "missing_file.h5",
        "42.0", "0.5",
        str(tmp_path),
        "--config", test_paths["config"]
    ])
    
    assert result.exit_code != 0
    assert "Path 'missing_file.h5' does not exist" in result.output
