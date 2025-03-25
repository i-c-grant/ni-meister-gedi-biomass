import pytest
from pathlib import Path
from click.testing import CliRunner
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import scripts.summarize_biwf_in_emap_hexes as summarize_biwf
import glob

@pytest.fixture(scope="session")
def hex_grid_path():
    """Fixture for production hexgrid path"""
    path = Path("data/fia/menlove_healey/2020_biohex_merged.gpkg")
    if not path.exists():
        pytest.skip("Production hexgrid file not found")
    return str(path)

@pytest.fixture(scope="module")
def hex_gdf(hex_grid_path):
    """Fixture for loaded hexgrid GeoDataFrame"""
    return summarize_biwf.load_hexgrid(hex_grid_path)

@pytest.fixture
def gpkg_paths():
    """Fixture for actual GPKG files from model runs"""
    path_pattern = "results/model_runs/fifth_run/**/*.gpkg"
    paths = glob.glob(path_pattern, recursive=True)
    
    if not paths:
        pytest.skip(f"No GPKG files found in {path_pattern}")
        
    return paths

@pytest.fixture
def gpkg_pattern(gpkg_paths):
    """Fixture for glob pattern matching test files"""
    return str(Path(gpkg_paths[0]).parent / "*.gpkg")

def test_load_hexgrid(hex_grid_path):
    """Test hexgrid loading functionality"""
    gdf = summarize_biwf.load_hexgrid(hex_grid_path)
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert "hex_id" in gdf.columns
    assert gdf.crs == "EPSG:4326"
    assert gdf.sindex is not None

def test_process_file(hex_gdf, gpkg_paths):
    """Test processing of individual GPKG files"""
    result = summarize_biwf.process_file(gpkg_paths[0], hex_gdf)
    
    assert isinstance(result, pd.DataFrame)
    assert set(result.columns) == {"hex_id", "biwf_mean", "n_waveforms"}
    assert result.n_waveforms.sum() > 0  # Should have at least one valid measurement
    assert result.biwf_mean.isna().sum() == 0  # Ensure no nulls in results

    
def test_cli_full_flow(hex_grid_path, gpkg_pattern, tmp_path):
    """Test end-to-end CLI execution"""
    runner = CliRunner()
    output_path = tmp_path / "results.gpkg"
    
    result = runner.invoke(summarize_biwf.main, [
        "--gpkg-pattern", gpkg_pattern,
        "--hex-grid", hex_grid_path,
        "--output", str(output_path),
        # "--max-files", "10"
    ])
    
    assert result.exit_code == 0
    assert output_path.exists()
    
    gdf = gpd.read_file(output_path)
    assert not gdf.empty
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert set(gdf.columns) >= {"hex_id", "final_biwf", "geometry"}

def test_max_files_limit(hex_grid_path, gpkg_pattern, tmp_path):
    """Test processing limit with max-files parameter"""
    runner = CliRunner()
    output_path = tmp_path / "limited.gpkg"
    
    result = runner.invoke(summarize_biwf.main, [
        "--gpkg-pattern", gpkg_pattern,
        "--hex-grid", hex_grid_path,
        "--output", str(output_path),
        "--max-files", "2"
    ])
    
    assert result.exit_code == 0
    gdf = gpd.read_file(output_path)
    assert len(gdf) > 0  # Should process subset of files
    assert "geometry" in gdf.columns
