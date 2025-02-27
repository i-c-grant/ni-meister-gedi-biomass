import pytest
from datetime import datetime
from pathlib import Path
import h5py
from click.testing import CliRunner
from process_gedi_granules import main as process_gedi_granules
import yaml
from shapely.geometry import box

from util.raster_utils import create_global_raster, create_polygon_raster

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
def temp_data_dir(tmp_path_factory):
    """Fixture that creates a complete temporary data directory structure"""
    temp_dir = tmp_path_factory.mktemp("test_data")
    
    # Create directory structure
    (temp_dir / "allom").mkdir()
    (temp_dir / "gedi").mkdir()
    (temp_dir / "boundaries").mkdir()
    (temp_dir / "config").mkdir()
    
    return temp_dir

@pytest.fixture(scope="session")
def hse_raster(temp_data_dir, full_boundary_polygon):
    """Fixture for HSE raster using full boundary coverage"""
    path = temp_data_dir / "allom" / "hse_full.tif"
    # Create HSE raster with value 1.0 over full boundary polygon
    create_polygon_raster(
        path,
        full_boundary_polygon,
        value=1.0,
        resolution=(0.5, 0.5),
        dtype="float32"
    )
    return path

@pytest.fixture(scope="session")
def hse_raster_half(temp_data_dir, half_boundary_polygon):
    """Fixture for HSE raster using half boundary coverage"""
    path = temp_data_dir / "allom" / "hse_half.tif"
    # Create HSE raster with value 1.0 over half boundary polygon
    create_polygon_raster(
        path,
        half_boundary_polygon,
        value=1.0,
        resolution=(0.5, 0.5),
        dtype="float32"
    )
    return path

@pytest.fixture(scope="session")
def full_boundary_polygon(boundary_file):
    """Fixture for polygon covering full boundary extent"""
    import geopandas as gpd
    # Load boundary and get its extent
    boundary = gpd.read_file(boundary_file)
    minx, miny, maxx, maxy = boundary.total_bounds
    
    # Create polygon covering full boundary extent
    full_poly = box(minx, miny, maxx, maxy)
    
    return full_poly

@pytest.fixture(scope="session")
def half_boundary_polygon(full_boundary_polygon):
    """Fixture for polygon covering only half the boundary extent"""
    import geopandas as gpd
    # Load boundary and get its extent

    minx, miny, maxx, maxy = full_boundary_polygon.bounds
    half_poly = box(minx, miny, (maxx + minx) / 2, maxy)
    
    return half_poly


@pytest.fixture(scope="session")
def k_allom_raster(temp_data_dir, full_boundary_polygon):
    """Fixture for K_allom raster using full boundary coverage"""
    path = temp_data_dir / "allom" / "k_allom_full.tif"
    # Create K_allom raster with value 2.0 over full boundary polygon
    create_polygon_raster(
        path,
        full_boundary_polygon,
        value=2.0,
        resolution=(0.5, 0.5),
        dtype="float32"
    )
    return path


@pytest.fixture(scope="session")
def k_allom_raster_half(temp_data_dir, half_boundary_polygon):
    """Fixture for K_allom raster using half boundary coverage"""
    path = temp_data_dir / "allom" / "k_allom_half.tif"
    # Create K_allom raster with value 2.0 over half boundary polygon
    create_polygon_raster(
        path,
        half_boundary_polygon,
        value=2.0,
        resolution=(0.5, 0.5),
        dtype="float32"
    )
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

def test_full_coverage_raster(l1b_file, l2a_file, l4a_file, hse_raster, k_allom_raster, default_hse, default_k_allom, config_file, output_dir):
    """Test parameter loading with full coverage raster"""
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
        "--n_workers", "8",
        "--parallel"
    ])
    assert result.exit_code == 0
    
    # Verify all waveforms got HSE values from raster
    # (Actual verification would depend on your output format)

def test_partial_coverage_raster(l1b_file, l2a_file, l4a_file, k_allom_raster_half, default_hse, default_k_allom, config_file, output_dir):
    """Test parameter loading with partial coverage raster"""
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
        "--n_workers", "8",
        "--parallel"
    ])
    assert result.exit_code == 0
    
    # Verify some waveforms got K_allom from raster, others used default
    # (Actual verification would depend on your output format)

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
        "--n_workers", "8",
        "--parallel"
    ])
    assert result.exit_code == 0
    
    # Verify waveforms got:
    # - HSE from full coverage raster
    # - K_allom from partial coverage raster where available, else default
    # (Actual verification would depend on your output format)

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
        "--n_workers", "8",
        "--parallel"
    ])
    assert result.exit_code == 0
    
    # Verify all waveforms used default parameter values
    # (Actual verification would depend on your output format)

def test_verify_output_gpkg_parameters(output_dir):
    """Verify parameter values in output geopackages from all tests"""
    import geopandas as gpd
    
    # Find all output geopackages from previous tests
    test_dirs = [d for d in Path("tests/output").iterdir() if d.is_dir()]
    
    # Verify each test's output
    for test_dir in test_dirs:
        output_gpkg = next(test_dir.glob("*.gpkg"))
        
        # Read back and verify
        gdf = gpd.read_file(output_gpkg)
        assert not gdf.empty
        assert "hse" in gdf.columns
        assert "k_allom" in gdf.columns
        
        # Get expected values based on test name
        test_name = test_dir.name.split('_')[0]
        
        if test_name == "test_full_coverage_raster":
            # Should have HSE from raster (1.0) and K_allom from raster (2.0)
            assert set(gdf["hse"].values) == {1.0}
            assert set(gdf["k_allom"].values) == {2.0}
            
        elif test_name == "test_partial_coverage_raster":
            # Should have default HSE (5.0) and mix of K_allom from raster (2.0) and default (10.0)
            assert set(gdf["hse"].values) == {5.0}
            assert set(gdf["k_allom"].values) == {2.0, 10.0}
            
        elif test_name == "test_mixed_parameters":
            # Should have HSE from raster (1.0) and mix of K_allom from raster (2.0) and default (10.0)
            assert set(gdf["hse"].values) == {1.0}
            assert set(gdf["k_allom"].values) == {2.0, 10.0}
            
        elif test_name == "test_default_parameters_only":
            # Should have default HSE (5.0) and default K_allom (10.0)
            assert set(gdf["hse"].values) == {5.0}
            assert set(gdf["k_allom"].values) == {10.0}
            
        # Verify data types
        assert all(isinstance(v, float) for v in gdf["hse"].values)
        assert all(isinstance(v, float) for v in gdf["k_allom"].values)
