"""
Test the GEDI area download functionality using test input files.
"""
from pathlib import Path
TEST_DIR = Path(__file__).parent
SRC_DIR = TEST_DIR.parent

import sys
sys.path.append(str(SRC_DIR))

import os
import shutil
import pytest
import geopandas as gpd
from requests.auth import HTTPBasicAuth

from access_gedi.download_gedi_area import (
    get_cmr_granules,
    download_granule
)

# Test data paths
INPUT_DIR = TEST_DIR / "input"
OUTPUT_DIR = TEST_DIR / "output"

# Test boundary file
TEST_BOUNDARY = INPUT_DIR / "lope_bounding_box.gpkg"

@pytest.fixture(scope="module")
def test_output_dir():
    """Create and clean up test output directory."""
    output_dir = OUTPUT_DIR / "test_download_gedi_area"
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)
    yield output_dir
    shutil.rmtree(output_dir)

def test_get_cmr_granules(test_output_dir):
    """Test CMR granule search with test boundary."""
    # Load test boundary
    boundary = gpd.read_file(TEST_BOUNDARY)
    
    # Test all product types from run_on_maap.py
    products = ["l1b", "l2a", "l4a"]
    
    for product in products:
        # Search for granules
        granules = get_cmr_granules(
            product=product,
            boundary=boundary,
            date_range=("2020-01-01", "2020-12-31"),
            max_granules=10
        )
        
        # Verify we got results
        assert len(granules) > 0
        
        # Verify granule structure
        granule = granules[0]
        assert "id" in granule
        assert "url" in granule

        # Print out URLs
        print([granule["url"] for granule in granules])

def test_download_granule(test_output_dir):
    """Test downloading a single granule."""
    # Get credentials from environment
    username = os.getenv("EARTHDATA_USERNAME")
    password = os.getenv("EARTHDATA_PASSWORD")
    
    if not username or not password:
        pytest.skip("Earthdata credentials not provided in environment variables")
    
    auth = HTTPBasicAuth(username, password)
    
    # Load test boundary
    boundary = gpd.read_file(TEST_BOUNDARY)
    
    # Get test granule
    granules = get_cmr_granules(
        product="l2a",
        boundary=boundary,
        date_range=("2020-01-01", "2020-12-31"),
        max_granules=1
    )
    
    # Skip if no granules found
    if not granules:
        pytest.skip("No granules found for test area/date range")
    
    # Test download
    granule = granules[0]
    output_path = download_granule(
        granule_id=granule["id"],
        granule_url=granule["url"],
        output_dir=test_output_dir,
        auth=auth,
        retries=1
    )
    
    # Verify file was downloaded
    assert output_path.exists()
    assert output_path.stat().st_size > 0

def test_integration(test_output_dir):
    """Test the full download workflow."""
    # This would test the main() function but requires:
    # 1. Earthdata credentials
    # 2. Mocking of the download process
    # 3. Proper error handling
    pytest.skip("Integration test not implemented yet")
