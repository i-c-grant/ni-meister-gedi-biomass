"""
Download GEDI data for a geographic area using Earthdata HTTPS endpoints.

This script handles:
- Earthdata authentication
- Searching CMR for granules in a geographic area
- Downloading matching granules
- Progress tracking and error handling

Usage:
    python download_gedi_area.py --username <earthdata_username> --password <earthdata_password>
        --boundary <boundary_file> --output_dir <output_directory>
        [--product <l1b|l2a|l4a>] [--date_range <YYYY-MM-DD,YYYY-MM-DD>]
        [--max_granules <number>]
"""

import os
import time
import logging
from pathlib import Path
from typing import List, Optional, Tuple
import subprocess

import click
import geopandas as gpd
import requests
from requests.auth import HTTPBasicAuth
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

from cmr import CollectionQuery, GranuleQuery

def get_cmr_granules(
    product: str,
    boundary: gpd.GeoDataFrame,
    date_range: Optional[Tuple[str, str]] = None,
    max_granules: int = 1000
) -> List[dict]:
    """
    Search CMR for granules within a geographic boundary using python_cmr.
    
    Args:
        product: GEDI product type (l1b, l2a, l4a)
        boundary: GeoDataFrame containing boundary polygon
        date_range: Optional tuple of start/end dates (YYYY-MM-DD)
        max_granules: Maximum number of granules to return
        
    Returns:
        List of granule metadata dictionaries
    """
    # First verify collections exist using CollectionQuery
    collection_query = CollectionQuery()
    
    # Get collection short names from run_on_maap.py
    collection_map = {
        "l1b": "GEDI01_B",
        "l2a": "GEDI02_A",
        "l4a": "GEDI_L4A_AGB_Density_V2_1_2056"
    }
    
    # Get bounding box from boundary
    bbox = boundary.total_bounds

    # Create GranuleQuery
    api = GranuleQuery()
    
    # Set collection and spatial filter
    api.short_name(collection_map[product])
    api.bounding_box(*bbox)
    
    # Set temporal filter if provided
    if date_range:
        api.temporal(*date_range)
        
    # Get results
    granules = api.get(max_granules)

    # Convert to list of dicts with download links
    results = []
    for granule in granules:
        result = {
            "id": granule.get("producer_granule_id"),
            "url": granule["links"][0]["href"]
        }
        results.append(result)

    return results

def download_granule(
        granule_id: str,
        granule_url: str,
        output_dir: Path,
        auth: HTTPBasicAuth,
        retries: int = 3,
        delay: int = 5
) -> Path:
    """
    Download a single granule using Earthdata HTTPS.
    
    Args:
        granule: Granule metadata from CMR
        output_dir: Directory to save the file
        auth: Earthdata authentication
        retries: Number of download retry attempts
        delay: Delay between retries in seconds
        
    Returns:
        Path to downloaded file
    """
    # Download with wget
    subprocess.run(["wget",
                    "-P", output_dir,
                    granule_url,
                    "--user", auth.username,
                    "--password", auth.password,
                    "-q", "--show-progress"])
    
    
    return output_path

@click.command()
@click.option("--username", "-u", required=True, help="Earthdata username")
@click.option("--password", "-p", required=True, help="Earthdata password")
@click.option("--boundary", "-b", required=True, 
              help="Path to boundary file (GeoPackage or Shapefile)")
@click.option("--output_dir", "-o", required=True,
              help="Directory to save downloaded files")
@click.option("--product", "-p", default="l1b",
              type=click.Choice(["l1b", "l2a", "l4a"]),
              help="GEDI product type to download")
@click.option("--date_range", "-d", 
              help="Date range in format YYYY-MM-DD,YYYY-MM-DD")
@click.option("--max_granules", "-m", default=100,
              help="Maximum number of granules to download")
def main(
    username: str,
    password: str,
    boundary: str,
    output_dir: str,
    product: str,
    date_range: Optional[str],
    max_granules: int
):
    """Main function to download GEDI data for an area."""
    # Prepare authentication
    auth = HTTPBasicAuth(username, password)
    
    # Read boundary file
    boundary_gdf = gpd.read_file(boundary)
    
    # Parse date range if provided
    date_range_tuple = None
    if date_range:
        start_date, end_date = date_range.split(",")
        date_range_tuple = (start_date.strip(), end_date.strip())
    
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Search for granules
    logging.info("Searching CMR for matching granules...")
    granules = get_cmr_granules(
        product=product,
        boundary=boundary_gdf,
        date_range=date_range_tuple,
        max_granules=max_granules
    )
    
    if not granules:
        logging.warning("No granules found matching search criteria")
        return
    
    logging.info(f"Found {len(granules)} granules to download")
    
    # Download granules
    for granule in granules:
        try:
            logging.info(f"Downloading {granule['producer_granule_id']}")
            download_granule(granule, output_path, auth)
        except Exception as e:
            logging.error(f"Failed to download {granule['producer_granule_id']}: {str(e)}")
            continue

if __name__ == "__main__":
    main()
