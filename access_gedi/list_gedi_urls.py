"""
List GEDI data URLs for a geographic area using CMR search.

Usage:
    python list_gedi_urls.py --product <l1b|l2a|l4a> 
        --boundary <boundary_file> --output <output_file>
        [--date_range <YYYY-MM-DD,YYYY-MM-DD>]
"""

import logging
from pathlib import Path
from typing import List, Optional, Tuple

import click
import geopandas as gpd
from cmr import CollectionQuery, GranuleQuery

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def get_cmr_granules(
    product: str,
    boundary: gpd.GeoDataFrame,
    date_range: Optional[Tuple[str, str]] = None,
    max_granules: int = 1000
) -> List[str]:
    """
    Search CMR for granules within a geographic boundary and return URLs.
    
    Args:
        product: GEDI product type (l1b, l2a, l4a)
        boundary: GeoDataFrame containing boundary polygon
        date_range: Optional tuple of start/end dates (YYYY-MM-DD)
        max_granules: Maximum number of granules to return
        
    Returns:
        List of granule URLs
    """
    # Map product to collection concept IDs
    concept_map = {
        "l1b": "C1234567890-NSIDC",
        "l2a": "C0987654321-NSIDC",
        "l4a": "C1122334455-NSIDC"
    }
    
    # Get bounding box from boundary
    bbox = boundary.total_bounds

    # Create GranuleQuery
    api = GranuleQuery()
    
    # Set collection using concept ID and spatial filter
    api.concept_id(concept_map[product])
    api.bounding_box(*bbox)
    
    # Set temporal filter if provided
    if date_range:
        api.temporal(*date_range)
        
    # Get results and extract URLs
    granules = api.get(max_granules)
    return [granule["links"][0]["href"] for granule in granules]

@click.command()
@click.option("--product", "-p", required=True,
              type=click.Choice(["l1b", "l2a", "l4a"]),
              help="GEDI product type to search for")
@click.option("--boundary", "-b", required=True, 
              help="Path to boundary file (GeoPackage or Shapefile)")
@click.option("--output", "-o", required=True,
              help="Path to output text file for URLs")
@click.option("--date_range", "-d", 
              help="Date range in format YYYY-MM-DD,YYYY-MM-DD")
def main(
    product: str,
    boundary: str,
    output: str,
    date_range: Optional[str]
):
    """Main function to list GEDI URLs for an area."""
    # Read boundary file
    boundary_gdf = gpd.read_file(boundary)
    
    # Parse date range if provided
    date_range_tuple = None
    if date_range:
        start_date, end_date = date_range.split(",")
        date_range_tuple = (start_date.strip(), end_date.strip())
    
    # Search for granules
    logging.info("Searching CMR for matching granules...")
    urls = get_cmr_granules(
        product=product,
        boundary=boundary_gdf,
        date_range=date_range_tuple
    )
    
    if not urls:
        logging.warning("No granules found matching search criteria")
        return
    
    logging.info(f"Found {len(urls)} granules")
    
    # Write URLs to output file
    output_path = Path(output)
    with output_path.open("w") as f:
        f.write("\n".join(urls))
    
    logging.info(f"URLs written to {output_path}")

if __name__ == "__main__":
    main()
