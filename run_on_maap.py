import click

import geopandas as gpd
from geopandas import GeoDataFrame
from maap.maap import MAAP
import maap.Result.Granule as Granule

maap = MAAP(maap_host='api.maap-project.org')

def granules_match(l1b: Granule, l2a: Granule):
    """Check if two L1B and L2A granules contain the same shots"""
    l1b_name = l1b['Granule']['GranuleUR']
    l2a_name = l2a['Granule']['GranuleUR']

    # Extract the unique, common part of filename
    # datetime of aquisition + orbit number + granule number within orbit
    # See <https://lpdaac.usgs.gov/data/get-started-data/collection-overview/
    # missions/gedi-overview/> for filename structure
    l1b_base = l1b_name.split("_")[2:5]
    l2a_base = l2a_name.split("_")[2:5]

    return l1b_base == l2a_base

@click.command()
@click.option("username", "-u", type=str, required=True)
@click.option("boundary", "-b", type=click.Path(exists=True),
              help=("Path to a shapefile or GeoPackage containing "
                    "a boundary polygon. Note: should be accessible "
                    "to MAAP DPS workers.")
@click.option("date_range", "-d", type=str,
              help=("Date range for granule search. "
                    "See <https://cmr.earthdata.nasa.gov/search/site/"
                    "docs/search/api.html#temporal-range-searches> "
                    "for valid formats."))
@click.option("job_limit", "-j", type=int,
              help="Limit the number of jobs submitted.")
def main(boundary: str, date_range: str):
    l1b_id = maap.SearchCollection(
        short_name="GEDI01_B",
        version="002",
        cmr_host="cmr.earthdata.nasa.gov",
        cloud_hosted=True
    )[0]['concept-id']

    l2a_id = maap.SearchCollection(
        short_name="GEDI02_A",
        version="002",
        cmr_host="cmr.earthdata.nasa.gov",
        cloud_hosted=True
    )[0]['concept-id']

    kwargs = {'concept_id'=[l1b_id, l2a_id]}

    if date_range:
        kwargs['temporal'] = date_range

    if boundary:
        # Get bounding box of boundary to restrict granule search
        boundary_gdf: GeoDataFrame = gpd.read_file(boundary)
        boundary_bbox: tuple = boundary_gdf.total_bounds
        boundary_bbox_str: str = ','.join(map(str, boundary_bbox))
        kwargs['bounding_box'] = boundary_bbox_str

    granules: List[Granule] = maap.SearchGranule(**kwargs)

    # pair corresponding L1B and L2A granules
    l1b_granules = (
        [granule if granule['collection']['short_name'] == 'GEDI01_B'
         for granule in granules]
    )

    l2a_granules = (
        [granule if granule['collection']['short_name'] == 'GEDI02_A'
         for granule in granules]
    )

    paired_granule_ids: List[Dict[str, str]] = []

    for l1b_granule in l1b_granules:
        for l2a_granule in l2a_granules:
            if granules_match(l1b_granule, l2a_granule):
                paired_granule_ids.append({"l1b": l1b_granule['concept-id'],
                                           "l2a": l2a_granule['concept-id']})
                break
                
    click.echo(f"Found {len(pairs)} matching pairs of granules.")

    # Submit jobs for each pair of granules
    jobs = []
    for pair in paired_granule_ids:
        job_kwargs = {
            "identifier": "nmbim_gedi_processing",
            "algo_id": "gedi_nmbim",
            "username": username,
            "queue": "maap-dps-worker-8gb",
            "l1b_id": pair['l1b'],
            "l2a_id": pair['l2a'],
        }

        if boundary:
            job_kwargs['boundary'] = boundary

        if date_range:
            job_kwargs['date_range'] = date_range

        job = maap.SubmitJob(**job_kwargs)
        jobs.append(job)
        if job_limit and len(jobs) >= job_limit:
            break

    print(f"Submitted {jobs_submitted} jobs.")
