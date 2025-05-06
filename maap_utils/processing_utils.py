from pathlib import Path
from typing import Set
import logging
import click
from maap import MAAP
from .RunConfig import RunConfig

maap = MAAP(maap_host="api.maap-project.org")

def get_existing_keys(config: RunConfig) -> Set[str]:
    """Get set of processed output keys from previous run

    Note: Assumes that the output GeoPackages are named consistent with the
    keys specified in extract_key_from_granule and that the outputs are
    compressed, i.e. <key>.gpkg.bz2 (this is the current behavior of
    WaveformWriter).
    """
    s3 = boto3.client('s3')
    existing = set()

    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(
        Bucket="maap-ops-workspace",
        Prefix=(f"{config.username}/dps_output/{config.algo_id}/"
                "{config.algo_version}/{config.redo_tag}/")
    ):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.gpkg.bz2'):
                # Extract just the filename without path or extensions
                filename = Path(obj['Key']).name
                key = filename.split(".")[0].strip()
                existing.add(key)

    return existing


def s3_url_to_local_path(s3_url: str) -> str:
    """
    Converts MAAP S3 URLs to local filesystem paths.

    Args:
        s3_url: S3 URL starting with s3://maap-ops-workspace/

    Returns:
        Local filesystem path
    """
    if not s3_url.startswith("s3://maap-ops-workspace/"):
        raise ValueError("URL must start with s3://maap-ops-workspace/")

    # Remove the s3://maap-ops-workspace/ prefix
    path = s3_url.replace("s3://maap-ops-workspace/", "")

    # Extract username and determine bucket based on path structure
    if path.startswith("shared/"):
        _, username, *rest = path.split("/")
        bucket = "my-public-bucket"
        path = "/".join(rest)
    else:
        username, *rest = path.split("/")
        bucket = "my-private-bucket"
        path = "/".join(rest)

    return f"/projects/{bucket}/{path}"


# processing utilities
def validate_redo_tag(config: RunConfig) -> None:
    """Validate redo tag parameters and check for existing outputs"""
    if not config.force_redo and config.redo_tag == config.tag:
        raise ValueError(
            f"Cannot redo with same tag '{config.tag}' "
            "- use --force-redo to override"
        )

    # Verify S3 path exists
    s3 = boto3.client('s3')
    prefix = (f"{config.username}/dps_output/{config.algo_id}/"
              f"{config.algo_version}/{config.redo_tag}/")
    result = s3.list_objects_v2(
        Bucket="maap-ops-workspace",
        Prefix=prefix,
        MaxKeys=1  # Just check existence
    )
    if not result.get('KeyCount'):
        raise ValueError("No output directory found for "
                         f"redo tag '{redo_tag}'")


def get_collection_id(product: str) -> str:
    """Get collection ID for a GEDI product (l1b/l2a/l4a)"""
    host = "cmr.earthdata.nasa.gov"
    product_map = {
        "l1b": ("GEDI01_B", "002"),
        "l2a": ("GEDI02_A", "002"),
        "l4a": ("GEDI_L4A_AGB_Density_V2_1_2056", None)
    }
    short_name, version = product_map[product]
    params = {
        "short_name": short_name,
        "cmr_host": host,
        "cloud_hosted": "true"
    }
    if version:
        params["version"] = version
    return maap.searchCollection(**params)[0]["concept-id"]


def get_bounding_box(boundary: str) -> tuple:
    """
    Get bounding box of a shapefile or GeoPackage.

    Args:
        boundary: s3 path to the boundary shapefile or GeoPackage.

    Returns:
        Bounding box as a tuple (minx, miny, maxx, maxy).
    """
    boundary_path = s3_url_to_local_path(boundary)
    boundary_gdf: GeoDataFrame = gpd.read_file(boundary_path,
                                               driver="GPKG")
    bbox: tuple = boundary_gdf.total_bounds
    return bbox


def prepare_job_kwargs(
        matched_granules: List[Dict[str, Granule]],
        config: RunConfig
):
    """Prepare job submission parameters for each triplet of granules."""

    job_kwargs_list = []
    if config.job_limit:
        n_jobs = min(len(matched_granules), config.job_limit)
    else:
        n_jobs = len(matched_granules)
    log_and_print(f"Submitting {n_jobs} " f"jobs.")

    job_kwargs_list = []
    for matched in matched_granules:
        job_kwargs = {
            "identifier": config.tag,
            "algo_id": config.algo_id,
            "version": config.algo_version,
            "username": config.username,
            "queue": "maap-dps-worker-16gb",
            "L1B": extract_s3_url_from_granule(matched["l1b"]),
            "L2A": extract_s3_url_from_granule(matched["l2a"]),
            "L4A": extract_s3_url_from_granule(matched["l4a"]),
            "config": config.model_config,  # Pass S3 URL directly
            "hse": config.hse,
            "k_allom": config.k_allom
        }

        if config.boundary:
            job_kwargs["boundary"] = config.boundary  # Pass S3 URL directly

        if config.date_range:
            job_kwargs["date_range"] = config.date_range

        job_kwargs_list.append(job_kwargs)

    return job_kwargs_list
