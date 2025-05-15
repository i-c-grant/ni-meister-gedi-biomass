import logging
import warnings
import time
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional, Union

import boto3
import requests
import geopandas as gpd
from geopandas import GeoDataFrame
from maap.maap import MAAP
from maap.Result import Granule

from .RunConfig import RunConfig

maap = MAAP(maap_host="api.maap-project.org")


# Processing utilities
def get_existing_keys(config: RunConfig) -> Set[str]:
    """Get set of processed output keys from previous run"""
    s3 = boto3.client("s3")
    existing = set()

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket="maap-ops-workspace",
        Prefix=(
            f"{config.username}/dps_output/{config.algo_id}/"
            f"{config.algo_version}/{config.redo_tag}/"
        ),
    ):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".gpkg.bz2"):
                filename = Path(obj["Key"]).name
                key = filename.split(".")[0].strip()
                existing.add(key)

    return existing


def validate_redo_tag(config: RunConfig) -> None:
    """Validate redo tag parameters and check for existing outputs"""
    if not config.force_redo and config.redo_tag == config.tag:
        raise ValueError(
            f"Cannot redo with same tag '{config.tag}' "
            "- use --force-redo to override"
        )

    s3 = boto3.client("s3")
    prefix = (
        f"{config.username}/dps_output/{config.algo_id}/"
        f"{config.algo_version}/{config.redo_tag}/"
    )
    result = s3.list_objects_v2(
        Bucket="maap-ops-workspace", Prefix=prefix, MaxKeys=1
    )
    if not result.get("KeyCount"):
        raise ValueError(
            "No output directory found for " f"redo tag '{config.redo_tag}'"
        )


def get_bounding_box(boundary: str) -> tuple:
    """Get bounding box of a shapefile or GeoPackage."""
    boundary_path = s3_url_to_local_path(boundary)
    boundary_gdf: GeoDataFrame = gpd.read_file(boundary_path, driver="GPKG")
    return boundary_gdf.total_bounds


def prepare_job_kwargs(
    matched_granules: List[Dict[str, Granule]], config: RunConfig
):
    """Prepare job submission parameters for each triplet of granules."""
    job_kwargs_list = []
    n_jobs = (
        min(len(matched_granules), config.job_limit)
        if config.job_limit
        else len(matched_granules)
    )
    logging.info(f"Submitting {n_jobs} jobs.")

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
            "config": config.model_config,
            "hse": config.hse,
            "k_allom": config.k_allom,
        }
        if config.boundary:
            job_kwargs["boundary"] = config.boundary
        if config.date_range:
            job_kwargs["date_range"] = config.date_range
        job_kwargs_list.append(job_kwargs)
    return job_kwargs_list


# Granule utilities
def extract_key_from_granule(granule: Granule) -> str:
    """Extract matching base key string from granule UR"""
    ur = granule["Granule"]["GranuleUR"]
    ur = ur[ur.rfind("GEDI") :]  # Get meaningful part
    parts = ur.split("_")[2:5]  # Get the key segments
    return "_".join(parts)


def hash_granules(granules: List[Granule]) -> Dict[str, Granule]:
    """Create {base_key: granule} mapping with duplicate checking"""
    hashed = {}
    for granule in granules:
        key = extract_key_from_granule(granule)
        if key in hashed:
            raise ValueError(f"Duplicate base key {key} found in granules")
        hashed[key] = granule
    return hashed


def extract_s3_url_from_granule(granule: Granule) -> str:
    urls = granule["Granule"]["OnlineAccessURLs"]["OnlineAccessURL"]
    s3_urls = [url["URL"] for url in urls if url["URL"].startswith("s3")]
    if len(s3_urls) > 1:
        warnings.warn(f"Multiple S3 URLs found in granule: {s3_urls}")
    return s3_urls[0]


def get_collection_id(product: str) -> str:
    """Get collection ID for a GEDI product (l1b/l2a/l4a)"""
    host = "cmr.earthdata.nasa.gov"
    product_map = {
        "l1b": ("GEDI01_B", "002"),
        "l2a": ("GEDI02_A", "002"),
        "l4a": ("GEDI_L4A_AGB_Density_V2_1_2056", None),
    }
    short_name, version = product_map[product]
    params = {
        "short_name": short_name,
        "cmr_host": host,
        "cloud_hosted": "true",
    }
    if version:
        params["version"] = version

    try:
        results = maap.searchCollection(**params)
        if not results:
            raise ValueError(
                f"No collections found for {product} ({short_name} v{version})"
            )
        return results[0]["concept-id"]
    except Exception as e:
        logging.error(f"Failed to get collection ID for {product}: {str(e)}")
        logging.error("Verify the product name and parameters are correct")
        if "Could not parse XML response" in str(e):
            logging.error(
                "CMR returned invalid XML - service may be unavailable"
            )
        raise RuntimeError(f"Collection ID lookup failed for {product}") from e


def granules_match(g1: Granule, g2: Granule) -> bool:
    """Check if two granules match using their extracted keys"""
    try:
        return extract_key_from_granule(g1) == extract_key_from_granule(g2)
    except ValueError as e:
        raise ValueError(f"Granule matching failed: {str(e)}")


def stripped_granule_name(granule: Granule) -> str:
    return granule["Granule"]["GranuleUR"].strip().split(".")[0]


def query_granules(
    product: str,
    date_range: str = None,
    boundary: str = None,
    limit: int = None,
) -> Dict[str, List[Granule]]:
    """Query granules from CMR and filter by date range and boundary"""
    collection_id = get_collection_id(product)
    response_limit = min(limit, 10000) if limit else 10000
    search_kwargs = {
        "concept_id": collection_id,
        "cmr_host": "cmr.earthdata.nasa.gov",
        "limit": response_limit,
    }
    if date_range:
        search_kwargs["temporal"] = date_range
    if boundary:
        boundary_bbox: tuple = get_bounding_box(boundary)
        search_kwargs["bounding_box"] = ",".join(map(str, boundary_bbox))

    logging.info("Searching for granules...")

    # Add retry logic with backoff and error handling
    max_retries = 3
    retry_delay = 10  # seconds
    for attempt in range(max_retries):
        try:
            granules = maap.searchGranule(**search_kwargs)
            # Check for HTTP errors in response
            if (
                hasattr(granules, "response")
                and granules.response.status_code >= 400
            ):
                if granules.response.status_code == 429:
                    logging.error("CMR rate limit exceeded - try again later")
                    raise RuntimeError("CMR rate limit exceeded")
                granules.response.raise_for_status()

            logging.info(f"Found {len(granules)} {product} granules.")
            return granules
        except Exception as e:
            if attempt < max_retries - 1:
                logging.warning(
                    f"CMR query failed (attempt {attempt+1}/{max_retries}): {str(e)}"
                )
                logging.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
                continue
            logging.error("Failed to query CMR after multiple attempts")
            logging.error("This could be due to:")
            logging.error(
                "- CMR service outage (check https://cmr.earthdata.nasa.gov/health)"
            )
            logging.error("- Rate limiting (try again later)")
            logging.error("- Invalid query parameters")
            raise RuntimeError(f"CMR query failed: {str(e)}") from e


def match_granules(
    product_granules: Dict[str, List[Granule]]
) -> List[Dict[str, Granule]]:
    """Match granules across products"""
    hashed_granules = {
        product_key: hash_granules(gran_list)
        for product_key, gran_list in product_granules.items()
    }
    common_keys = (
        set(hashed_granules["l1b"])
        .intersection(hashed_granules["l2a"])
        .intersection(hashed_granules["l4a"])
    )
    return [
        {
            "l1b": hashed_granules["l1b"][key],
            "l2a": hashed_granules["l2a"][key],
            "l4a": hashed_granules["l4a"][key],
        }
        for key in common_keys
    ]


def exclude_redo_granules(
    matched_granules: List[Dict[str, Granule]], config: RunConfig
):
    """Prune already processed granules"""
    exclude_keys = get_existing_keys(config)
    if not exclude_keys:
        logging.info("No existing outputs found - processing all granules")
        return matched_granules

    pre_count = len(matched_granules)
    matched_granules = [
        matched
        for matched in matched_granules
        if extract_key_from_granule(matched["l1b"]) not in exclude_keys
    ]
    logging.info(f"Excluded {pre_count - len(matched_granules)} granules")
    return matched_granules


def s3_url_to_local_path(s3_url: str) -> str:
    """Convert MAAP S3 URLs to local filesystem paths"""
    if not s3_url.startswith("s3://maap-ops-workspace/"):
        raise ValueError("URL must start with s3://maap-ops-workspace/")

    path = s3_url.replace("s3://maap-ops-workspace/", "")
    if path.startswith("shared/"):
        _, username, *rest = path.split("/")
        bucket = "my-public-bucket"
    else:
        username, *rest = path.split("/")
        bucket = "my-private-bucket"
    return f"/projects/{bucket}/{'/'.join(rest)}"
