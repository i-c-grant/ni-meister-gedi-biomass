# Granule and path utilities
def extract_key_from_granule(granule: Granule) -> str:
    """Extract matching base key string from granule UR"""
    ur = granule["Granule"]["GranuleUR"]
    ur = ur[ur.rfind("GEDI"):]  # Get meaningful part
    parts = ur.split("_")[2:5]  # Get the key segments
    return "_".join(parts)  # Join with underscores as string


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

    s3_url = s3_urls[0]

    return s3_url


def granules_match(g1: Granule, g2: Granule) -> bool:
    """Check if two granules match using their extracted keys"""
    try:
        key1 = extract_key_from_granule(g1)
        key2 = extract_key_from_granule(g2)
        return key1 == key2
    except ValueError as e:
        raise ValueError(f"Granule matching failed: {str(e)}")


def stripped_granule_name(granule: Granule) -> str:
    return granule["Granule"]["GranuleUR"].strip().split(".")[0]

def query_granules(product: str,
                   date_range: str = None,
                   boundary: str = None) -> Dict[str, List[Granule]]:
    """
    Query granules from CMR and filter by date range and boundary
    Returns: Dictionary of lists of granules for each product
    """

    # Get collection IDs using the lookup function
    collection_id = get_collection_id(product)

    # Set up search parameters for CMR granule query
    host = "cmr.earthdata.nasa.gov"  # Define host here
    max_results = 10000
    search_kwargs = {
        "concept_id": collection_id,
        "cmr_host": host,
        "limit": max_results,
    }

    if date_range:
        search_kwargs["temporal"] = date_range

    if boundary:
        boundary_bbox: tuple = get_bounding_box(boundary)
        boundary_bbox_str: str = ",".join(map(str, boundary_bbox))
        search_kwargs["bounding_box"] = boundary_bbox_str

    # Query CMR for granules separately per product to handle response limits
    log_and_print("Searching for granules.")
    click.echo("(This may take a few minutes.)")

    granules = maap.searchGranule(**search_kwargs)

    log_and_print(f"Found {len(granules)} {product} granules.")

    return granules

# Hash each product's granules separately
    hashed_granules = {
        product_key: hash_granules(gran_list)
        for product_key, gran_list in product_granules.items()
    }

    # Find subset of keys that occur in all 3 products
    common_keys = (
        set(hashed_granules["l1b"])
        .intersection(hashed_granules["l2a"])
        .intersection(hashed_granules["l4a"])
    )

    # Build matched granules list
    matched_granules: List[Dict[str, Granule]] = []
    for key in common_keys:
        matched_granules.append({
            "l1b": hashed_granules["l1b"][key],
            "l2a": hashed_granules["l2a"][key],
            "l4a": hashed_granules["l4a"][key]
        })

    # Validate that we found matches
    if not matched_granules:
        raise ValueError("No matching granules found"
                         "across all three products")

    log_and_print(f"Found {len(matched_granules)} matching "
                  "sets of granules.")


def match_granules(
        product_granules: Dict[str: List[Granule]]
) -> List[Dict[str, Granule]]:
    # Hash each product's granules separately
    hashed_granules = {
        product_key: hash_granules(gran_list)
        for product_key, gran_list in product_granules.items()
    }

    # Find subset of keys that occur in all 3 products
    common_keys = (
        set(hashed_granules["l1b"])
        .intersection(hashed_granules["l2a"])
        .intersection(hashed_granules["l4a"])
    )

    # Build matched granules list
    matched_granules: List[Dict[str, Granule]] = []
    for key in common_keys:
        matched_granules.append({
            "l1b": hashed_granules["l1b"][key],
            "l2a": hashed_granules["l2a"][key],
            "l4a": hashed_granules["l4a"][key]
        })

    # Validate that we found matches
    if not matched_granules:
        raise ValueError("No matching granules found"
                         "across all three products")

    log_and_print(f"Found {len(matched_granules)} matching "
                  "sets of granules.")

    return matched_granules
