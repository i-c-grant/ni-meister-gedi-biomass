from maap.maap import MAAP
maap = MAAP(maap_host='api.maap-project.org')

def get_collection_id(product: str):
    if product == 'l1b':
        short_name = "GEDI01_B"
    elif product == 'l2a':
        short_name = "GEDI02_A"
    else:
        raise ValueError(f"Invalid product: {product}"
                         f"Expected 'l1b' or 'l2a'")

    collection = maap.SearchCollection(
        short_name=short_name,
        version="002",
        cmr_host="cmr.earthdata.nasa.gov",
        cloud_hosted=True
    )[0]

    return collection['concept-id']

def find_unique_granule(granule_ur: str,
                        collection_id: str):

    """Given a granule UR and collection ID, find the unique granule
    that matches the UR in that collection.

    Note: the UR is equivalent to the filename without the h5 extension.
    """
    
    granules = maap.SearchGranule(granule_ur=granule_ur,
                                  concept_id=collection_id,
                                  cmr_host='cmr.earthdata.nasa.gov')
    if len(granules) == 0:
        raise ValueError(f'No granules found for {granule_ur}')
    if len(granules) > 1:
        raise ValueError(f'Multiple granules found for {granule_ur}')

    unique_granule = granules[0]
    return unique_granule




