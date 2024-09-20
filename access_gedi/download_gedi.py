from maap.maap import MAAP
import h5py
import boto3
import botocore
import fsspec
import s3fs
from pathlib import Path
from typing import Union, List, Dict, Any

maap = MAAP(maap_host="api.maap-project.org")

def gedi_filename_to_s3_url(filename: str) -> str:
    name_components = filename.split("_")

    if name_components[0:2] == ["GEDI01", "B"]:
        gedi_type = "l1b"
    elif name_components[0:2] == ["GEDI02", "A"]:
        gedi_type = "l2a"
    else:
        raise ValueError(f"Unknown GEDI file type. "
                         f"Expected 'GEDI01_B' or 'GEDI02_A', "
                         f"got {name_components[0]}_{name_components[1]}")

    base_s3 = "s3://lp-prod-protected"
    if gedi_type == "l1b":
        base_s3 += "/GEDI01_B.002"
    elif gedi_type == "l2a":
        base_s3 += "/GEDI02_A.002"

    basename = filename.split(".")[0]  # remove the .h5 extension

    return f"{base_s3}/{basename}/{filename}"

def copy_gedi_file(s3_url: str, s3: fsspec.filesystem) -> str:
    basename = os.path.basename(url)
    outfp = f"/projects/my-private-bucket/{basename}"
    print("Copying file...")
    s3.get(url, outfp)
    print("File copied.")
    return outfp

def get_gedi_data(filename: str):
    credentials = maap.aws.earthdata_s3_credentials(
        'https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials'
    )

    s3 = fsspec.filesystem(
        "s3",
        key=credentials['accessKeyId'],
        secret=credentials['secretAccessKey'],
        token=credentials['sessionToken']
    )

    outfp = f"output/{filename}"
    s3_url = gedi_filename_to_s3_url(filename)
    copy_gedi_file(s3_url, s3)
    
