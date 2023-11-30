1import sys
import h5py
import boto3
import botocore
import fsspec
from maap.maap import MAAP
maap = MAAP(maap_host="api.maap-project.org")

def lpdaac_gedi_https_to_s3(url):
    dir_comps = url.split("/")
    return f"s3://lp-prod-protected/{dir_comps[6]}/{dir_comps[8].strip('.h5')}/{dir_comps[8]}"

def get_gedi_data(url):
    credentials = maap.aws.earthdata_s3_credentials(
        'https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials'
    )

    s3 = fsspec.filesystem(
        "s3",
        key=credentials['accessKeyId'],
        secret=credentials['secretAccessKey'],
        token=credentials['sessionToken']
    )
    with s3.open(lpdaac_gedi_https_to_s3(url), "rb") as f:
        gedi_ds = h5py.File(f, "r")
        
    return gedi_ds