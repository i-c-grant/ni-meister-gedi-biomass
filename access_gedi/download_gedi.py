import os
import shutil
import tempfile
import time

import backoff
import fsspec
from maap.maap import MAAP

maap = MAAP(maap_host="api.maap-project.org")

def infer_product(filename: str) -> str:
    """Infer the product type from a GEDI filename."""
    # name_components = filename.split("_")

    if "GEDI01_B" in filename:
        return "l1b"

    elif "GEDI02_A" in filename:
        return "l2a"

    elif "GEDI04_A" in filename or "GEDI_L4A" in filename:
        return "l4a"

    else:
        raise ValueError(
            f"Unknown GEDI file type. "
            f"Expected 'GEDI01_B', 'GEDI02_A', 'GEDI04_A', or 'GEDI_L4A': "
            f"got filename {filename}"
        )

def gedi_filename_to_s3_url(filename: str) -> str:
    """Convert a GEDI filename to an s3 URL.
    Filename can include .h5 extension (as in the proper GEDI filenames)
    or not (as in the granule URs from the NASA CMR).
    """

    name_components = filename.split("_")

    gedi_type = infer_product(filename)

    if gedi_type == "l1b":
        base_s3 = "s3://lp-prod-protected/GEDI01_B.002"
    elif gedi_type == "l2a":
        base_s3 = "s3://lp-prod-protected/GEDI02_A.002"
    elif gedi_type == "l4a":
        base_s3 = ("s3://ornl-cumulus-prod-protected/gedi/"
                   "GEDI_L4A_AGB_Density_V2_1/data")

    # Distinguish between granule UR (no extension)
    # and filename (with extension)
    if filename.endswith(".h5"):
        granule_ur = filename[:-3]
    else:
        granule_ur = filename
        filename += ".h5"

    if gedi_type == "l4a":
        s3_url = f"{base_s3}/{filename}"
    else:
        s3_url = f"{base_s3}/{granule_ur}/{filename}"

    return s3_url


def open_s3_session(daac: str):
    """Get a new session token from MAAP and open an s3 filesystem"""
    if daac.lower() == 'lp':
        credentials_url = "https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials"
    elif daac.lower() == 'ornl':
        credentials_url = "https://data.ornldaac.earthdata.nasa.gov/s3credentials"
        
    credentials = maap.aws.earthdata_s3_credentials(credentials_url)

    s3 = fsspec.filesystem(
        "s3",
        key=credentials["accessKeyId"],
        secret=credentials["secretAccessKey"],
        token=credentials["sessionToken"],
    )

    return s3

def get_gedi_data(filename: str,
                  target_dir: str,
                  retries: int = 5,
                  max_delay: int = 120):
    """Download a GEDI file from S3 with exponential backoff.

    Args:
        filename (str): GEDI filename or s3 URL.
        target_dir (str): Directory to save the file.
        retries (int, optional): Number of retries. Defaults to 5.
        max_delay (int, optional): Max delay between retries. Defaults to 120.
"""

    # Prepare the output path
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    
    # Get the s3 URL if not already passed
    if filename.startswith("s3://"):
        s3_url = filename
        filename = os.path.basename(filename)
    else:
        s3_url = gedi_filename_to_s3_url(filename)

    output_path = os.path.join(target_dir, filename)

    if os.path.exists(output_path):
        raise FileExistsError(f"File already exists at {output_path}")

    file_type = infer_product(filename)
    if file_type  in ["l1b", "l2a"]:
        daac = 'lp'
    elif file_type == "l4a":
        daac = 'ornl'

    # Define the backoff handler
    @backoff.on_exception(
        backoff.expo,  # Exponential backoff
        Exception,  # Retry on any exception
        max_tries=retries,  # Max number of attempts
        max_time=max_delay  # Max total wait time
    )
    def download_file():
        s3 = open_s3_session(daac)

        # Use a temporary file to avoid partial downloads
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_fp = os.path.join(temp_dir, "tempfile")
            print(f"Downloading {s3_url} to {temp_fp}")
            s3.get(s3_url, temp_fp)
            print(f"Downloaded. Moving to final location: {output_path}")
            shutil.move(temp_fp, output_path)

    # Download the file with retries
    try:
        download_file()
    except Exception as e:
        print(f"All retries failed. Last error: {e}")
        raise

    print(f"File downloaded successfully to {output_path}")
    return output_path
