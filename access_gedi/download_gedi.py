import os
import shutil
import tempfile
import time

import backoff
import fsspec
from maap.maap import MAAP

maap = MAAP(maap_host="api.maap-project.org")


def gedi_filename_to_s3_url(filename: str) -> str:
    """Convert a GEDI filename to an s3 URL.
    Filename can include .h5 extension (as in the proper GEDI filenames)
    or not (as in the granule URs from the NASA CMR).
    """
    name_components = filename.split("_")

    if name_components[0:2] == ["GEDI01", "B"]:
        gedi_type = "l1b"
    elif name_components[0:2] == ["GEDI02", "A"]:
        gedi_type = "l2a"
    else:
        raise ValueError(
            f"Unknown GEDI file type. "
            f"Expected 'GEDI01_B' or 'GEDI02_A', "
            f"got {name_components[0]}_{name_components[1]}"
        )

    base_s3 = "s3://lp-prod-protected"
    if gedi_type == "l1b":
        base_s3 += "/GEDI01_B.002"
    elif gedi_type == "l2a":
        base_s3 += "/GEDI02_A.002"

    # Distinguish between granule UR (no extension) and filename (with extension)
    if filename.endswith(".h5"):
        granule_ur = filename[:-3]
    else:
        granule_ur = filename
        filename += ".h5"

    return f"{base_s3}/{granule_ur}/{filename}"


def open_s3_session():
    """Get a new session token from MAAP and open an s3 filesystem"""
    credentials = maap.aws.earthdata_s3_credentials(
        "https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials"
    )

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
    """Download a GEDI file from S3 with exponential backoff"""

    # Prepare the output path
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    output_path = os.path.join(target_dir, filename)
    if os.path.exists(output_path):
        raise FileExistsError(f"File already exists at {output_path}")

    # Open the S3 filesystem and get the S3 URL
    s3_url = gedi_filename_to_s3_url(filename)

    # Define the backoff handler
    @backoff.on_exception(
        backoff.expo,  # Exponential backoff
        Exception,  # Retry on any exception
        max_tries=retries,  # Max number of attempts
        max_time=max_delay  # Max total wait time
    )
    def download_file():
        s3 = open_s3_session()

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
