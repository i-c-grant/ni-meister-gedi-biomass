import os
import shutil
import tempfile
import time

import fsspec
from maap.maap import MAAP

maap = MAAP(maap_host="api.maap-project.org")


def gedi_filename_to_s3_url(filename: str) -> str:
    """Convert a GEDI filename to an s3 URL"""
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

    basename = filename.split(".")[0]  # remove the .h5 extension

    return f"{base_s3}/{basename}/{filename}"


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


def get_gedi_data(filename: str, target_dir: str, retries: int = 3):
    """Download a GEDI file from the MAAP s3 bucket"""

    attempt = 0
    delay = 5

    # Open the s3 filesystem and get the s3 URL
    s3 = open_s3_session()
    s3_url = gedi_filename_to_s3_url(filename)

    # Prepare the output path
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    output_path = os.path.join(target_dir, filename)
    if os.path.exists(output_path):
        raise FileExistsError(f"File already exists at {output_path}")

    # Download the file
    while attempt < retries:
        try:
            # Use a temporary file to avoid partial downloads
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_fp = os.path.join(temp_dir, "tempfile")
                print(f"Downloading {s3_url} to {temp_fp}")
                s3.get(s3_url, temp_fp)
                print(f"Downloaded. Moving to final location: {output_path}")
                shutil.move(temp_fp, output_path)
                break
        except Exception as e:
            attempt += 1
            print(f"Attempt {attempt} failed: {e}")
            if attempt < retries:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("All retries failed.")
                raise

    print(f"File downloaded successfully to {output_path}")

    return output_path
