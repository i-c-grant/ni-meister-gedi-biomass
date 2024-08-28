import os
# import the MAAP package
from maap.maap import MAAP
import sys


def download_gedi(url,shortname):
    # Get output dir
    # invoke the MAAP constructor using the maap_host argument
    maap = MAAP(maap_host='api.maap-project.org')
    granule_name = os.path.basename(url)

    # Search for granule data using CMR host name and download
    results = maap.searchGranule(
        cmr_host='cmr.earthdata.nasa.gov',
        short_name=shortname,
        readable_granule_name = granule_name)
    # Download first result
    if shortname=="GEDI01_B":
        level = "L1B"
    elif shortname=="GEDI02_A":
        level = "L2A"
    try:
        ## GET CWD of file to save
        CWD = os.path.dirname(os.path.abspath(__file__))
        filename = results[0].getData(CWD)
    except Exception as e:
        print(f"Cant get data for granule {granule_name}")
        print(e)
        return 1

if __name__ == "__main__":
    url_path = sys.argv[1] # first index is python file name, second is arg1, etc
    shortname = sys.argv[2] # e.g. 'GEDI01_B' or 'GEDI02_A'
    # outdir = sys.argv[3]
    download_gedi(url_path,shortname)