import os

import click
from maap.maap import MAAP
maap = MAAP(maap_host='api.maap-project.org')
from maap.Result import Granule

from access_gedi import download_gedi

@click.command()
@click.argument('l1b_ur', type=str)
@click.argument('l2a_ur', type=str)
@click.argument('output_dir', type=str)
def main(l1b_ur, l2a_ur, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    l1b_s3_url = download_gedi.gedi_filename_to_s3_url(l1b_ur)
    l2a_s3_url = download_gedi.gedi_filename_to_s3_url(l2a_ur)

    download_gedi.get_gedi_data(l1b_s3_url, output_dir)
    download_gedi.get_gedi_data(l2a_s3_url, output_dir)
       
if __name__ == '__main__':
    main()
