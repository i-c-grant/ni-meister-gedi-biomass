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

    # Add .h5 extension if not present
    if not l1b_ur.endswith('.h5'):
        l1b_ur += '.h5'

    if not l2a_ur.endswith('.h5'):
        l2a_ur += '.h5'

    # Download the GEDI data
    download_gedi.get_gedi_data(filename=l1b_ur,
                                target_dir=output_dir)
    download_gedi.get_gedi_data(filename=l2a_ur,
                                target_dir=output_dir)
       
if __name__ == '__main__':
    main()
