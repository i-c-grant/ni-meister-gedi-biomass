import os

import click
from maap.maap import MAAP
maap = MAAP(maap_host='api.maap-project.org')
from maap.Result import Granule

from access_gedi import maap_utils

@click.command()
@click.argument('l1b_ur', type=str)
@click.argument('l2a_ur', type=str)
@click.argument('output_dir', type=str)
def main(l1b_ur, l2a_ur, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    l1b_collection: str = maap_utils.get_collection_id("l1b")
    l2a_collection: str = maap_utils.get_collection_id("l2a")

    l1b_granule: Granule = maap_utils.find_unique_granule(l1b_ur,
                                                          l1b_collection)

    l2a_granule: Granule = maap_utils.find_unique_granule(l2a_ur,
                                                          l2a_collection)
    
    click.echo(f"Downloading {l1b_ur} to {output_dir}")
    l1b_granule.getData(destpath=output_dir)

    click.echo(f"Downloading {l2a_ur} to {output_dir}")
    l2a_granule.getData(destpath=output_dir)

if __name__ == '__main__':
    main()
