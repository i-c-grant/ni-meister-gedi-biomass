############################################################
# Top-level functions for processing and writing waveforms #
############################################################
from typing import Dict

from nmbim import (Waveform, WaveformCollection, WaveformProcessor,
                   WaveformWriter)


def build_output_filename(l1b_path: str, l2a_path: str) -> str:
    """Build output filename from input filenames"""

    # For each of l1b and l2a, extract the unique, common part of filename:
    # datetime of aquisition + orbit number + granule number within orbit

    # See <https://lpdaac.usgs.gov/data/get-started-data/collection-overview/
    # missions/gedi-overview/> for filename structure

    l1b_base = l1b_path.split("/")[-1].split(".")[0].split("_")
    l1b_base = "_".join(l1b_base[2:5])

    l2a_base = l2a_path.split("/")[-1].split(".")[0].split("_")
    l2a_base = "_".join(l2a_base[2:5])

    if l1b_base == l2a_base:
        return l1b_base
    else:
        raise ValueError(
            "Input filenames do not match: {l1b_base}, {l2a_base}"
        )


def get_beam_names():
    """Return a list of beam names"""
    return [
        "BEAM0000",
        "BEAM0001",
        "BEAM0010",
        "BEAM0011",
        "BEAM0101",
        "BEAM0110",
        "BEAM1000",
        "BEAM1011",
    ]


def process_waveforms(
    waveforms: WaveformCollection, processor_params: Dict[str, Dict]
):
    """Process waveforms with a pipeline of algorithms defined by
    processor_params"""

    pipeline = []
    for proc_name in processor_params:
        p = WaveformProcessor(
            **processor_params[proc_name], waveforms=waveforms
        )
        pipeline.append(p)

    for p in pipeline:
        p.process()


def write_waveforms(waveforms: WaveformCollection, output_path: str):
    """Write processed waveforms to a GeoPackage file"""

    # Columns with results of interest
    results_cols = {"biwf": "results/biomass_index"}

    # Columns that are always present in Waveform metadata
    context_cols = {
        "time": "metadata/time",
        "rh_100": "processed/veg_ground_sep/veg_top",
        "num_modes": "metadata/modes/num_modes",
        "quality_flag": "metadata/flags/quality",
        "modis_treecover": "metadata/landcover/modis_treecover",
        "modis_nonvegetated": "metadata/landcover/modis_nonvegetated",
        "landsat_treecover": "metadata/landcover/landsat_treecover",
    }

    # Write processed data
    waveform_writer = WaveformWriter(
        path=output_path,
        append=True,
        cols={**context_cols, **results_cols},
        waveforms=waveforms,
    )

    waveform_writer.write()
