############################################################
# Top-level functions for processing and writing waveforms #
############################################################

from typing import Dict, Optional

from nmbim import (
    Waveform,
    WaveformCollection,
    WaveformProcessor,
    WaveformWriter,
)

def process_waveforms(
    waveforms: WaveformCollection, processor_params: Dict[str, Dict]
):
    """Process waveforms with a pipeline of algorithms defined by processor_params"""

    pipeline = []
    for proc_name in processor_params:
        p = WaveformProcessor(
            **processor_params[proc_name], waveforms=waveforms
        )
        pipeline.append(p)

    for p in pipeline:
        p.process()


def write_waveforms(
    waveforms: WaveformCollection, output_dir: str, output_name: str
):
    """Write processed waveforms to a GeoPackage file"""

    # Columns with results of interest
    results_cols = {"biwf": "results/biomass_index"}

    # Columns that are always present in Waveform metadata
    context_cols = {
        "rh_100": "processed/veg_ground_sep/veg_top",
        "num_modes": "metadata/modes/num_modes",
        "quality_flag": "metadata/flags/quality",
        "modis_treecover": "metadata/landcover/modis_treecover",
        "modis_nonvegetated": "metadata/landcover/modis_nonvegetated",
        "landsat_treecover": "metadata/landcover/landsat_treecover",
    }

    # Write processed data
    waveform_writer = WaveformWriter(
        path=f"{output_dir}/{output_name}.gpkg",
        append=True,
        cols={**context_cols, **results_cols},
        waveforms=waveforms,
    )

    waveform_writer.write()


def define_filters():
    """Define filters that determine which waveforms are processed"""

    # Quality control filters
    def flag_filter(wf: Waveform) -> bool:
        """Filter waveforms based on metadata or data quality."""
        if wf.get_data("metadata/flags/quality") == 1:
            return True
        else:
            return False

    def modes_filter(wf: Waveform) -> bool:
        """Keep only waveforms with more than one mode."""
        if wf.get_data("metadata/modes/num_modes") > 1:
            return True
        else:
            return False

    def landcover_filter(wf: Waveform) -> bool:
        """Keep only waveforms with more than 50% tree cover."""
        if wf.get_data("metadata/landcover/modis_treecover") > 10:
            return True
        else:
            return False

    filters = [flag_filter, modes_filter, landcover_filter]

    return filters
