#!/usr/bin/env python
"""
process_lvis_granules.py

This script processes LVIS L1 and L2 granules to calculate parameters for each LVIS footprint.
It loads LVIS caches, constructs LVISWaveform objects, builds a WaveformCollection 
from them, and processes the waveforms using the standard processing pipeline.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import click
import yaml

from nmbim import app_utils, filters, algorithms
from nmbim import WaveformCollection, ParameterLoader, ScalarSource, RasterSource
from nmbim import LVISCacheL1, LVISCacheL2, LVISWaveform

def log_and_print(message: str) -> None:
    logging.info(message)
    click.echo(message)

def load_config(config_path: str) -> Dict[str, Any]:
    try:
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logging.error(f"Error loading config file: {e}")
        raise

@click.command()
@click.option("--default-hse", type=float, required=True,
              help="Default height scaling exponent value")
@click.option("--default-k-allom", type=float, required=True,
              help="Default k-allometric value")
@click.option("--config", "-c", type=click.Path(exists=True), required=True,
              help="Path to the filter configuration YAML file")
@click.argument("lvis_l1_path", type=click.Path(exists=True))
@click.argument("lvis_l2_path", type=click.Path(exists=True))
@click.argument("output_dir", type=click.Path())
@click.option("--hse-path", type=click.Path(), help="Optional raster file for HSE values")
@click.option("--k-allom-path", type=click.Path(), help="Optional raster file for K_allom values")
@click.option("--boundary", type=click.Path(exists=True), help="Path to boundary file (e.g., .gpkg)")
@click.option("--date_range", help="Date range in format 'YYYY-MM-DDTHH:MM:SSZ,YYYY-MM-DDTHH:MM:SSZ'")
@click.option("--max_shots", type=int, help="Maximum number of shots to process")
# Add profiling flag
@click.option("--profile", is_flag=True, help="Enable profiling")
def main(lvis_l1_path: str, lvis_l2_path: str, output_dir: str,
         default_hse: float, default_k_allom: float,
         config: str, hse_path: Optional[str], k_allom_path: Optional[str],
         boundary: Optional[str], date_range: Optional[str], max_shots: Optional[int], profile: bool) -> None:
    """
    Process LVIS L1 and L2 granules to calculate parameters for each LVIS footprint.
    Constructs a WaveformCollection from LVISWaveform objects.
    """
    if profile:
        import cProfile
        profiler = cProfile.Profile()
        profiler.enable()

    start_time = datetime.now()
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_name = Path(app_utils.build_output_filename(lvis_l1_path, lvis_l2_path))
    output_path = (output_dir / output_name).with_suffix(".gpkg")
    
    logging.basicConfig(
        filename=str(output_dir / "run.log"),
        format="%(asctime)s - %(message)s",
        level=logging.INFO,
    )
    logging.info(f"Run started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Load configuration
    full_config = load_config(config)
    processor_config = full_config.get("processing_pipeline", {})
    processor_kwargs_dict = processor_config.copy()
    for proc_name, proc_config in processor_kwargs_dict.items():
        alg_name = proc_config["alg_fun"]
        proc_config["alg_fun"] = getattr(algorithms, alg_name)

    filter_config = full_config.get("filters", {})
    if boundary:
        filter_config["spatial"] = {"file_path": boundary}
        log_and_print(f"Spatial filter applied with boundary file: {boundary}")
    if date_range:
        start_date, end_date = date_range.split(",")
        filter_config["temporal"] = {"time_start": start_date, "time_end": end_date}
        log_and_print(f"Temporal filter applied with date range: {date_range}")
    full_config["filters"] = filter_config
    my_filters = filters.generate_filters(filter_config)
    
    # Load LVIS caches
    l1_cache = LVISCacheL1(lvis_l1_path)
    l2_cache = LVISCacheL2(lvis_l2_path)
    
    # Get maximum index from L1 cache and iterate from 0 to max_index
    max_index = l1_cache.get_max_index()
    log_and_print(f"{max_index} shots available in LVIS L1 data.")
    
    indices = list(range(max_index))
    if max_shots is not None:
        indices = indices[:max_shots]
    
    # Construct LVISWaveform objects using the cache index
    waveforms = []
    with click.progressbar(indices, label="Loading waveforms") as bar:
        for idx in bar:
            try:
                wf = LVISWaveform(l1_cache, l2_cache, cache_index=idx)
                waveforms.append(wf)
            except Exception as e:
                logging.error(f"Error processing cache index {idx}: {e}")
                continue
    log_and_print(f"{len(waveforms)} waveforms selected after filtering.")
    
    # Build a WaveformCollection from LVISWaveform objects
    collection = WaveformCollection.from_waveforms(waveforms)
    
    # Parameterize waveforms
    log_and_print("Parameterizing waveforms...")
    param_sources = {
        "default_hse": ScalarSource(default_hse),
        "default_k_allom": ScalarSource(default_k_allom)
    }
    if hse_path:
        param_sources["raster_hse"] = RasterSource(hse_path)
    if k_allom_path:
        param_sources["raster_k_allom"] = RasterSource(k_allom_path)
    param_loader = ParameterLoader(sources=param_sources, waveforms=collection)
    param_loader.parameterize()

    # Process the waveforms
    log_and_print("Processing waveforms...")
    app_utils.process_waveforms(collection, processor_kwargs_dict)
    log_and_print("Waveform processing complete.")

    # Write outputs
    app_utils.write_waveforms(collection, str(output_path))
    log_and_print(f"Output written to {output_path}")

    if profile:
        profiler.disable()
        # Create a unique profile filename based on the lvis_l1_path stem
        profile_path = Path(output_dir) / f"profile_{Path(lvis_l1_path).stem}.prof"
        profiler.dump_stats(str(profile_path))
        logging.info(f"Profiling data saved to {profile_path}")

    finish_time = datetime.now()
    logging.info(f"Run completed at {finish_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"Run duration: {finish_time - start_time}")

if __name__ == "__main__":
    main()
