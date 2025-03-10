"""
Process matching LVIS L1 and L2 file pairs in parallel.
Assumes files in each directory are sorted and correspond 1:1.
"""

import os
from pathlib import Path
from multiprocessing import Pool
import subprocess
from typing import Tuple
import logging
import click

def process_pair(args: Tuple[str, str, float, float, str, str, str, str, int, str]):
    """Process a single pair using process_lvis_granules.py"""
    lvis_l1, lvis_l2, default_hse, default_k_allom, output_dir, hse_path, k_allom_path, config, max_shots, boundary = args

    cmd = [
        "python", "process_lvis_granules.py",
        "--default-hse", str(default_hse),
        "--default-k-allom", str(default_k_allom),
        "--config", config,
    ]
    if max_shots is not None:
        cmd.extend(["--max_shots", str(max_shots)])
    cmd.extend([lvis_l1, lvis_l2, output_dir])
    
    # Add optional arguments
    if hse_path:
        cmd.extend(["--hse-path", hse_path])
    if k_allom_path:
        cmd.extend(["--k-allom-path", k_allom_path])
    if boundary:
        cmd.extend(["--boundary", boundary])
    
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error processing pair {lvis_l1}, {lvis_l2}: {e}")
        raise

@click.command()
@click.option("--default-hse", type=float, required=True,
              help="Default height scaling exponent value")
@click.option("--default-k-allom", type=float, required=True,
              help="Default k-allometric value")
@click.option("--config", "-c", type=click.Path(exists=True), required=True,
              help="Path to configuration YAML file")
@click.argument("lvis_dir", type=click.Path(exists=True))
@click.argument("output_dir", type=click.Path(exists=True))
@click.option("--hse-path", type=click.Path(exists=True),
              help="Optional raster file for HSE values")
@click.option("--k-allom-path", type=click.Path(exists=True),
              help="Optional raster file for K_allom values")
@click.option("--max_shots", type=int, help="Maximum number of shots to process")
@click.option("--boundary", type=click.Path(exists=True),
              help="Path to boundary file (e.g., .gpkg)")
@click.option("--n-workers", "-n", default=4,
              help="Number of parallel workers to use")
@click.option("--skip-existing", is_flag=True,
              help="Skip file pairs if an output with a matching key already exists")
def main(lvis_dir: str,
         output_dir: str,
         default_hse: float,
         default_k_allom: float,
         config: str,
         hse_path: str,
         k_allom_path: str,
         max_shots: int,
         boundary: str,
         n_workers: int,
         skip_existing: bool):
    """Process matching LVIS file pairs in parallel."""
    
    # Ensure output directory exists and set up file logging to run.log
    output_dir_path = Path(output_dir)
    output_dir_path.mkdir(parents=True, exist_ok=True)
    log_file = output_dir_path / "run.log"
    file_handler = logging.FileHandler(str(log_file))
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logging.getLogger().addHandler(file_handler)
    logging.getLogger().setLevel(logging.INFO)
    
    # Recursively search for LVIS1B (.h5) and LVIS2 (.txt) files in the directory
    lvis_l1_files = sorted([f for f in Path(lvis_dir).rglob("*.h5") if "LVIS1B" in f.name])
    lvis_l2_files = sorted([f for f in Path(lvis_dir).rglob("*.[tT][xX][tT]") if "LVIS2" in f.name])

    # Verify equal lengths
    if len(lvis_l1_files) != len(lvis_l2_files):
        raise click.ClickException("Directories must contain equal numbers of LVIS1B and LVIS2 files")
    
    logging.info(f"Found {len(lvis_l1_files)} file pairs")
    
    # Prepare arguments for each pair
    args_list = []
    if skip_existing:
        existing_keys = set()
        for file in Path(output_dir).glob("*.gpkg"):
            key = "_".join(file.stem.split("_")[-3:])
            existing_keys.add(key)
        logging.info(f"Found existing output keys: {existing_keys}")
    for l1, l2 in zip(lvis_l1_files, lvis_l2_files):
        if skip_existing:
            key = "_".join(l1.stem.split("_")[-3:])
            if key in existing_keys:
                logging.info(f"Skipping pair: {l1.name}, {l2.name} (file corresponding to {key} already in output directory)")
                continue
        args_list.append((str(l1), str(l2), default_hse, default_k_allom, output_dir, hse_path, k_allom_path, config, max_shots, boundary))
    
    # Process pairs in parallel
    with Pool(n_workers) as pool:
        pool.map(process_pair, args_list)
    
    logging.info("All pairs processed")

if __name__ == "__main__":
    main()
