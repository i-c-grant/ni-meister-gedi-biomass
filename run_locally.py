"""
Process matching GEDI L1B, L2A, and L4A file triplets in parallel.

This script finds matching triplets of GEDI files based on their shot numbers
(the number immediately before .h5 extension) and processes them using
process_gedi_granules.py in parallel.
"""

import os
import re
from pathlib import Path
from multiprocessing import Pool
import subprocess
from typing import List, Tuple, Dict
import click

def extract_shot_number(filename: str) -> str:
    """Extract the shot number from a GEDI filename."""
    match = re.search(r"(\d+)\.h5$", filename)
    if not match:
        raise ValueError(f"Could not extract shot number from {filename}")
    return match.group(1)

def find_matching_triplets(l1b_dir: str, l2a_dir: str, l4a_dir: str) -> List[Tuple[str, str, str]]:
    """Find matching triplets of GEDI files based on shot numbers."""
    # Get all files in each directory
    l1b_files = {extract_shot_number(f.name): f for f in Path(l1b_dir).glob("*.h5")}
    l2a_files = {extract_shot_number(f.name): f for f in Path(l2a_dir).glob("*.h5")}
    l4a_files = {extract_shot_number(f.name): f for f in Path(l4a_dir).glob("*.h5")}

    # Find common shot numbers
    common_shots = set(l1b_files.keys()) & set(l2a_files.keys()) & set(l4a_files.keys())

    # Create list of matching triplets
    triplets = []
    for shot in common_shots:
        triplets.append((
            str(l1b_files[shot]),
            str(l2a_files[shot]),
            str(l4a_files[shot])
        ))
    
    return triplets

def process_triplet(args: Tuple[str, str, str, float, float, str, str, str, str, str]):
    """Process a single triplet using process_gedi_granules.py"""
    l1b, l2a, l4a, default_hse, default_k_allom, output_dir, hse_path, k_allom_path, config, boundary = args
    
    cmd = [
        "python", "process_gedi_granules.py",
        "--default-hse", str(default_hse),
        "--default-k-allom", str(default_k_allom),
        "--config", config,
        l1b, l2a, l4a, output_dir
    ]
    
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
        print(f"Error processing triplet {l1b}, {l2a}, {l4a}: {e}")
        raise

@click.command()
@click.option("--default-hse", type=float, required=True,
              help="Default height scaling exponent value")
@click.option("--default-k-allom", type=float, required=True,
              help="Default k-allometric value")
@click.option("--config", "-c", type=click.Path(exists=True), required=True,
              help="Path to configuration YAML file")
@click.argument("l1b_dir", type=click.Path(exists=True))
@click.argument("l2a_dir", type=click.Path(exists=True))
@click.argument("l4a_dir", type=click.Path(exists=True))
@click.argument("output_dir", type=click.Path(exists=True))
@click.option("--hse-path", type=click.Path(exists=True),
              help="Optional raster file for HSE values")
@click.option("--k-allom-path", type=click.Path(exists=True),
              help="Optional raster file for K_allom values")
@click.option("--boundary", type=click.Path(exists=True),
              help="Path to boundary file (e.g., .gpkg)")
@click.option("--n-workers", "-n", default=4,
              help="Number of parallel workers to use")
def main(l1b_dir: str,
         l2a_dir: str,
         l4a_dir: str,
         default_hse: float,
         default_k_allom: float,
         output_dir: str,
         hse_path: str,
         k_allom_path: str,
         config: str,
         boundary: str,
         n_workers: int):
    """Process matching GEDI file triplets in parallel."""
    
    # Find all matching triplets
    triplets = find_matching_triplets(l1b_dir, l2a_dir, l4a_dir)
    print(f"Found {len(triplets)} matching file triplets")
    
    # Prepare arguments for each triplet
    args_list = [
        (*triplet, default_hse, default_k_allom, output_dir, hse_path, k_allom_path, config, boundary)
        for triplet in triplets
    ]
    
    # Process triplets in parallel
    with Pool(n_workers) as pool:
        pool.map(process_triplet, args_list)
    
    print("All triplets processed")

if __name__ == "__main__":
    main()
