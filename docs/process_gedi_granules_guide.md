# Guide for process_gedi_granules.py

This script processes a single set of GEDI L1B, L2A, and L4A granules locally to calculate the Ni-Meister Biomass Index (NMBI) for each waveform footprint. It supports both serial and parallel execution modes.

## 1. Installation

Create and activate the conda environment using the provided `environment.yml`:

```bash
conda env create -f environment.yml
conda activate nmbim-env
```

## 2. Basic CLI Usage

```bash
python process_gedi_granules.py \
  <l1b_path> <l2a_path> <l4a_path> \
  <hse_path> <k_allom_path> <output_dir> \
  --config config.yaml \
  --parallel \
  --n_workers 4 \
  --boundary boundary.gpkg \
  --date_range "YYYY-MM-DD,YYYY-MM-DD"
```

Arguments:
- `<l1b_path>`, `<l2a_path>`, `<l4a_path>`: GEDI HDF5 files (Level 1B, 2A, 4A).
- `<hse_path>`, `<k_allom_path>`: GeoTIFF rasters named `hse.tif` and `k_allom.tif`.
- `<output_dir>`: Directory to write the output GeoPackage (`.gpkg`).

Options:
- `--config, -c`: Path to filter configuration YAML.
- `--parallel, -p`: Enable multiprocessing mode.
- `--n_workers, -n`: Number of parallel workers (default: 4).
- `--boundary`: Path to spatial boundary file (GeoPackage or Shapefile).
- `--date_range`: Temporal filter range (`start,end`).

## 3. Example CLI Invocation

```bash
python process_gedi_granules.py \
  GEDI01_B_20181001_034643_003_01.h5 \
  GEDI02_A_20181001_034643_003_02.h5 \
  GEDI04_A_20181001_034643_003_04.h5 \
  hse.tif k_allom.tif ./output \
  --config filters.yaml \
  --parallel --n_workers 4 \
  --boundary boundary.gpkg \
  --date_range "2018-10-01,2018-10-02"
```

## 4. Programmatic Batch Processing via process_gedi_granules

You can invoke the `process_gedi_granules.py` CLI script from Python to process each granule triplet in serial or parallel with `concurrent.futures`.

```python
import os
import concurrent.futures
import subprocess

# Define input granule triplets
triplets = [
    {"l1b": "l1b1.h5", "l2a": "l2a1.h5", "l4a": "l4a1.h5"},
    {"l1b": "l1b2.h5", "l2a": "l2a2.h5", "l4a": "l4a2.h5"},
]

hse = "hse.tif"
k_allom = "k_allom.tif"
output_dir = "output"
os.makedirs(output_dir, exist_ok=True)

def run_triplet(t):
    cmd = [
        "python", "process_gedi_granules.py",
        t["l1b"], t["l2a"], t["l4a"],
        hse, k_allom, output_dir,
        "--config", "config.yaml"
    ]
    subprocess.run(cmd, check=True)

# Serial execution
for t in triplets:
    run_triplet(t)

# Or parallelize with ProcessPoolExecutor
with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
    executor.map(run_triplet, triplets)
```

This approach lets you easily script batch processing of multiple granule sets using the same CLI entrypoint.
