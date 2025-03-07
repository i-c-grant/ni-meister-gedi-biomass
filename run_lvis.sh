#!/usr/bin/env bash
LVIS_L1=/home/ian/projects/ni-meister-gedi-biomass-global/tests/data/lvis/LVIS1B_Gabon2016_0222_R1808_043849.h5
LVIS_L2=/home/ian/projects/ni-meister-gedi-biomass-global/tests/data/lvis/LVIS2_Gabon2016_0222_R1808_043849.TXT
CONFIG=/home/ian/projects/ni-meister-gedi-biomass-global/config/config_lvis.yaml
OUTDIR=/home/ian/projects/ni-meister-gedi-biomass-global/tests/output
python process_lvis_granules.py --default-hse 1.0 --default-k-allom 2.0 --config $CONFIG $LVIS_L1 $LVIS_L2 $OUTDIR --max_shots 1000
