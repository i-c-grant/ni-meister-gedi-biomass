#!/bin/bash
python -m scripts.summarize_biwf_in_emap_hexes \
  --gpkg-pattern "results/model_runs/fifth_run/**/*.gpkg" \
  --hex-grid "data/fia/menlove_healey/2020_biohex_merged.gpkg" \
  --output "/tmp/biwf_test_output.gpkg" 

