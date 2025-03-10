#!/usr/bin/env python
"""
System test for process_lvis_granules using real LVIS input files.

This test uses the real LVIS data located at:
  /home/ian/projects/ni-meister-gedi-biomass-global/tests/data/lvis
and verifies that the process_lvis_granules CLI completes successfully and produces an output.
"""

import os
import tempfile
import yaml
from click.testing import CliRunner

import process_lvis_granules

import pytest
from datetime import datetime
from pathlib import Path

@pytest.fixture
def lvis_output_dir():
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    out_dir = Path("tests/output/lvis") / f"output_{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=True)
    return str(out_dir)


def test_process_lvis_granules_system(lvis_output_dir):
    # Use real LVIS input files
    real_lvis_dir = "tests/data/lvis"
    lvis_l1_path = os.path.join(real_lvis_dir, "LVIS1B_Gabon2016_0222_R1808_043849.h5")
    lvis_l2_path = os.path.join(real_lvis_dir, "LVIS2_Gabon2016_0222_R1808_043849.TXT")
    config_path = os.path.join("config/config_lvis.yaml")
            
    runner = CliRunner()
    result = runner.invoke(process_lvis_granules.main, [
        "--default-hse", "1.0",
        "--default-k-allom", "2.0",
        "--config", config_path,
        "--max_shots", 1000,
        lvis_l1_path,
        lvis_l2_path,
        lvis_output_dir
    ], catch_exceptions=False)

    
    assert result.exit_code == 0, f"Command failed: {result.output}"
    
    output_files = os.listdir(lvis_output_dir)
    gpkg_files = [fname for fname in output_files if fname.endswith(".gpkg")]
    assert len(gpkg_files) > 0, "No output .gpkg file found in output directory"
