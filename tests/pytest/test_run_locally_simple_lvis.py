#!/usr/bin/env python
"""
System test for run_locally_simple_lvis using real LVIS input files.

This test uses the real LVIS data located at:
  tests/data/lvis
and verifies that the run_locally_simple_lvis CLI completes successfully and produces an output.
"""

import os
from pathlib import Path
from datetime import datetime
from click.testing import CliRunner
import run_locally_simple_lvis
import pytest

@pytest.fixture
def lvis_output_dir():
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    out_dir = Path("tests/output/lvis") / f"output_{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=True)
    return str(out_dir)

def test_run_locally_simple_lvis_system(lvis_output_dir):
    real_lvis_dir = "tests/data/lvis"
    config_path = os.path.join("config", "config_lvis.yaml")
    
    runner = CliRunner()
    result = runner.invoke(run_locally_simple_lvis.main, [
        "--default-hse", "1.0",
        "--default-k-allom", "2.0",
        "--config", config_path,
        "--max_shots", 1000,
        real_lvis_dir,
        lvis_output_dir
    ], catch_exceptions=False)

    assert result.exit_code == 0, f"Command failed: {result.output}"
    
    output_files = os.listdir(lvis_output_dir)
    gpkg_files = [fname for fname in output_files if fname.endswith(".gpkg")]
    assert len(gpkg_files) > 0, "No output .gpkg file found in output directory"
