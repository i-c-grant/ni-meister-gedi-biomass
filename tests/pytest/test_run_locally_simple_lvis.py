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
        "--max_shots", "1000",
        "--n-workers", "1",
        real_lvis_dir,
        lvis_output_dir
    ], catch_exceptions=False)

    assert result.exit_code == 0, f"Command failed: {result.output}"
    
    output_files = os.listdir(lvis_output_dir)
    gpkg_files = [fname for fname in output_files if fname.endswith(".gpkg")]
    assert len(gpkg_files) > 0, "No output .gpkg file found in output directory"
    
    
def test_run_locally_simple_lvis_skip_existing(lvis_output_dir):
    real_lvis_dir = "tests/data/lvis"
    config_path = os.path.join("config", "config_lvis.yaml")
    
    # Find a LVIS1B file to extract a key for dummy file
    lvis1_files = [f for f in Path(real_lvis_dir).rglob("*.h5") if "LVIS1B" in f.name]
    assert lvis1_files, "No LVIS1B files found in input directory"
    dummy_key = "_".join(lvis1_files[0].stem.split("_")[-3:])
    dummy_filename = "DUMMY_" + dummy_key + ".gpkg"
    dummy_file_path = Path(lvis_output_dir) / dummy_filename
    # Create dummy file to simulate an already processed pair
    dummy_file_path.touch()
    
    from click.testing import CliRunner
    runner = CliRunner()
    result = runner.invoke(run_locally_simple_lvis.main, [
        "--default-hse", "1.0",
        "--default-k-allom", "2.0",
        "--config", config_path,
        "--max_shots", "1000",
        "--n-workers", "1",
        "--skip-existing",
        real_lvis_dir,
        lvis_output_dir
    ], catch_exceptions=False)
    
    assert result.exit_code == 0, f"Command failed: {result.output}"
    
    # Verify that the dummy file was not overwritten and one additional output file was created.
    # Since we assume there are two LVIS pairs, one pair should be skipped (dummy) and one should be processed.
    output_files = os.listdir(lvis_output_dir)
    gpkg_files = [fname for fname in output_files if fname.endswith(".gpkg")]
    # Expecting dummy file + one new output file (total 2 files)
    assert len(gpkg_files) == 2, f"Expected 2 gpkg files, found {len(gpkg_files)}"
    
    produced_files = [fname for fname in gpkg_files if fname != dummy_filename]
    for fname in produced_files:
        produced_key = "_".join(Path(fname).stem.split("_")[-3:])
        assert produced_key != dummy_key, f"Produced file has dummy key {dummy_key}"

def test_run_locally_simple_lvis_no_max_shots(lvis_output_dir):
    real_lvis_dir = "tests/data/lvis"
    config_path = os.path.join("config", "config_lvis.yaml")
    
    runner = CliRunner()
    result = runner.invoke(run_locally_simple_lvis.main, [
        "--default-hse", "1.0",
        "--default-k-allom", "2.0",
        "--config", config_path,
        real_lvis_dir,
        lvis_output_dir
    ], catch_exceptions=False)
    
    assert result.exit_code == 0, f"Command failed: {result.output}"
    
    output_files = os.listdir(lvis_output_dir)
    gpkg_files = [fname for fname in output_files if fname.endswith(".gpkg")]
    assert len(gpkg_files) > 0, "No output .gpkg file found in output directory"
