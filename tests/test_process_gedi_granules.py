import pytest
import subprocess
import os
from pathlib import Path

@pytest.fixture
def test_paths():
    current_dir = Path(__file__).parent
    project_dir = current_dir.parent
    return {
        "current_dir": current_dir,
        "l1b_path": current_dir / "input" / "GEDI01_B_2021151223415_O13976_02_T00676_02_005_02_V002.h5",
        "l2a_path": current_dir / "input" / "GEDI02_A_2021151223415_O13976_02_T00676_02_003_02_V002.h5",
        "output_dir": current_dir / "output",
        "config_path": project_dir / "nmbim" / "config.yaml",
        "boundary_path": current_dir / "input" / "test_boundary_link.gpkg"
    }

def test_process_gedi_granules(test_paths, capsys):
    # Get the absolute path to the process_gedi_granules.py script
    script_path = os.path.abspath(os.path.join(test_paths["current_dir"].parent, "process_gedi_granules.py"))

    # Construct the command
    command = [
        "conda", "run", "-n", "nmbim-env", "python",
        script_path,
        str(test_paths["l1b_path"]),
        str(test_paths["l2a_path"]),
        str(test_paths["output_dir"]),
        "--config", str(test_paths["config_path"]),
        "--boundary", str(test_paths["boundary_path"]),
        "--date_range", "2019-04-18T00:00:00Z,2019-04-19T00:00:00Z"
    ]

    # Run the command
    result = subprocess.run(command, capture_output=True, text=True)

    # Capture the output
    captured = capsys.readouterr()

    # Print the stderr regardless of the test result
    print(f"Command stderr output:\n{result.stderr}")

    # Check if the command was successful
    assert result.returncode == 0, f"Command failed with error: {result.stderr}"

    # Check if output files were created
    expected_output = test_paths["output_dir"] / "GEDI01_B_2021151223415_O13976_02_T00676_02_005_02_V002.gpkg"
    assert expected_output.exists(), f"Expected output file {expected_output} was not created"

    # Add more assertions here to check the content of the output file if needed
