import os
import pytest
from pathlib import Path

from nmbim.LVISCacheL1 import LVISCacheL1
from nmbim.LVISCacheL2 import LVISCacheL2

class TestLVISCacheIntegration:
    @pytest.fixture
    def lvis_data_path(self):
        # Path to the LVIS test data directory.
        return Path("tests/data/lvis")

    @pytest.fixture
    def l1b_file_path(self, lvis_data_path):
        # Path to the LVIS L1B HDF5 file.
        return lvis_data_path / "LVIS1B_Gabon2016_0222_R1808_043849.h5"

    @pytest.fixture
    def l2_file_path(self, lvis_data_path):
        # Path to the LVIS L2 TXT file.
        return lvis_data_path / "LVIS2_Gabon2016_0222_R1808_043849.TXT"

    def test_lvis_cache_l1_integration(self, l1b_file_path):
        # Skip test if file does not exist.
        if not l1b_file_path.exists():
            pytest.skip(f"LVIS L1B file not found: {l1b_file_path}")
        # Initialize LVISCacheL1 with the file path.
        cache_l1 = LVISCacheL1(str(l1b_file_path))
        # Load the data.
        data = LVISCacheL1.load(str(l1b_file_path))
        # Check that essential keys exist in the loaded data.
        assert "SHOTNUMBER" in data
        assert "LFID" in data
        # Use extract_dataset to get the SHOTNUMBER array.
        shot_numbers = cache_l1.extract_dataset("SHOTNUMBER")
        assert shot_numbers is not None
        # Choose the first shot number.
        first_shot = shot_numbers[0]
        # Find the corresponding shot index.
        index = cache_l1.where_shot(first_shot)
        # Use extract_value to get the LFID for that index.
        lfid = cache_l1.extract_value("LFID", index)
        assert lfid is not None

    def test_lvis_cache_l2_integration(self, l2_file_path):
        # Skip test if file does not exist.
        if not l2_file_path.exists():
            pytest.skip(f"LVIS L2 TXT file not found: {l2_file_path}")
        # Initialize LVISCacheL2 with the file path.
        cache_l2 = LVISCacheL2(str(l2_file_path))
        # Load the DataFrame.
        df = LVISCacheL2.load(str(l2_file_path))
        # Expected columns in the LVIS L2 file.
        expected_columns = ['LFID', 'SHOTNUMBER', 'ZG', 'ZT', 'ZH', 
                            'RH50', 'RH75', 'RH90', 'RH95', 'RH100']
        for col in expected_columns:
            assert col in df.columns
        # Get the first row values for LFID and SHOTNUMBER.
        first_row = df.iloc[0]
        lfid = first_row['LFID']
        shot_number = first_row['SHOTNUMBER']
        # Use extract_value to retrieve a known field value.
        zg = cache_l2.extract_value(lfid, shot_number, "ZG")
        assert zg == first_row['ZG']
