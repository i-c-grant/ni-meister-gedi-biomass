import os
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from tempfile import NamedTemporaryFile

from nmbim.LVISCacheL2 import LVISCacheL2


class TestLVISCacheL2:
    """Unit tests for LVISCacheL2."""

    @pytest.fixture
    def sample_txt_file(self):
        """Create a temporary TXT file with sample LVIS L2 data."""
        with NamedTemporaryFile(suffix='.TXT', delete=False) as f:
            # Write sample LVIS L2 data
            f.write(b"# LVIS L2 Sample Data\n")
            f.write(b"# Created for testing\n")
            f.write(b"LFID SHOTNUMBER ZG ZT ZH RH50 RH75 RH90 RH95 RH100\n")
            f.write(b"101 1001 100.1 150.1 145.1 125.1 135.1 140.1 142.1 145.1\n")
            f.write(b"102 1002 100.2 150.2 145.2 125.2 135.2 140.2 142.2 145.2\n")
            f.write(b"103 1003 100.3 150.3 145.3 125.3 135.3 140.3 142.3 145.3\n")
            f.write(b"104 1004 100.4 150.4 145.4 125.4 135.4 140.4 142.4 145.4\n")
            f.write(b"105 1005 100.5 150.5 145.5 125.5 135.5 140.5 142.5 145.5\n")
            filename = f.name

        yield filename
        
        # Clean up the temporary file
        os.unlink(filename)

    def test_load(self, sample_txt_file):
        """Test loading a TXT file into the cache."""
        # Clear the cache to ensure a clean test
        LVISCacheL2._cache = {}
        
        # Load the file
        df = LVISCacheL2.load(sample_txt_file)
        
        # Check that the DataFrame was created correctly
        assert not df.empty
        assert len(df) == 5
        assert list(df.columns) == ['LFID', 'SHOTNUMBER', 'ZG', 'ZT', 'ZH', 
                                   'RH50', 'RH75', 'RH90', 'RH95', 'RH100']
        
        # Check that numeric conversion worked
        assert df['LFID'].dtype == np.int64 or df['LFID'].dtype == np.float64
        assert df['SHOTNUMBER'].dtype == np.int64 or df['SHOTNUMBER'].dtype == np.float64
        assert df['ZG'].dtype == np.float64
        
        # Check some values
        assert df.loc[0, 'LFID'] == 101
        assert df.loc[2, 'SHOTNUMBER'] == 1003
        assert df.loc[4, 'ZG'] == 100.5
        assert df.loc[1, 'RH75'] == 135.2
        
        # Check that the file is now in the cache
        assert sample_txt_file in LVISCacheL2._cache
        
        # Load the file again and check that it's the same object (cached)
        df2 = LVISCacheL2.load(sample_txt_file)
        assert df is df2  # Should be the same object reference

    def test_init_and_find_column(self, sample_txt_file):
        """Test initializing the cache and finding columns."""
        # Create a cache object
        cache = LVISCacheL2(sample_txt_file)
        
        # Test finding columns (case-insensitive)
        assert cache.find_column('LFID') == 'LFID'
        assert cache.find_column('lfid') == 'LFID'
        assert cache.find_column('LfId') == 'LFID'
        assert cache.find_column('RH50') == 'RH50'
        assert cache.find_column('rh50') == 'RH50'
        
        # Test finding non-existent column
        assert cache.find_column('NONEXISTENT') is None

    def test_extract_value(self, sample_txt_file):
        """Test extracting values for specific shots."""
        cache = LVISCacheL2(sample_txt_file)
        
        # Test extracting values for existing shots
        assert cache.extract_value(101, 1001, 'ZG') == 100.1
        assert cache.extract_value(103, 1003, 'ZT') == 150.3
        assert cache.extract_value(105, 1005, 'RH90') == 140.5
        
        # Test case-insensitive field names
        assert cache.extract_value(102, 1002, 'zg') == 100.2
        assert cache.extract_value(104, 1004, 'rh75') == 135.4
        
        # Test non-existent shot
        assert cache.extract_value(999, 9999, 'ZG') is None
        
        # Test non-existent field
        assert cache.extract_value(101, 1001, 'NONEXISTENT') is None

    def test_cache_reuse(self, sample_txt_file):
        """Test that multiple cache objects reuse the same cached data."""
        # Create two cache objects for the same file
        cache1 = LVISCacheL2(sample_txt_file)
        cache2 = LVISCacheL2(sample_txt_file)
        
        # Verify they're using the same cached data
        assert LVISCacheL2.load(sample_txt_file) is LVISCacheL2.load(sample_txt_file)
        
        # Modify the cached data through the DataFrame
        df = LVISCacheL2.load(sample_txt_file)
        original_value = df.loc[0, 'ZG']
        df.loc[0, 'ZG'] = 999.9
        
        # Verify the change is visible through both objects
        assert cache1.extract_value(101, 1001, 'ZG') == 999.9
        assert cache2.extract_value(101, 1001, 'ZG') == 999.9
        
        # Restore the original value
        df.loc[0, 'ZG'] = original_value

    def test_empty_file_handling(self):
        """Test handling of empty or invalid files."""
        with NamedTemporaryFile(suffix='.TXT', delete=False) as f:
            # Create an empty file
            filename = f.name
        
        try:
            # Clear the cache
            LVISCacheL2._cache = {}
            
            # Load the empty file
            df = LVISCacheL2.load(filename)
            
            # Check that an empty DataFrame was returned
            assert df.empty
            
            # Create a cache object and test extraction
            cache = LVISCacheL2(filename)
            assert cache.extract_value(101, 1001, 'ZG') is None
            assert cache.find_column('LFID') is None
            
        finally:
            # Clean up
            os.unlink(filename)

    def test_malformed_file_handling(self):
        """Test handling of malformed files."""
        with NamedTemporaryFile(suffix='.TXT', delete=False) as f:
            # Create a file with inconsistent columns
            f.write(b"# Malformed LVIS L2 Data\n")
            f.write(b"LFID SHOTNUMBER ZG\n")
            f.write(b"101 1001 100.1\n")
            f.write(b"102 1002 100.2 EXTRA_COLUMN\n")  # This line has an extra column
            f.write(b"103 1003\n")  # This line is missing a column
            filename = f.name
        
        try:
            # Clear the cache
            LVISCacheL2._cache = {}
            
            # Load the malformed file
            df = LVISCacheL2.load(filename)
            
            # Check that only valid rows were loaded
            assert len(df) == 1
            assert df.loc[0, 'LFID'] == 101
            assert df.loc[0, 'ZG'] == 100.1
            
        finally:
            # Clean up
            os.unlink(filename)
