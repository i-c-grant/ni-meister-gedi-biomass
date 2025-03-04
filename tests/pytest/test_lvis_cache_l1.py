import os
import pytest
import numpy as np
import h5py
from pathlib import Path
from tempfile import NamedTemporaryFile

from nmbim.LVISCacheL1 import LVISCacheL1


class TestLVISCacheL1:
    """Unit tests for LVISCacheL1."""

    @pytest.fixture
    def sample_h5_file(self):
        """Create a temporary HDF5 file with sample LVIS data."""
        with NamedTemporaryFile(suffix='.h5', delete=False) as f:
            filename = f.name

        # Create a sample HDF5 file with LVIS-like structure
        with h5py.File(filename, 'w') as h5f:
            # Create datasets
            h5f.create_dataset('SHOTNUMBER', data=np.array([1001, 1002, 1003, 1004, 1005]))
            h5f.create_dataset('LFID', data=np.array([101, 102, 103, 104, 105]))
            h5f.create_dataset('LON0', data=np.array([-74.1, -74.2, -74.3, -74.4, -74.5]))
            h5f.create_dataset('LAT0', data=np.array([40.1, 40.2, 40.3, 40.4, 40.5]))
            h5f.create_dataset('Z0', data=np.array([100.1, 100.2, 100.3, 100.4, 100.5]))
            
            # Create a group with nested datasets
            group = h5f.create_group('SUBGROUP')
            group.create_dataset('NESTED_DATA', data=np.array([1.1, 2.2, 3.3, 4.4, 5.5]))

        yield filename
        
        # Clean up the temporary file
        os.unlink(filename)

    def test_load(self, sample_h5_file):
        """Test loading an HDF5 file into the cache."""
        # Clear the cache to ensure a clean test
        LVISCacheL1._cache = {}
        
        # Load the file
        data = LVISCacheL1.load(sample_h5_file)
        
        # Check that the data was loaded correctly
        assert 'SHOTNUMBER' in data
        assert 'LFID' in data
        assert 'LON0' in data
        assert 'LAT0' in data
        assert 'Z0' in data
        assert 'SUBGROUP' in data
        assert 'NESTED_DATA' in data['SUBGROUP']
        
        # Check that the data values are correct
        np.testing.assert_array_equal(data['SHOTNUMBER'], np.array([1001, 1002, 1003, 1004, 1005]))
        np.testing.assert_array_equal(data['LFID'], np.array([101, 102, 103, 104, 105]))
        
        # Check that the nested data was loaded correctly
        np.testing.assert_array_equal(data['SUBGROUP']['NESTED_DATA'], 
                                     np.array([1.1, 2.2, 3.3, 4.4, 5.5]))
        
        # Check that the file is now in the cache
        assert sample_h5_file in LVISCacheL1._cache
        
        # Load the file again and check that it's the same object (cached)
        data2 = LVISCacheL1.load(sample_h5_file)
        assert data is data2  # Should be the same object reference

    def test_init_and_extract_value(self, sample_h5_file):
        """Test initializing the cache and extracting values."""
        # Create a cache object
        cache = LVISCacheL1(sample_h5_file)
        
        # Test extracting values
        assert cache.extract_value('SHOTNUMBER', 0) == 1001
        assert cache.extract_value('SHOTNUMBER', 2) == 1003
        assert cache.extract_value('LON0', 1) == -74.2
        assert cache.extract_value('LAT0', 3) == 40.4
        
        # Test extracting from nested group
        assert cache.extract_value('SUBGROUP/NESTED_DATA', 2) == 3.3

    def test_extract_dataset(self, sample_h5_file):
        """Test extracting entire datasets."""
        cache = LVISCacheL1(sample_h5_file)
        
        # Test extracting full datasets
        np.testing.assert_array_equal(
            cache.extract_dataset('SHOTNUMBER'),
            np.array([1001, 1002, 1003, 1004, 1005])
        )
        
        np.testing.assert_array_equal(
            cache.extract_dataset('LON0'),
            np.array([-74.1, -74.2, -74.3, -74.4, -74.5])
        )
        
        # Test extracting nested dataset
        np.testing.assert_array_equal(
            cache.extract_dataset('SUBGROUP/NESTED_DATA'),
            np.array([1.1, 2.2, 3.3, 4.4, 5.5])
        )

    def test_where_shot(self, sample_h5_file):
        """Test finding shot indices."""
        cache = LVISCacheL1(sample_h5_file)
        
        # Test finding existing shots
        assert cache.where_shot(1001) == 0
        assert cache.where_shot(1003) == 2
        assert cache.where_shot(1005) == 4
        
        # Test finding non-existent shot
        with pytest.raises(ValueError):
            cache.where_shot(9999)

    def test_cache_reuse(self, sample_h5_file):
        """Test that multiple cache objects reuse the same cached data."""
        # Create two cache objects for the same file
        cache1 = LVISCacheL1(sample_h5_file)
        cache2 = LVISCacheL1(sample_h5_file)
        
        # Verify they're using the same cached data
        assert LVISCacheL1.load(sample_h5_file) is LVISCacheL1.load(sample_h5_file)
        
        # Modify the cached data through one object
        data = LVISCacheL1.load(sample_h5_file)
        original = data['SHOTNUMBER'].copy()
        data['SHOTNUMBER'][0] = 9999
        
        # Verify the change is visible through both objects
        assert cache1.extract_value('SHOTNUMBER', 0) == 9999
        assert cache2.extract_value('SHOTNUMBER', 0) == 9999
        
        # Restore the original data
        data['SHOTNUMBER'] = original
