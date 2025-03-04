import os
import tempfile
import numpy as np
import h5py
import pytest
from pathlib import Path

from nmbim.LVISWaveform import LVISWaveform


@pytest.fixture
def mock_l1b_file():
    """Create a mock L1B HDF5 file for testing."""
    with tempfile.NamedTemporaryFile(suffix='.h5') as temp_file:
        with h5py.File(temp_file.name, 'w') as f:
            # Create datasets - Note: LVIS L1B keys are uppercase
            f.create_dataset('LFID', data=np.array([1657449111, 1657449111]))
            f.create_dataset('SHOTNUMBER', data=np.array([7332097, 7332098]))
            f.create_dataset('AZIMUTH', data=np.array([329.47, 322.09]))
            f.create_dataset('INCIDENTANGLE', data=np.array([0.601, 0.593]))
            f.create_dataset('RANGE', data=np.array([7224.40, 7219.00]))
            f.create_dataset('TIME', data=np.array([60655.90200, 60655.90300]))
            f.create_dataset('LON0', data=np.array([11.744102, 11.744035]))
            f.create_dataset('LAT0', data=np.array([-0.325047, -0.325103]))
            f.create_dataset('Z0', data=np.array([261.74, 264.69]))
            f.create_dataset('LON1023', data=np.array([11.744100, 11.744032]))
            f.create_dataset('LAT1023', data=np.array([-0.325044, -0.325099]))
            f.create_dataset('Z1023', data=np.array([217.60, 218.83]))
            f.create_dataset('SIGMEAN', data=np.array([10.5, 11.2]))
            
            # Create waveform datasets
            tx_wave = np.zeros((2, 128), dtype=np.float32)
            tx_wave[0, 64] = 100  # Simple pulse in the middle
            tx_wave[1, 64] = 100
            f.create_dataset('TXWAVE', data=tx_wave)
            
            rx_wave = np.zeros((2, 1024), dtype=np.float32)
            rx_wave[0, 512] = 50  # Simple return pulse
            rx_wave[1, 512] = 60
            f.create_dataset('RXWAVE', data=rx_wave)
        
        yield temp_file.name


@pytest.fixture
def mock_l2_txt_file():
    """Create a mock L2 TXT file for testing."""
    with tempfile.NamedTemporaryFile(suffix='.txt', mode='w+') as temp_file:
        temp_file.write("""# This is a comment line
# Another comment line
# 
# LVIS L2 data
LFID SHOTNUMBER TIME GLON GLAT ZG HLON HLAT ZH TLON TLAT ZT RH10 RH15 RH20 RH25 RH30 RH35 RH40 RH45 RH50 RH55 RH60 RH65 RH70 RH75 RH80 RH85 RH90 RH95 RH96 RH97 RH98 RH99 RH100 AZIMUTH INCIDENTANGLE RANGE COMPLEXITY CHANNEL_L1B CHANNEL_ZG CHANNEL_RH
1657449111 7332097 60655.90200 11.744100 -0.325044 217.60 -999 -999 -999 11.744102 -0.325047 261.74 11.24 15.06 19.86 22.18 23.53 24.73 25.78 26.75 27.58 28.33 29.23 30.20 31.33 33.05 34.55 35.82 37.17 38.60 38.89 39.27 39.79 40.69 44.14 329.47 0.601 7224.40 -999 1 1 1
1657449111 7332098 60655.90300 11.744032 -0.325099 218.83 -999 -999 -999 11.744035 -0.325103 264.69 23.45 27.20 30.20 32.45 33.80 34.62 35.29 35.89 36.34 36.79 37.32 37.77 38.22 38.82 39.34 40.02 40.77 41.82 42.11 42.41 42.79 43.46 45.86 322.09 0.593 7219.00 -999 1 1 1
""")
        temp_file.flush()
        yield temp_file.name


class TestLVISWaveform:
    """Test the LVISWaveform class."""

    def test_init(self, mock_l1b_file, mock_l2_txt_file):
        """Test initialization of LVISWaveform."""
        with h5py.File(mock_l1b_file, 'r') as l1b:
            # Create a waveform for the first shot
            wf = LVISWaveform(
                shot_number=7332097,
                l1b=l1b,
                l2_txt_path=mock_l2_txt_file
            )
            
            # Check basic metadata
            assert wf.get_data("metadata/shot_number") == 7332097
            assert wf.get_data("metadata/lfid") == 1657449111
            assert wf.get_data("metadata/l1b_path") == mock_l1b_file
            assert wf.get_data("metadata/l2_path") == mock_l2_txt_file
            
            # Check coordinates
            coords = wf.get_data("metadata/coords")
            assert isinstance(coords, dict)
            assert "lon" in coords
            assert "lat" in coords
            assert "z" in coords
            
            # Check waveform data
            assert wf.get_data("raw/tx_wave") is not None
            assert wf.get_data("raw/rx_wave") is not None
            assert wf.get_data("raw/mean_noise") is not None
            
            # Check L2 data
            assert wf.get_data("raw/rh") is not None
            assert "RH50" in wf.get_data("raw/rh")
            assert "RH75" in wf.get_data("raw/rh")
            assert "RH90" in wf.get_data("raw/rh")
            assert "RH95" in wf.get_data("raw/rh")
            assert "RH100" in wf.get_data("raw/rh")
            
            # Check ground elevation
            assert wf.get_data("raw/elev/ground") is not None
    
    def test_equality(self, mock_l1b_file, mock_l2_txt_file):
        """Test equality comparison of LVISWaveform objects."""
        with h5py.File(mock_l1b_file, 'r') as l1b:
            # Create two waveforms with the same shot number
            wf1 = LVISWaveform(
                shot_number=7332097,
                l1b=l1b,
                l2_txt_path=mock_l2_txt_file
            )
            
            wf2 = LVISWaveform(
                shot_number=7332097,
                l1b=l1b,
                l2_txt_path=mock_l2_txt_file
            )
            
            # Create a waveform with a different shot number
            wf3 = LVISWaveform(
                shot_number=7332098,
                l1b=l1b,
                l2_txt_path=mock_l2_txt_file
            )
            
            # Test equality
            assert wf1 == wf2
            assert wf1 != wf3
            assert hash(wf1) == hash(wf2)
            assert hash(wf1) != hash(wf3)
    
    def test_immutability(self, mock_l1b_file, mock_l2_txt_file):
        """Test that data is immutable by default."""
        with h5py.File(mock_l1b_file, 'r') as l1b:
            wf = LVISWaveform(
                shot_number=7332097,
                l1b=l1b,
                l2_txt_path=mock_l2_txt_file
            )
            
            # Get coordinates and modify them
            coords = wf.get_data("metadata/coords")
            original_lon = coords["lon"]
            coords["lon"] = 999.999
            
            # Check that the original data is unchanged
            assert wf.get_data("metadata/coords")["lon"] == original_lon
    
    def test_mutable_option(self, mock_l1b_file, mock_l2_txt_file):
        """Test that data can be made mutable."""
        with h5py.File(mock_l1b_file, 'r') as l1b:
            wf = LVISWaveform(
                shot_number=7332097,
                l1b=l1b,
                l2_txt_path=mock_l2_txt_file,
                immutable=False
            )
            
            # Get coordinates and modify them
            coords = wf.get_data("metadata/coords")
            coords["lon"] = 999.999
            
            # Check that the data was changed
            assert wf.get_data("metadata/coords")["lon"] == 999.999
    
    def test_error_handling(self, mock_l1b_file, mock_l2_txt_file):
        """Test error handling for invalid inputs."""
        with h5py.File(mock_l1b_file, 'r') as l1b:
            # Test with non-existent shot number
            with pytest.raises(ValueError, match="Shot number .* not found"):
                LVISWaveform(
                    shot_number=99999,
                    l1b=l1b,
                    l2_txt_path=mock_l2_txt_file
                )
            
            # Test with invalid path in save_data
            wf = LVISWaveform(
                shot_number=7332097,
                l1b=l1b,
                l2_txt_path=mock_l2_txt_file
            )
            
            with pytest.raises(ValueError, match="Invalid path provided"):
                wf.save_data(data="test", path="invalid/path")
