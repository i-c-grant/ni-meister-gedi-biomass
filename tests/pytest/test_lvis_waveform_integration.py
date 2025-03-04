import os
import h5py
import pytest
from pathlib import Path

from nmbim.LVISWaveform import LVISWaveform
from nmbim.LVISCacheL1 import LVISCacheL1
from nmbim.LVISCacheL2 import LVISCacheL2


class TestLVISWaveformIntegration:
    """Integration tests for LVISWaveform using real LVIS data."""

    @pytest.fixture
    def lvis_data_path(self):
        """Path to the LVIS test data directory."""
        return Path("tests/data/lvis")

    @pytest.fixture
    def l1b_file_path(self, lvis_data_path):
        """Path to the L1B HDF5 file."""
        return lvis_data_path / "LVIS1B_Gabon2016_0222_R1808_043849.h5"

    @pytest.fixture
    def l2_file_path(self, lvis_data_path):
        """Path to the L2 TXT file."""
        return lvis_data_path / "LVIS2_Gabon2016_0222_R1808_043849.TXT"

    def test_load_real_data(self, l1b_file_path, l2_file_path):
        """Test loading real LVIS data files."""
        # Skip test if files don't exist
        if not l1b_file_path.exists() or not l2_file_path.exists():
            pytest.skip(f"LVIS data files not found at {l1b_file_path} or {l2_file_path}")

        # Open the L1B file to get a shot number
        with h5py.File(l1b_file_path, 'r') as l1b:
            # Get the first shot number from the file
            shot_numbers = l1b['SHOTNUMBER'][:]
            first_shot = int(shot_numbers[0])
            
            # Create cache objects
            l1_cache = LVISCacheL1(str(l1b_file_path))
            l2_cache = LVISCacheL2(str(l2_file_path))
            
            # Create a waveform for this shot
            wf = LVISWaveform(
                shot_number=first_shot,
                l1_cache=l1_cache,
                l2_cache=l2_cache
            )
            
            # Verify basic metadata
            assert wf.get_data("metadata/shot_number") == first_shot
            assert wf.get_data("metadata/lfid") is not None
            
            # Verify coordinates
            coords = wf.get_data("metadata/coords")
            assert isinstance(coords, dict)
            assert "lon" in coords
            assert "lat" in coords
            assert "z" in coords
            
            # Verify waveform data
            tx_wave = wf.get_data("raw/tx_wave")
            rx_wave = wf.get_data("raw/rx_wave")
            assert tx_wave is not None
            assert rx_wave is not None
            assert len(tx_wave) == 128  # LVIS transmitted waveform is 128 bins
            assert len(rx_wave) == 1024  # LVIS return waveform is 1024 bins
            
            # Print some information about the waveform for debugging
            print(f"\nLVIS Waveform Integration Test:")
            print(f"Shot Number: {first_shot}")
            print(f"LFID: {wf.get_data('metadata/lfid')}")
            print(f"Coordinates: Lon={coords['lon']:.6f}, Lat={coords['lat']:.6f}")
            print(f"Elevation: {coords['z']:.2f} meters")
            
            # Check if L2 data was loaded
            if wf._l2_loaded:
                rh = wf.get_data("raw/rh")
                print(f"RH metrics: RH50={rh['RH50']:.2f}, RH100={rh['RH100']:.2f}")
                
                # Verify RH metrics
                assert "RH50" in rh
                assert "RH75" in rh
                assert "RH90" in rh
                assert "RH95" in rh
                assert "RH100" in rh
                
                # Verify ground elevation
                ground_elev = wf.get_data("raw/elev/ground")
                assert ground_elev is not None
                print(f"Ground elevation: {ground_elev:.2f} meters")
            else:
                print("L2 data was not loaded - check if shot exists in L2 file")

    def test_multiple_shots(self, l1b_file_path, l2_file_path):
        """Test loading multiple shots from real LVIS data."""
        # Skip test if files don't exist
        if not l1b_file_path.exists() or not l2_file_path.exists():
            pytest.skip(f"LVIS data files not found at {l1b_file_path} or {l2_file_path}")

        # Create cache objects
        l1_cache = LVISCacheL1(str(l1b_file_path))
        l2_cache = LVISCacheL2(str(l2_file_path))
        
        # Get the first few shot numbers
        with h5py.File(l1b_file_path, 'r') as l1b:
            shot_numbers = l1b['SHOTNUMBER'][:]
            test_shots = shot_numbers[:5]  # Test first 5 shots
        
        waveforms = []
        for shot in test_shots:
            try:
                wf = LVISWaveform(
                    shot_number=int(shot),
                    l1_cache=l1_cache,
                    l2_cache=l2_cache
                )
                waveforms.append(wf)
            except Exception as e:
                print(f"Error loading shot {shot}: {e}")
            
            # Verify we loaded at least one waveform
            assert len(waveforms) > 0
            
            # Check that waveforms are different
            if len(waveforms) > 1:
                # Waveforms with different shot numbers should not be equal
                assert waveforms[0] != waveforms[1]
                
                # Hash values should be different
                assert hash(waveforms[0]) != hash(waveforms[1])
                
                # Print some stats about the waveforms
                print(f"\nLoaded {len(waveforms)} waveforms")
                for i, wf in enumerate(waveforms):
                    coords = wf.get_data("metadata/coords")
                    print(f"Waveform {i}: Shot={wf.get_data('metadata/shot_number')}, "
                          f"Lon={coords['lon']:.6f}, Lat={coords['lat']:.6f}")
