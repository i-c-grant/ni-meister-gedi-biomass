from dataclasses import dataclass, field

from typing import List, Callable, Union, Optional
from pathlib import Path
import h5py
import numpy as np

from nmbim.Beam import Beam
from nmbim.Waveform import Waveform

Filter = Callable[[Waveform], bool]

class WaveformCollection:
    """
    A collection of Waveform objects created from GEDI L1B and L2A files.

    The class reads waveform data from two HDF5 files, checks consistency, 
    and constructs Waveform objects for each shot. It applies one or more 
    user-defined filters to include only waveforms that meet the specified 
    criteria.

    Attributes:
        l1b_path (Path): Path to the L1B HDF5 file.
        l2a_path (Path): Path to the L2A HDF5 file.
        limit (Optional[int]): Limit on the number of waveforms to process.
        waveforms (List[Waveform]): List of filtered Waveform objects.
        filters (List[Filter]): List of filter functions to apply.

    Methods:
        add_waveform(wf: Waveform):
            Adds a waveform if it passes the filters.

        __iter__():
            Returns an iterator over the waveforms.
    """

    def __init__(
            self, 
            input_l1b: str,
            input_l2a: str,
            filters: Union[Filter, List[Filter]] = None,
            limit: Optional[int] = None,
            cache_beams: bool = True
    ):
        """
        Initialize the WaveformCollection by loading waveform data from two HDF5 files.

        Parameters:
            input_l1b (str): Path to the L1B HDF5 file.
            input_l2a (str): Path to the L2A HDF5 file.
            filters (Union[Filter, List[Filter]], optional): A filter function or list 
                of functions to apply to each waveform. Defaults to None (no filters).
            limit (Optional[int], optional): Maximum number of waveforms to process. 
                Defaults to None (process all waveforms).
        """
        
        self.l1b_path = Path(input_l1b)
        self.l2a_path = Path(input_l2a)
        self.limit = limit
        self.waveforms = []
        self.cache_beams = cache_beams

        if filters is None:
            filters = []
        elif callable(filters):
            filters = [filters]

        self.filters = filters

        # Read in waveform data
        with h5py.File(input_l1b, "r") as l1b, h5py.File(input_l2a, "r") as l2a:

            # Get beam names, excluding metadata group
            beams: List[str] = [key for key in l1b.keys() if key != 'METADATA']

            # Construct waveforms for each beam, caching a beam at a time
            for beam_name in beams:
                l1b_beam = Beam(file=l1b, beam=beam_name, cache=self.cache_beams)
                l2a_beam = Beam(file=l2a, beam=beam_name, cache=self.cache_beams)

                shot_numbers_l1b: ArrayLike = l1b_beam.extract_dataset("shot_number")
                shot_numbers_l2a: ArrayLike = l2a_beam.extract_dataset("shot_number")

                # Check that shot numbers match between files
                if not np.array_equal(shot_numbers_l1b, shot_numbers_l2a):
                    raise ValueError(
                        f"Shot numbers don't match between {input_l1b}"
                        f"and {input_l2a}"
                    )

                # Check that both files have the same number of shots
                if len(shot_numbers_l1b) != len(shot_numbers_l2a):
                    raise ValueError(
                        f"{input_l1b} has {len(shot_numbers_l1b)} shots,"
                        f"but {input_l2a} has {len(shot_numbers_l2a)} shots."
                    )

                if limit is None:
                    limit = len(shot_numbers_l1b)

                shot_numbers = shot_numbers_l1b[:limit]

                # Create the Waveforms for this beam
                waveform_args = {"l1b_beam": l1b_beam, "l2a_beam": l2a_beam}

                for shot_number in shot_numbers:
                        waveform_args["shot_number"] = shot_number
                        # Add waveforms if they pass the collection's filters
                        self.add_waveform(Waveform(**waveform_args))

    def add_waveform(self, wf: Waveform):
        """Add a waveform to collection if it passes the collection's filters."""
        if all(filt(wf) for filt in self.filters):
            self.waveforms.append(wf)

    def __iter__(self):
        return iter(self.waveforms)