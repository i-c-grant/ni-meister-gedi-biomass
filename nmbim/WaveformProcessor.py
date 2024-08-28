from nmbim.Waveform import Waveform
from typing import Any, Callable, Dict, List, Iterable
from dataclasses import dataclass, field

@dataclass(frozen=True)
class WaveformProcessor:
    """Responsible for processing one collection of Waveforms with one algorithm.
    Attributes are supplied at initialization and are immutable.
    Processsing function can only be called once.

    Attributes
    ----------
    waveforms: Iterable[Waveform]
        Collection of Waveform objects to process in place.

    alg_fun: Callable
        The algorithm function to apply to each waveform in the supplied collection.

    params: Dict[str, Any]
        Dictionary containing the parameters for the algorithm function.
    Parameters are algorithm inputs other than the waveform data itself.

    input_map: Dict[str, str]
        Dictionary mapping algorithm function arguments to Waveform data paths.
    Together, params and input_map should contain all arguments that alg_fun requires.

    output_path: str
        Path indicating where to save processed data in each Waveform.

    Methods
    -------
    process() -> None
        Applies the algorithm and saves the results to the Waveform.
    Can only be called once.
"""
    alg_fun: Callable
    params: Dict[str, Any]
    input_map: Dict[str, str]
    output_path: str
    waveforms: Iterable[Waveform]

    _processed: bool = field(default=False, init=False, repr=False)

    def process(self) -> None:
        """Apply the algorithm to each waveform in the collection and save the results.
        Can only be called once to prevent accidental reprocessing.
        """
        if self._processed:
            raise RuntimeError("This WaveformProcessor has already been processed.")

        for w in self.waveforms:
            self._process_next(w)
        
        object.__setattr__(self, "_processed", True)

    def _process_next(self, waveform: Waveform) -> None:
        # Apply the algorithm to the next waveform in the collection

        # Get input data from waveform
        data: Dict[str, Any] = {}

        for key in self.input_map:
            path_to_data: List[str] = self.input_map[key]
            data[key] = waveform.get_data(path_to_data)

        # Apply algorithm
        results = self.alg_fun(**data, **self.params)
        
        # Save results to waveform
        waveform.save_data(results, self.output_path)
