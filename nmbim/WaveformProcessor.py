from nmbim.Waveform import Waveform
from typing import Any, Callable, Dict, List, Optional
from collections import Deque

class WaveformProcessor:
    """Object to process Waveforms with one algorithm and parameter set.
    
    Attributes
    ----------
    alg_fun: Callable
        The algorithm function to apply to the waveform.

    params: Dict[str, Any]
        Dictionary containing the parameters for the algorithm.

    input_map: Dict[str, str]
        Dictionary mapping algorithm input names to waveform data keys, where
    the keys are the algorithm function's arguments and the values are the
    paths to the data in the Waveform.

    output_path: str
        Path indicating where to save processed data in Waveform.

    Methods
    -------
    process() -> None
        Applies the algorithm and saves the results to the Waveform.
"""

    def __init__(self,
                 alg_fun: Callable,
                 input_map: Dict[str, str],
                 output_path: str,
                 params: Dict[str, Any],
        ) -> None:
        """Initializes the WaveformProcessor object.

        Parameters
        ----------
        alg_fun: Callable
            The algorithm function to apply to the waveform.

        params: Dict[str, Any]
            Dictionary containing the parameters for the algorithm.

        input_map: Dict[str, str]
            Dictionary mapping algorithm input names to waveform data keys.

        output_path: str
            Path indicating where to save processed data in Waveform.

        queue: Deque[Waveform]
            Deque of Waveform objects to process. First in, first out.
        """
            
        self.alg_fun: Callable = alg_fun
        self.params: Dict[str, Any] = params
        self.input_map: Dict[str, str] = input_map
        self.output_path: str = output_path
        self.queue: Deque[Waveform] = Deque()

    def add_waveform(self, waveform: Waveform) -> None:
        """Adds a waveform to the processing queue."""
        self.queue.append(waveform)

    def process(self) -> None:
        """Applies the algorithm to the waveform data."""
        while self.queue:
            self._process_next()

    def _process_next(self) -> None:
        """Applies the algorithm to the waveform data,
        modifying the next waveform in processing queue in place.
        """
        # Get data from waveform
        waveform: Waveform = self.queue.popleft()
        data: Dict[str, Any] = {}

        for key in self.input_map:
            path_to_data: List[str] = self.input_map[key]
            data[key] = waveform.get_data(path_to_data)

        # Apply algorithm
        results = self.alg_fun(**data, **self.params)
        
        # Save results to waveform
        waveform.save_data(results, self.output_path)
        
        self.complete = True

    @property
    def params(self) -> Dict[str, Any]:
        return self._params

    @params.setter
    def params(self, params: Dict[str, Any]) -> None:
        if not isinstance(params, dict):
            raise TypeError("params must be a dictionary")
        self._params = params

    def has_params(self) -> bool:
        """Check if the processor has been supplied parameters.

        None means no parameters have been supplied.
        A dict (even an empty one) means parameters have been supplied.
        """
        return bool(self.params is not None)

    def clear_params(self) -> None:
        """Clear the parameters from the processor."""
        self.params = None

    def __repr__(self) -> str:
        return f"WaveformProcessor(alg_fun={self.alg_fun.__name__}, params={self.params}, input_map={self.input_map}, output_path={self.output_path}, complete={self.complete})"

    def __str__(self) -> str:
        return f"WaveformProcessor object: {self.alg_fun.__name__}"
    
    
