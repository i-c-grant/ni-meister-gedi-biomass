class WaveformProcessor:
    """Object to process one Waveform with an algorithm.
    
    Attributes
    ----------
    waveform: Waveform
        The waveform object to process.

    alg_fun: Callable
        The algorithm function to apply to the waveform.

    params: dict[str, Any]
        Dictionary containing the parameters for the algorithm.

    input_map: dict[str, List[str]]
        Dictionary mapping algorithm input names to waveform data keys.

    output_path: List[str]
        List of keys indicating where to save processed data in Waveform.

    complete: bool
        Flag indicating if the processing is complete.

    Methods
    -------
    process() -> None
        Applies the algorithm and saves the results to the Waveform.
"""

    def __init__(self,
                 waveform: Waveform,
                 alg_fun: Callable,
                 params: Dict[str, Any],
                 input_map: Dict[str, List[str]],
                 output_path: List[str])
        ) -> None:
        """Initializes the WaveformProcessor object.

        Parameters
        ----------
        waveform: Waveform
            The waveform object to process.

        alg_fun: Callable
            The algorithm function to apply to the waveform.

        params: dict[str, Any]
            Dictionary containing the parameters for the algorithm.

        input_map: dict[str, List[str]]
            Dictionary mapping algorithm input names to waveform data keys.

        output_path: List[str]
            List of keys indicating where to save processed data in Waveform.
        """
            
        self.waveform: Waveform = waveform
        self.alg_fun: Callable = alg_fun
        self.params: Dict[str, Any] = params
        self.input_map: Dict[str, Any] = input_map
        self.output_path: List[str] = output_path
        self.complete: bool = False

    def process(self) -> None:
        """Applies the algorithm to the waveform data."""
        # Get data from waveform
        data: Dict[str, Any] = {}
        for key in self.input_map:
            path_to_data: List[str] = self.input_map[key]
            data[key] = waveform.get_data(path_to_data)
        
        # Apply algorithm
        results = self.alg_fun(**data, **self.params)
        
        # Save results to waveform
        self.waveform.save_data(results, self.output)
        
        self.complete = True
