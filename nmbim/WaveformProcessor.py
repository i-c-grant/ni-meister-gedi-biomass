from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, Optional, Union

from nmbim import Waveform, WaveformCollection


class ProcessorState:
    """Stores the state of a WaveformProcessor object, which is otherwise immutable.

    Attributes
    ----------
    processed: bool
        Indicates whether the WaveformProcessor has been processed.

    waveform_iter: Iterator[Waveform]
        Iterator over the collection of Waveforms to process.
    """

    def __init__(self) -> None:
        self.processed: bool = False

    def mark_processed(self) -> None:
        self.processed = True

    def set_waveform_iter(
        self, waveforms: Union[WaveformCollection, Iterable[Waveform]]
    ) -> None:
        self.waveform_iter = iter(waveforms)

    def was_processed(self) -> bool:
        return self.processed


@dataclass(frozen=True)
class WaveformProcessor:
    """Processes one collection of Waveforms in-place with one algorithm.

    This class serves as an interface between a collection of Waveform
    objects (either a WaveformCollection or an iterable of Waveforms)
    and an algorithm function, allowing the algorithm functions to be
    specified separately from the Waveform structure. In order to simplify
    usage, attributes are supplied at initialization and are
    immutable, and processing can only be done once.
    Attributes
    ----------
    waveforms: Union[WaveformCollection, Iterable[Waveform]]
    Collection of Waveform objects to process in place. A single
    waveform can be supplied and will be wrapped in a list.

    alg_fun: Callable
    The algorithm function to apply to each waveform in the supplied
    collection.

    params: Dict[str, Any]
    Dictionary containing the parameters for
    the algorithm function. Parameters are algorithm inputs other
    than the waveform data itself.

    input_map: Dict[str, str]
    Dictionary mapping algorithm function
    arguments to Waveform data paths. Together, params and
    input_map should contain all arguments that alg_fun requires.

    output_path: str
    Path indicating where to save processed data in
    each Waveform.

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
    waveforms: Union[WaveformCollection, Iterable[Waveform]]

    _state: ProcessorState = field(
        init=False, default_factory=ProcessorState, repr=False
    )

    def __post_init__(self) -> None:
        # Ensure waveforms is an iterable
        if isinstance(self.waveforms, Waveform):
            # TODO: fix this, maybe just get rid of frozenness?
            object.__setattr__(self, "waveforms", [self.waveforms])

        self._state.set_waveform_iter(self.waveforms)

    def process(self) -> None:
        """Apply the algorithm to each waveform in the collection and
        save the results. Can only be called once to prevent
        accidental reprocessing.
        """
        if self._state.was_processed():
            raise RuntimeError(
                "This WaveformProcessor has already been processed."
            )

        while self._process_next() is not None:
            pass

        self._state.mark_processed()

    def _get_next(self) -> Optional[Waveform]:
        try:
            return next(self._state.waveform_iter)
        except StopIteration:
            return None

    def _process_next(self) -> Optional[Waveform]:
        # Apply the algorithm to the next waveform in the collection
        waveform = self._get_next()

        if waveform is not None:
            # Get input data from waveform
            data: Dict[str, Any] = {}

            for key in self.input_map:
                path_to_data: str = self.input_map[key]
                data[key] = waveform.get_data(path_to_data)

            # Apply algorithm
            results = self.alg_fun(**data, **self.params)

            # Save results to waveform
            waveform.save_data(results, self.output_path)

        return waveform
