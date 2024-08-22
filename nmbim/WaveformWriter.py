class WaveformWriter:
    def __init__(self, path: str, cols: Dict[str, str], append: bool = False):
        """
        Initialize the WaveformWriter with the file path, columns, and append mode.

        Parameters
        ----------
        path: str
            The path to the file where the waveform data will be written.
        cols: Dict[str, str]
            A dictionary mapping column names to data paths within the Waveform object.
        append: bool
            Whether to append to the file or overwrite it.
        """
        self.path = path
        self.cols = cols
        self.append = append
        self.data = {}
        self.col_len = None

    def _load_data(self, waveform) -> None:
        # Load data to write into the data dict
        for col_name in self.cols:
            data_path = self.cols[col_name]
            self.data[col_name] = waveform.get_data(data_path)
        self.col_len = len(list(self.data.values())[0])

    def _validate_data(self) -> None:
        # Validate types and lengths of columns
        for col_name, col_data in self.data.items():
            col_type = type(col_data)
            col_len = len(col_data)
            if col_type not in [h5py.Dataset, np.ndarray]:
                raise TypeError(
                    f"All CSV columns must be h5py datasets or numpy arrays, " 
                    f"but {col_name} is a {col_type}" 
                )
            if col_len != self.col_len:
                raise ValueError(
                    f"All CSV columns must have the same length; " 
                    f"column {col_name} has length {col_len}, " 
                    f"but the first column has length {self.col_len}" 
                )

    def _to_csv(self, waveform) -> None:
        """
        Write specified columns of the waveform to a CSV file.

        Parameters
        ----------
        waveform: Waveform
            The waveform object containing the data to be written.
        """

        shot_number: int = waveform.get_data("metadata/shot_number")
        beam: str = waveform.get_data("metadata/beam")

        self._load_data(waveform)
        self._validate_data()

        # Write to the file
        with open(self.path, "a" if self.append else "w", newline="") as csv_file:
            writer = csv.writer(csv_file)

            # Write header if file is empty
            if csv_file.tell() == 0:
                header = ["shot_number", "beam"] + list(self.data.keys())
                writer.writerow(header)

            # Write data rows
            for i in range(self.col_len):
                row = [shot_number, beam] + [col_data[i] for col_data in self.data.values()]
                writer.writerow(row)

    def write(self, waveform):
        """
        Process and write the waveform data to the CSV file.

        Parameters
        ----------
        waveform: Waveform
            The waveform object containing the data to be written.
        """
        self._to_csv(waveform)

    def reset(self) -> None:
        """
        Reset the internal state of the WaveformWriter.

        This clears the data dictionary and resets the column length,
        preparing the WaveformWriter to handle a new waveform.
        """
        self.data.clear()
        self.col_len = None

    def __repr__(self) -> str:
        return f"WaveformWriter(path={self.path!r}, cols={self.cols!r}, append={self.append})"

    def __str__(self) -> str:
        return f"WaveformWriter writing to {self.path} with columns {list(self.cols.keys())}"


