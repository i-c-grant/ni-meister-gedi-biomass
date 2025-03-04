from typing import Any, Dict, Set


class NestedDict:
    """A class to manage nested dictionaries with path-based access.

    This class allows storing and retrieving data in a nested dictionary
    structure using string paths similar to file system paths. It also
    keeps track of all available data paths.

    Attributes
    ----------
    _data : Dict[str, Any]
        The root dictionary storing all nested data.
    _paths : Set[str]
        A set of all terminal paths where data is stored.
    """

    def __init__(self) -> None:
        """Initialize the nested dictionary."""
        self._data: Dict[str, Any] = {}
        self._paths: Set[str] = set()

    def get_paths(self) -> Set[str]:
        """Returns a set of terminal paths in the nested dictionary."""
        return self._paths

    def get_data(self, path: str) -> Any:
        """Retrieve data from the nested dictionary with '/' separated path.

        Parameters
        ----------
        path : str
            The '/' separated path to the data.

        Returns
        -------
        Any
            The data stored at the specified path.

        Raises
        ------
        KeyError
            If the path does not exist in the nested dictionary.
        """
        keys = path.strip("/").split("/")

        if not keys or any(not key for key in keys):
            raise ValueError("Invalid path provided.")

        data = self._data
        for key in keys:
            if isinstance(data, dict) and key in data:
                data = data[key]
            else:
                raise KeyError(
                    f"Path '{path}' not found in NestedDict."
                    f" Available paths: {sorted(self._paths)}"
                )
        return data

    def has_path(self, path: str) -> bool:
        """Check if the nested dictionary contains the specified path.
        Invalid paths are considered not to exist.
        """
        try:
            self.get_data(path)
            return True
        except (KeyError, ValueError):
            return False

    def save_data(self, data: Any, path: str, overwrite: bool = False) -> None:
        """Save data to the nested dictionary at the specified path.
        Optionally disallows overwriting existing data at the path.

        Parameters
        ----------
        data : Any
            The data to be stored.

        path : str
            The '/' separated path where the data will be stored.

        overwrite : bool, optional
            Whether to allow overwriting existing data at the path.
            Defaults to False.

        Raises
        ------
        ValueError
            If the path already exists and overwriting is not allowed.

        ValueError
            If the path is invalid.

        TypeError
            If an intermediate key in the path is not a dictionary.
        """
        normalized_path = path.strip("/")
        if not overwrite and normalized_path in self._paths and self.get_data(path) is not None:
            raise ValueError(
                f"There is already data at path '{path}'. "
                f"Use overwrite=True to replace it."
            )

        keys = normalized_path.split("/")
        if not keys or any(not key for key in keys):
            raise ValueError("Invalid path provided.")

        # Traverse the nested dictionary to the correct location
        current = self._data
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            elif not isinstance(current[key], dict):
                raise TypeError(
                    f"Cannot create subkey under non-dict "
                    f"key '{key}', as it is not a dictionary."
                )
            current = current[key]

        # Save the data at the terminal key
        current[keys[-1]] = data

        # Update paths with new terminal paths
        if isinstance(data, dict):
            new_paths = NestedDict._find_terminal_paths(
                nested_dict=data, parent_path=normalized_path
            )
        else:
            new_paths = {normalized_path}
        self._paths.update(new_paths)

    def _get_all_paths(self) -> Set[str]:
        """Retrieve all terminal paths from the nested dictionary.

        Returns
        -------
        Set[str]
            A set of all terminal paths in the nested dictionary.
        """
        paths = self._find_terminal_paths(
            nested_dict=self._data, parent_path=""
        )
        return paths

    @staticmethod
    def _find_terminal_paths(
        nested_dict: Dict[str, Any], parent_path: str
    ) -> Set[str]:
        """Recursively find and return all terminal paths."""
        paths = set()
        for key, value in nested_dict.items():
            current_path = f"{parent_path}/{key}" if parent_path else key
            if isinstance(value, dict):
                paths.update(
                    NestedDict._find_terminal_paths(value, current_path)
                )
            else:
                paths.add(current_path)
        return paths
