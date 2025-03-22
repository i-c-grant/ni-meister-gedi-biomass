import pandas as pd
from typing import Optional, Any

class LVISCacheL2:
    def __init__(self, filepath: str):
        """Initialize with the path to the L2 TXT file.
        
        Args:
            filepath: Path to the L2 TXT file
        """
        self.filepath = filepath
        self._cache = None  # Instance-specific DataFrame cache
        self.lfid_col = None
        self.shot_col = None
        self._load_cache()  # Load data during initialization
        self.max_index = len(self._cache.index)

    def _load_cache(self) -> None:
        """Parse L2 TXT file and cache as a DataFrame with MultiIndex."""
        # Step 1: Read and parse the file
        with open(self.filepath, "r") as f:
            lines = []
            header = None
            for line in f:
                if not line.startswith("#"):
                    lines.append(line)
                elif "LFID" in line:
                    header = line.lstrip("#").strip().split()

            if not header:
                raise ValueError("Header with 'LFID' not found in file")

        # Step 2: Build DataFrame with only required columns
        required_columns = {"LFID", "SHOTNUMBER", "RH50", "RH75", "RH90", "RH95", "RH100", "ZG", "ZH", "ZT"}
        
        # Find indices of required columns in header
        col_indices = []
        present_columns = []
        for idx, col in enumerate(header):
            if col.upper() in required_columns:
                col_indices.append(idx)
                present_columns.append(col)
        
        missing = required_columns - set(col.upper() for col in present_columns)
        if missing:
            print(f"Warning: Missing required columns in {self.filepath}: {missing}")

        # Build data with only required columns
        data = []
        for line in lines:
            row = line.strip().split()
            if len(row) != len(header):
                print(f"Warning: Skipping malformed row: {line}")
                continue
                
            # Only keep columns we need
            filtered_row = [row[i] for i in col_indices]
            data.append(filtered_row)
        
        # Create DataFrame with explicit schema
        dtype_map = {
            "LFID": "Int64",
            "SHOTNUMBER": "Int64",
            "RH50": "float32",
            "RH75": "float32", 
            "RH90": "float32",
            "RH95": "float32",
            "RH100": "float32",
            "ZG": "float32",
            "ZH": "float32",
            "ZT": "float32"
        }
        
        # Convert data to appropriate types during DataFrame creation
        typed_data = []
        for row in data:
            typed_row = []
            for val, col in zip(row, present_columns):
                col_upper = col.upper()
                if col_upper in ("LFID", "SHOTNUMBER"):
                    typed_row.append(int(val) if val.isdigit() else None)
                else:
                    typed_row.append(float(val) if self._is_float(val) else None)
            typed_data.append(typed_row)
        
        # Create DataFrame with enforced types
        df = pd.DataFrame(typed_data, columns=present_columns).astype(dtype_map)
        
        # Set index
        self.lfid_col = "LFID" if "LFID" in df.columns else df.columns[0]
        self.shot_col = "SHOTNUMBER" if "SHOTNUMBER" in df.columns else df.columns[1]
        df = df.set_index([self.lfid_col, self.shot_col])
        self._cache = df

    def _is_float(self, val: str) -> bool:
        """Helper method to check if a string can be converted to float."""
        try:
            float(val)
            return True
        except ValueError:
            return False

    def get_max_index(self) -> int:
        return self.max_index

    def extract_value(self, lfid: int, shot_number: int, field: str) -> Any:
        """Get value from cached DataFrame using precomputed index."""
        try:
            return self._cache.loc[(lfid, shot_number), field]
        except KeyError:
            return None

    def get_shot_number(self, index: int) -> int:
        """Get shot number at given index."""
        return self._cache.index[index][1]  # Index is (LFID, SHOTNUMBER) tuples
