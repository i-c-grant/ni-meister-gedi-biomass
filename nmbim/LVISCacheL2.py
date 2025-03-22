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

        # Step 2: Build DataFrame
        data = []
        for line in lines:
            row = line.strip().split()
            if len(row) == len(header):
                data.append(row)
            else:
                print(f"Warning: Skipping malformed row: {line}")
        
        df = pd.DataFrame(data, columns=header)
        
        # Step 3: Precompute column names and index
        self.lfid_col = next(c for c in df.columns if c.upper() == "LFID")
        self.shot_col = next(c for c in df.columns if c.upper() == "SHOTNUMBER")
        
        # Convert numeric columns and set index
        for col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col])
            except Exception:
                pass
        
        df = df.set_index([self.lfid_col, self.shot_col])
        self._cache = df

    def extract_value(self, lfid: int, shot_number: int, field: str) -> Any:
        """Get value from cached DataFrame using precomputed index."""
        try:
            return self._cache.loc[(lfid, shot_number), field]
        except KeyError:
            return None

    def get_shot_number(self, index: int) -> int:
        """Get shot number at given index."""
        return self._cache.index[index][1]  # Index is (LFID, SHOTNUMBER) tuples
