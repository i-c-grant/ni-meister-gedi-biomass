import pandas as pd
from typing import Optional, Any

class LVISCacheL2:
    _cache = {}

    @classmethod
    def load(cls, filepath: str) -> pd.DataFrame:
        """Load the L2 TXT file, parse the header and data into a DataFrame,
        and cache it for subsequent calls."""
        if filepath in cls._cache:
            return cls._cache[filepath]

        with open(filepath, "r") as f:
            lines = []
            header = None
            for line in f:
                if not line.startswith("#"):
                    lines.append(line)
                elif "LFID" in line:
                    header = line.lstrip("#").strip().split()

            if not header:
                raise ValueError("Header with 'LFID' not found in file")

        header_length = len(header)
        data = []
        
        for line in lines:
            row = line.strip().split()
            if len(row) == header_length:
                data.append(row)
            else:
                print(f"Warning: Skipping row with incorrect number of fields: {line}")
        df = pd.DataFrame(data, columns=header)
        # Attempt conversion of numeric columns.
        for col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col])
            except Exception:
                pass
        cls._cache[filepath] = df
        return df
        
    def __init__(self, filepath: str):
        """Initialize with the path to the L2 TXT file.
        
        Args:
            filepath: Path to the L2 TXT file
        """
        self.filepath = filepath
        # Ensure the file is loaded into the cache
        self.load(filepath)
        
    def find_column(self, field_name: str) -> Optional[str]:
        """Find a column name in the L2 data that matches the given field name (case-insensitive).
        
        Args:
            field_name: Field name to find (case-insensitive)
            
        Returns:
            The actual column name if found, None otherwise
        """
        df = self.__class__.load(self.filepath)
        col = next((c for c in df.columns if c.upper() == field_name.upper()), None)
        return col
        
    def extract_value(self, lfid: int, shot_number: int, field: str) -> Any:
        """Extract a value from the L2 data for a specific shot.
        
        Args:
            lfid: LFID to match
            shot_number: Shot number to match
            field: Field name to extract (case-insensitive)
            
        Returns:
            The value at the specified field for the matching row,
            or None if the field or row is not found
        """
        df = self.__class__.load(self.filepath)
        if df.empty:
            return None
            
        # Find the column names for LFID and SHOTNUMBER (case-insensitive)
        lfid_col = self.find_column("LFID")
        shot_col = self.find_column("SHOTNUMBER")
        
        if not lfid_col or not shot_col:
            return None
            
        # Find the matching row
        matching_rows = df[(df[lfid_col] == lfid) & (df[shot_col] == shot_number)]
        if matching_rows.empty:
            return None
            
        # Find the column for the requested field (case-insensitive)
        field_col = self.find_column(field)
        if not field_col:
            return None
            
        # Return the value
        return matching_rows.iloc[0][field_col]
