"""
Constant value derivation
"""

from typing import Any
import polars as pl
from .base import BaseDerivation


class ConstantDerivation(BaseDerivation):
    """Derive column with constant value"""
    
    def derive(self, 
               source_data: dict[str, pl.DataFrame],
               target_df: pl.DataFrame,
               column_spec: dict[str, Any]) -> pl.DataFrame:
        """Add constant value column."""
        col_name = column_spec["name"]
        constant_value = column_spec["derivation"]["constant"]
        
        # Determine number of rows
        n_rows = target_df.height if target_df.height > 0 else source_data.get("DM", pl.DataFrame()).height
        
        if n_rows == 0:
            raise ValueError("Cannot determine number of rows")
        
        # Add constant column
        values = pl.Series([constant_value] * n_rows)
        
        if target_df.height == 0:
            return pl.DataFrame({col_name: values})
        else:
            return target_df.with_columns(values.alias(col_name))