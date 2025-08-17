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
               column_spec: dict[str, Any]) -> pl.Series:
        """
        Apply constant value to all records
        
        Args:
            source_data: Dictionary of source datasets (not used)
            target_df: Current target dataset
            column_spec: Column specification with 'constant' field
        
        Returns:
            Series with constant value
        """
        derivation = column_spec.get("derivation", {})
        constant_value = derivation.get("constant")
        
        if constant_value is None:
            raise ValueError(f"No constant value specified for {column_spec.get('name')}")
        
        # Get the number of rows from target_df or use a default
        if target_df.height == 0:
            # If target is empty, try to get length from DM dataset
            if "DM" in source_data:
                n_rows = source_data["DM"].height
            else:
                raise ValueError("Cannot determine number of rows for constant derivation")
        else:
            n_rows = target_df.height
        
        self.logger.info(f"Applying constant value '{constant_value}' to {n_rows} rows")
        
        # Create a series with the constant value
        return pl.Series([constant_value] * n_rows)