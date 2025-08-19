"""Minimal base class for derivations."""

from abc import ABC, abstractmethod
from typing import Any
import polars as pl


class BaseDerivation(ABC):
    """Simple abstract base for all derivations."""
    
    @abstractmethod
    def derive(self, 
               col_spec: dict[str, Any], 
               source_data: dict[str, pl.DataFrame],
               target_df: pl.DataFrame) -> pl.Series:
        """
        Derive a column based on specification.
        
        Args:
            col_spec: Column specification including derivation rules
            source_data: Dictionary of available source DataFrames
            target_df: Target DataFrame being built
            
        Returns:
            Series with derived values
        """
        pass