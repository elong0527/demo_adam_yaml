"""
Source mapping derivation with optional value recoding
"""

from typing import Any
import polars as pl
import logging
from .base import BaseDerivation


class SourceDerivation(BaseDerivation):
    """Direct source mapping with optional value mapping"""
    
    def derive(self, 
                source_data: dict[str, pl.DataFrame],
                target_df: pl.DataFrame,
                column_spec: dict[str, Any]) -> pl.DataFrame:
        """
        Map values from source with optional recoding.
        Ensures one row per unique key combination.
        
        Args:
            source_data: Dictionary of source datasets (with renamed columns)
            target_df: Current target dataset
            column_spec: Column specification
        
        Returns:
            Series with mapped values (one per key combination)
        """
        derivation = column_spec.get("derivation", {})
        source_str = derivation.get("source")
        
        if not source_str:
            raise ValueError(f"No source specified for {column_spec.get('name')}")
        
        # Find the DataFrame containing the source column
        source_df = self.find_column(source_str, source_data, target_df)
        
        # Apply filter if specified
        filter_expr = derivation.get("filter", "")
        if filter_expr:
            source_df = self.apply_filter(source_df, filter_expr, source_data)
        
        # Verify column exists
        if source_str not in source_df.columns:
            raise ValueError(f"Column {source_str} not found in source dataset")
        
        # Get key columns from target
        key_cols = []
        if target_df.height > 0:
            # Find key columns that exist in both dataframes
            for col in target_df.columns:
                if col in source_df.columns and col.isupper() and len(col) <= 8:
                    # Likely a key variable (USUBJID, SUBJID, etc.)
                    key_cols.append(col)
        
        if key_cols and target_df.height > 0:
            # Join to ensure alignment with target keys
            cols_to_select = key_cols + [source_str]
            cols_to_select = list(dict.fromkeys(cols_to_select))  # Remove duplicates
            
            # Left join to maintain target structure
            result_df = target_df.select(key_cols).join(
                source_df.select(cols_to_select),
                on=key_cols,
                how="left"
            )
            result = result_df[source_str]
        else:
            # Direct extraction if no target yet
            result = source_df[source_str]
        
        # Apply value mapping if specified
        mapping = derivation.get("mapping")
        if mapping:
            logging.getLogger(__name__).info(f"Applying value mapping: {mapping}")
            clean_mapping = {}
            for old_val, new_val in mapping.items():
                if old_val != "":
                    clean_mapping[old_val] = new_val
            
            result = result.replace(clean_mapping, default=None if "" in mapping and mapping[""] is None else pl.first())
        
        # Add to dataframe
        col_name = column_spec["name"]
        if target_df.height == 0:
            return pl.DataFrame({col_name: result})
        else:
            return target_df.with_columns(result.alias(col_name))