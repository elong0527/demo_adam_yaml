"""
Source mapping derivation with optional value recoding
"""

from typing import Any
import polars as pl
from .base import BaseDerivation


class SourceDerivation(BaseDerivation):
    """Direct source mapping with optional value mapping"""
    
    def derive(self, 
               source_data: dict[str, pl.DataFrame],
               target_df: pl.DataFrame,
               column_spec: dict[str, Any]) -> pl.Series:
        """
        Map values from source with optional recoding
        
        Args:
            source_data: Dictionary of source datasets
            target_df: Current target dataset
            column_spec: Column specification
        
        Returns:
            Series with mapped values
        """
        derivation = column_spec.get("derivation", {})
        source_str = derivation.get("source")
        
        if not source_str:
            raise ValueError(f"No source specified for {column_spec.get('name')}")
        
        # Get source dataset and column
        source_df, source_col = self.get_source_dataset(source_str, source_data, target_df)
        
        # Apply filter if specified
        filter_expr = derivation.get("filter", "")
        if filter_expr:
            source_df = self.apply_filter(source_df, filter_expr, source_data)
        
        # Get the source values
        if source_col not in source_df.columns:
            raise ValueError(f"Column {source_col} not found in source dataset")
        
        # Determine join keys - try to find common key columns
        if target_df.height > 0:
            # Find common columns that could be used as keys
            common_cols = set(source_df.columns) & set(target_df.columns)
            # Prefer USUBJID if available, otherwise use any common columns
            join_keys = []
            if "USUBJID" in common_cols:
                join_keys = ["USUBJID"]
            elif common_cols:
                # Use other common columns as join keys
                join_keys = list(common_cols)
                self.logger.info(f"Joining on columns: {join_keys}")
            
            if join_keys:
                # Perform left join to maintain target dataset structure
                cols_to_select = join_keys + [source_col]
                # Remove duplicates from cols_to_select
                cols_to_select = list(dict.fromkeys(cols_to_select))
                
                merge_df = target_df.select(join_keys).join(
                    source_df.select(cols_to_select),
                    on=join_keys,
                    how="left"
                )
                result = merge_df[source_col]
            else:
                # No common keys - fall back to position-based mapping
                self.logger.warning("No common key columns found for joining, using position-based mapping")
                if source_df.height != target_df.height:
                    self.logger.warning(f"Source and target have different lengths, using first {target_df.height} records")
                    result = source_df[source_col].head(target_df.height)
                else:
                    result = source_df[source_col]
        else:
            # If target is empty, just get the column
            result = source_df[source_col]
        
        # Apply value mapping if specified
        mapping = derivation.get("mapping")
        if mapping:
            self.logger.info(f"Applying value mapping: {mapping}")
            # Apply mapping directly to series
            if isinstance(result, pl.Series):
                # Create a mapping dictionary for replace
                # Handle None values specially
                clean_mapping = {}
                for old_val, new_val in mapping.items():
                    if old_val != "":
                        clean_mapping[old_val] = new_val
                
                # Apply the mapping
                result = result.replace(clean_mapping, default=None if "" in mapping and mapping[""] is None else pl.first())
        
        return result