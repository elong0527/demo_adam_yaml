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
        
        # For subject-level data, merge on USUBJID
        if "USUBJID" in source_df.columns and "USUBJID" in target_df.columns:
            # Join on USUBJID to align records
            merge_df = target_df.select(["USUBJID"]).join(
                source_df.select(["USUBJID", source_col]),
                on="USUBJID",
                how="left"
            )
            result = merge_df[source_col]
        elif target_df.height == 0:
            # If target is empty, just get the column
            result = source_df[source_col]
        else:
            # Direct mapping if same length
            if source_df.height != target_df.height:
                self.logger.warning(f"Source and target have different lengths, using first {target_df.height} records")
                result = source_df[source_col].head(target_df.height)
            else:
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