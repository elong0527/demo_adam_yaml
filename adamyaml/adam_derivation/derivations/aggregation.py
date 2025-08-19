"""
Aggregation-based derivation for summarizing multiple records
"""

from typing import Any
import polars as pl
import logging
from .base import BaseDerivation


class AggregationDerivation(BaseDerivation):
    """Derive values using aggregation functions"""
    
    def derive(self, 
                source_data: dict[str, pl.DataFrame],
                target_df: pl.DataFrame,
                column_spec: dict[str, Any]) -> pl.DataFrame:
        """
        Apply aggregation to derive values
        
        Args:
            source_data: Dictionary of source datasets
            target_df: Current target dataset
            column_spec: Column specification
        
        Returns:
            Series with aggregated values
        """
        derivation = column_spec.get("derivation", {})
        source_str = derivation.get("source")
        
        if not source_str:
            raise ValueError(f"No source specified for {column_spec.get('name')}")
        
        # Find the DataFrame containing the source column
        source_df = self.find_column(source_str, source_data, target_df)
        source_col = source_str  # Column name is the same as source_str with renamed columns
        
        # Make a copy to avoid modifying original
        source_df = source_df.clone()
        
        # Apply filter if specified
        filter_expr = derivation.get("filter", "")
        if filter_expr:
            logging.getLogger(__name__).debug(f"Applying filter: {filter_expr}")
            logging.getLogger(__name__).debug(f"Before filter: {source_df.height} records")
            key_vars = column_spec.get('_key_vars', ["USUBJID"])
            source_df = self.apply_filter(source_df, filter_expr, source_data, target_df, key_vars)
            logging.getLogger(__name__).debug(f"After filter: {source_df.height} records")
        
        # Convert source column to numeric if it's string
        if source_col in source_df.columns and source_df[source_col].dtype == pl.Utf8:
            source_df = source_df.with_columns(
                pl.col(source_col).cast(pl.Float64, strict=False).alias(source_col)
            )
        
        # Get aggregation settings
        agg_config = derivation.get("aggregation", {})
        agg_function = agg_config.get("function", "first")
        target_var = agg_config.get("target")
        
        # Get key variables for grouping
        key_vars = column_spec.get('_key_vars', ["USUBJID"])
        # Find which key variables are actually in the source dataset
        # (not all datasets have all keys - e.g., VS may not have SUBJID)
        available_keys = [k for k in key_vars if k in source_df.columns]
        if not available_keys:
            raise ValueError(f"No key variables from {key_vars} found in source dataset")
        
        # Get unique subjects from target or source
        # Use available_keys since not all datasets have all keys
        if target_df.height > 0:
            target_keys = [k for k in available_keys if k in target_df.columns]
            if target_keys:
                subjects_df = target_df.select(target_keys).unique()
            else:
                subjects_df = source_df.select(available_keys).unique()
        elif "DM" in source_data:
            dm_keys = [k for k in available_keys if k in source_data["DM"].columns]
            if dm_keys:
                subjects_df = source_data["DM"].select(dm_keys).unique()
            else:
                subjects_df = source_df.select(available_keys).unique()
        else:
            subjects_df = source_df.select(available_keys).unique()
        
        # Apply aggregation function using available keys
        if agg_function == "first":
            agg_df = source_df.group_by(available_keys).agg(
                pl.col(source_col).first().alias(source_col)
            )
        elif agg_function == "last":
            agg_df = source_df.group_by(available_keys).agg(
                pl.col(source_col).last().alias(source_col)
            )
        elif agg_function == "mean":
            agg_df = source_df.group_by(available_keys).agg(
                pl.col(source_col).mean().alias(source_col)
            )
        elif agg_function == "median":
            agg_df = source_df.group_by(available_keys).agg(
                pl.col(source_col).median().alias(source_col)
            )
        elif agg_function == "min":
            agg_df = source_df.group_by(available_keys).agg(
                pl.col(source_col).min().alias(source_col)
            )
        elif agg_function == "max":
            agg_df = source_df.group_by(available_keys).agg(
                pl.col(source_col).max().alias(source_col)
            )
        elif agg_function == "sum":
            agg_df = source_df.group_by(available_keys).agg(
                pl.col(source_col).sum().alias(source_col)
            )
        elif agg_function == "count":
            agg_df = source_df.group_by(available_keys).agg(
                pl.count().alias(source_col)
            )
        elif agg_function == "closest":
            if not target_var:
                raise ValueError("Target variable required for 'closest' aggregation")
            
            # Get target date/value
            target_df_local = self.find_column(target_var, source_data, target_df)
            target_col = target_var  # Column name is the same with renamed columns
            
            # Find date column in source
            date_cols = [c for c in source_df.columns if "DTC" in c]
            if not date_cols:
                # If no date column, use first value
                agg_df = source_df.group_by("USUBJID").agg(
                    pl.col(source_col).first().alias(source_col)
                )
            else:
                date_col = date_cols[0]
                
                # Join target dates using available keys
                join_keys = [k for k in available_keys if k in target_df_local.columns]
                if join_keys and target_col in target_df_local.columns:
                    source_with_target = source_df.join(
                        target_df_local.select(join_keys + [target_col]).unique(),
                        on=join_keys,
                        how="left"
                    )
                    
                    # Convert dates to datetime for comparison
                    source_with_target = source_with_target.with_columns([
                        pl.col(date_col).str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("_date"),
                        pl.col(target_col).str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("_target")
                    ])
                    
                    # Calculate difference and find closest
                    source_with_target = source_with_target.with_columns(
                        (pl.col("_target") - pl.col("_date")).abs().alias("_diff")
                    )
                    
                    # Get the row with minimum difference per subject
                    agg_df = source_with_target.group_by(available_keys).agg(
                        pl.col(source_col).sort_by("_diff").first().alias(source_col)
                    )
                else:
                    # Fallback to first value
                    agg_df = source_df.group_by(available_keys).agg(
                        pl.col(source_col).first().alias(source_col)
                    )
        else:
            raise ValueError(f"Unknown aggregation function: {agg_function}")
        
        # Join with subjects to ensure all subjects are included
        # Use available_keys for joining
        result_df = subjects_df.join(agg_df, on=available_keys, how="left")
        
        # Get the result series in the correct order
        if target_df.height > 0 and target_keys:
            # Join back to target to maintain order using available keys
            final_df = target_df.select(target_keys).join(
                result_df,
                on=target_keys,
                how="left"
            )
            result = final_df[source_col]
        else:
            result = result_df[source_col]
        
        non_null = result.drop_nulls().len()
        logging.getLogger(__name__).info(f"Applied {agg_function} aggregation, {non_null} non-null values")
        
        # Add to dataframe
        col_name = column_spec["name"]
        if target_df.height == 0:
            return pl.DataFrame({col_name: result})
        else:
            return target_df.with_columns(result.alias(col_name))