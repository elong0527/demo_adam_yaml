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
        
        # Get source dataset and column
        source_df, source_col = self.get_source_dataset(source_str, source_data, target_df)
        
        # Make a copy to avoid modifying original
        source_df = source_df.clone()
        
        # Apply filter if specified
        filter_expr = derivation.get("filter", "")
        if filter_expr:
            logging.getLogger(__name__).debug(f"Applying filter: {filter_expr}")
            logging.getLogger(__name__).debug(f"Before filter: {source_df.height} records")
            source_df = self.apply_filter(source_df, filter_expr, source_data)
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
        
        # Ensure we have USUBJID for grouping
        if "USUBJID" not in source_df.columns:
            raise ValueError("USUBJID required for aggregation")
        
        # Get unique subjects from target or source
        if target_df.height > 0 and "USUBJID" in target_df.columns:
            subjects_df = target_df.select("USUBJID").unique()
        elif "DM" in source_data:
            subjects_df = source_data["DM"].select("USUBJID").unique()
        else:
            subjects_df = source_df.select("USUBJID").unique()
        
        # Apply aggregation function
        if agg_function == "first":
            agg_df = source_df.group_by("USUBJID").agg(
                pl.col(source_col).first().alias(source_col)
            )
        elif agg_function == "last":
            agg_df = source_df.group_by("USUBJID").agg(
                pl.col(source_col).last().alias(source_col)
            )
        elif agg_function == "mean":
            agg_df = source_df.group_by("USUBJID").agg(
                pl.col(source_col).mean().alias(source_col)
            )
        elif agg_function == "median":
            agg_df = source_df.group_by("USUBJID").agg(
                pl.col(source_col).median().alias(source_col)
            )
        elif agg_function == "min":
            agg_df = source_df.group_by("USUBJID").agg(
                pl.col(source_col).min().alias(source_col)
            )
        elif agg_function == "max":
            agg_df = source_df.group_by("USUBJID").agg(
                pl.col(source_col).max().alias(source_col)
            )
        elif agg_function == "sum":
            agg_df = source_df.group_by("USUBJID").agg(
                pl.col(source_col).sum().alias(source_col)
            )
        elif agg_function == "count":
            agg_df = source_df.group_by("USUBJID").agg(
                pl.count().alias(source_col)
            )
        elif agg_function == "closest":
            if not target_var:
                raise ValueError("Target variable required for 'closest' aggregation")
            
            # Get target date/value
            target_df_local, target_col = self.get_source_dataset(target_var, source_data, target_df)
            
            # Find date column in source
            date_cols = [c for c in source_df.columns if "DTC" in c]
            if not date_cols:
                # If no date column, use first value
                agg_df = source_df.group_by("USUBJID").agg(
                    pl.col(source_col).first().alias(source_col)
                )
            else:
                date_col = date_cols[0]
                
                # Join target dates
                if "USUBJID" in target_df_local.columns and target_col in target_df_local.columns:
                    source_with_target = source_df.join(
                        target_df_local.select(["USUBJID", target_col]).unique(),
                        on="USUBJID",
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
                    agg_df = source_with_target.group_by("USUBJID").agg(
                        pl.col(source_col).sort_by("_diff").first().alias(source_col)
                    )
                else:
                    # Fallback to first value
                    agg_df = source_df.group_by("USUBJID").agg(
                        pl.col(source_col).first().alias(source_col)
                    )
        else:
            raise ValueError(f"Unknown aggregation function: {agg_function}")
        
        # Join with subjects to ensure all subjects are included
        result_df = subjects_df.join(agg_df, on="USUBJID", how="left")
        
        # Get the result series in the correct order
        if target_df.height > 0 and "USUBJID" in target_df.columns:
            # Join back to target to maintain order
            final_df = target_df.select("USUBJID").join(
                result_df,
                on="USUBJID",
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