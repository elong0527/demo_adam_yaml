"""
Base derivation class and factory
"""

from abc import ABC, abstractmethod
from typing import Any
import polars as pl
import logging


class BaseDerivation(ABC):
    """Base class for all derivation methods"""
    
    @abstractmethod
    def derive(self, 
               source_data: dict[str, pl.DataFrame],
               target_df: pl.DataFrame,
               column_spec: dict[str, Any]) -> pl.DataFrame:
        """
        Derive column and return updated dataframe.
        
        Args:
            source_data: Source datasets with renamed columns
            target_df: Current target dataset
            column_spec: Column specification
        
        Returns:
            Updated dataframe with new column
        """
        pass
    
    def get_source_dataset(self, 
                          source_str: str, 
                          source_data: dict[str, pl.DataFrame],
                          target_df: pl.DataFrame) -> tuple[pl.DataFrame, str]:
        """
        Parse source string and return dataset and column.
        With renamed columns, the column name is already in DATASET.COLUMN format.
        
        Args:
            source_str: Source string (e.g., "DM.AGE" or "AGE")
            source_data: Dictionary of source datasets (with renamed columns)
            target_df: Target dataset being built
        
        Returns:
            Tuple of (dataset DataFrame, column name as it appears in the dataframe)
        """
        if "." in source_str:
            dataset_name, _ = source_str.split(".", 1)
            if dataset_name not in source_data:
                raise ValueError(f"Dataset {dataset_name} not loaded")
            # Return the full source_str as the column name since columns are already renamed
            return source_data[dataset_name], source_str
        else:
            # Reference to column in target dataset
            return target_df, source_str
    
    def apply_filter(self, 
                    df: pl.DataFrame,
                    filter_expr: str,
                    source_data: dict[str, pl.DataFrame]) -> pl.DataFrame:
        """
        Apply filter expression to dataframe using Polars
        
        Args:
            df: DataFrame to filter
            filter_expr: Filter expression
            source_data: Dictionary of available datasets
        
        Returns:
            Filtered DataFrame
        """
        if not filter_expr:
            return df
        
        try:
            # Split by & for multiple conditions
            conditions = filter_expr.split(" & ")
            result = df
            
            for cond in conditions:
                cond = cond.strip()
                
                # Handle equality checks
                if "==" in cond:
                    parts = cond.split("==")
                    col_ref = parts[0].strip()
                    value = parts[1].strip().strip('"').strip("'")
                    
                    # Extract column name from reference
                    if "." in col_ref:
                        dataset_name, col_name = col_ref.split(".", 1)
                        # Check if it matches current dataframe's dataset
                        if col_name in df.columns:
                            result = result.filter(pl.col(col_name) == value)
                    else:
                        col_name = col_ref
                        if col_name in df.columns:
                            result = result.filter(pl.col(col_name) == value)
                
                # Handle less than comparisons
                elif "<" in cond and "=" not in cond:
                    parts = cond.split("<")
                    col_ref = parts[0].strip()
                    compare_ref = parts[1].strip()
                    
                    # Extract column names
                    if "." in col_ref:
                        _, col_name = col_ref.split(".", 1)
                    else:
                        col_name = col_ref
                    
                    # Handle comparison with another column reference
                    if "." in compare_ref:
                        dataset_name, compare_col = compare_ref.split(".", 1)
                        if dataset_name in source_data and "USUBJID" in df.columns:
                            # Get comparison values from source dataset
                            compare_df = source_data[dataset_name]
                            if "USUBJID" in compare_df.columns and compare_col in compare_df.columns:
                                # Join to get comparison values
                                merged = result.join(
                                    compare_df.select(["USUBJID", compare_col]).unique(),
                                    on="USUBJID",
                                    how="left"
                                )
                                if col_name in merged.columns:
                                    result = merged.filter(pl.col(col_name) < pl.col(compare_col))
                                    # Drop the joined column
                                    if compare_col in result.columns and compare_col not in df.columns:
                                        result = result.drop(compare_col)
                    else:
                        # Direct value comparison
                        if col_name in df.columns:
                            result = result.filter(pl.col(col_name) < compare_ref)
            
            return result
            
        except Exception as e:
            logging.getLogger(__name__).warning(f"Filter failed: {e}, returning unfiltered data")
            return df


class DerivationFactory:
    """Factory to create appropriate derivation class"""
    
    @staticmethod
    def get_derivation(column_spec: dict[str, Any]) -> BaseDerivation:
        """Get derivation class based on specification."""
        derivation = column_spec.get("derivation", {})
        
        # Simple dispatch based on derivation keys
        if "constant" in derivation:
            from .constant import ConstantDerivation
            return ConstantDerivation()
        elif "function" in derivation:
            from .custom import CustomDerivation
            return CustomDerivation()
        elif "aggregation" in derivation:
            from .aggregation import AggregationDerivation
            return AggregationDerivation()
        elif "source" in derivation:
            from .source import SourceDerivation
            return SourceDerivation()
        else:
            raise ValueError(f"Unknown derivation type: {derivation}")