"""
Base derivation class and factory
"""

from abc import ABC, abstractmethod
from typing import Any
import polars as pl
import logging


class BaseDerivation(ABC):
    """Abstract base class for all derivation methods"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def derive(self, 
               source_data: dict[str, pl.DataFrame],
               target_df: pl.DataFrame,
               column_spec: dict[str, Any]) -> pl.Series:
        """
        Execute derivation for a single column
        
        Args:
            source_data: Dictionary of source SDTM datasets
            target_df: Current state of target ADaM dataset
            column_spec: Column specification from YAML
        
        Returns:
            Series with derived values
        """
        pass
    
    def get_source_dataset(self, 
                          source_str: str, 
                          source_data: dict[str, pl.DataFrame],
                          target_df: pl.DataFrame) -> tuple[pl.DataFrame, str]:
        """
        Parse source string and return dataset and column
        
        Args:
            source_str: Source string (e.g., "DM.AGE" or "AGE")
            source_data: Dictionary of source datasets
            target_df: Target dataset being built
        
        Returns:
            Tuple of (dataset DataFrame, column name)
        """
        if "." in source_str:
            dataset_name, column_name = source_str.split(".", 1)
            if dataset_name not in source_data:
                raise ValueError(f"Dataset {dataset_name} not loaded")
            return source_data[dataset_name], column_name
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
            self.logger.warning(f"Filter failed: {e}, returning unfiltered data")
            return df


class DerivationFactory:
    """Factory to create appropriate derivation class"""
    
    @staticmethod
    def get_derivation(column_spec: dict[str, Any]) -> BaseDerivation:
        """
        Get appropriate derivation class based on column specification
        
        Args:
            column_spec: Column specification from YAML
        
        Returns:
            Instance of appropriate derivation class
        """
        derivation = column_spec.get("derivation", {})
        
        # Check for conditional derivation first
        if "condition" in derivation:
            from .condition import ConditionalDerivation
            return ConditionalDerivation()
        
        # Check for constant
        if "constant" in derivation:
            from .constant import ConstantDerivation
            return ConstantDerivation()
        
        # Check for custom function
        if "function" in derivation:
            from .custom import CustomDerivation
            return CustomDerivation()
        
        # Check for categorization
        if "cut" in derivation:
            from .categorization import CategorizationDerivation
            return CategorizationDerivation()
        
        # Check for source with aggregation
        if "source" in derivation and "aggregation" in derivation:
            from .aggregation import AggregationDerivation
            return AggregationDerivation()
        
        # Check for source with mapping
        if "source" in derivation and "mapping" in derivation:
            from .source import SourceDerivation
            return SourceDerivation()
        
        # Default source derivation
        if "source" in derivation:
            from .source import SourceDerivation
            return SourceDerivation()
        
        raise ValueError(f"No suitable derivation method found for: {derivation}")