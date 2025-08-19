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
    
    def find_column(self, 
                   column_name: str, 
                   source_data: dict[str, pl.DataFrame],
                   target_df: pl.DataFrame) -> pl.DataFrame:
        """
        Find which DataFrame contains the specified column.
        
        Args:
            column_name: Column name to find (e.g., "DM.AGE" or "AGE")
            source_data: Dictionary of source datasets with renamed columns
            target_df: Target dataset being built
        
        Returns:
            DataFrame containing the column
        """
        # First check target DataFrame
        if column_name in target_df.columns:
            return target_df
            
        # Then check source datasets
        for dataset_name, df in source_data.items():
            if column_name in df.columns:
                return df
        
        # Column not found
        raise ValueError(f"Column {column_name} not found in any dataset")
    
    def apply_filter(self, 
                    df: pl.DataFrame,
                    filter_expr: str,
                    source_data: dict[str, pl.DataFrame],
                    target_df: pl.DataFrame,
                    key_vars: list[str] | None = None) -> pl.DataFrame:
        """
        Apply SQL-like filter expression to dataframe.
        
        Args:
            df: DataFrame to filter (primary data)
            filter_expr: SQL-like filter expression (e.g., "DM.AGE >= 65 AND VS.WEIGHT > 80")
            source_data: Dictionary of all source datasets
            target_df: Current target DataFrame being built
            key_vars: Key variables to use for joins (default: ["USUBJID"])
        
        Returns:
            Filtered DataFrame
        """
        if not filter_expr:
            return df
        
        try:
            # Use default key variables if not provided
            if key_vars is None:
                key_vars = ["USUBJID"]  # Default, but should be provided
            
            # Determine which datasets are referenced in the filter
            referenced_datasets = set()
            for dataset_name in source_data.keys():
                if f"{dataset_name}." in filter_expr:
                    referenced_datasets.add(dataset_name)
            
            # Also check for references to target columns
            needs_target = any(col in filter_expr for col in target_df.columns if col in filter_expr)
            
            # Create SQL context with all necessary datasets
            ctx = pl.SQLContext()
            
            # Register the main DataFrame
            ctx.register("main", df)
            
            # Register referenced source datasets
            for dataset_name in referenced_datasets:
                ctx.register(dataset_name.lower(), source_data[dataset_name])
            
            # Register target if needed
            if needs_target and target_df.height > 0:
                ctx.register("target", target_df)
            
            # Build SQL query
            # Need to handle column names with dots by adding backticks
            filter_expr_sql = filter_expr
            
            # Replace column references with backticks for SQL
            import re
            # Find all column references like DM.AGE or VS.WEIGHT
            pattern = r'([A-Z]+\.[A-Z_]+)'
            filter_expr_sql = re.sub(pattern, r'`\1`', filter_expr_sql)
            
            # Build the query with necessary joins
            if referenced_datasets:
                # Build join clauses using key variables
                join_clauses = []
                for dataset_name in referenced_datasets:
                    # Find common key columns between datasets
                    main_keys = [k for k in key_vars if k in df.columns]
                    dataset_keys = [k for k in key_vars if k in source_data[dataset_name].columns]
                    common_keys = list(set(main_keys) & set(dataset_keys))
                    
                    if common_keys:
                        # Build join condition
                        join_conditions = []
                        for key in common_keys:
                            join_conditions.append(f"main.{key} = {dataset_name.lower()}.{key}")
                        join_clause = " AND ".join(join_conditions)
                        join_clauses.append(f"LEFT JOIN {dataset_name.lower()} ON {join_clause}")
                
                joins = " ".join(join_clauses)
                sql_query = f"SELECT DISTINCT main.* FROM main {joins} WHERE {filter_expr_sql}"
            else:
                # Simple filter on main DataFrame only
                sql_query = f"SELECT * FROM main WHERE {filter_expr_sql}"
            
            # Execute query
            result = ctx.execute(sql_query).collect()
            
            return result
            
        except Exception as e:
            logging.getLogger(__name__).warning(f"Filter failed: {e}, returning unfiltered data")
            return df


