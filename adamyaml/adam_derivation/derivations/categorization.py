"""
Categorization derivation using cut-based rules
"""

from typing import Any
import polars as pl
import logging
from .base import BaseDerivation


class CategorizationDerivation(BaseDerivation):
    """Derive categorical values using cut rules"""
    
    def derive(self, 
               source_data: dict[str, pl.DataFrame],
               target_df: pl.DataFrame,
               column_spec: dict[str, Any]) -> pl.DataFrame:
        """
        Apply categorization rules to create categorical variable
        
        Args:
            source_data: Dictionary of source datasets
            target_df: Current target dataset
            column_spec: Column specification
        
        Returns:
            Updated dataframe with categorical column
        """
        derivation = column_spec.get("derivation", {})
        source_str = derivation.get("source")
        cut_rules = derivation.get("cut", {})
        
        if not source_str:
            raise ValueError(f"No source specified for {column_spec.get('name')}")
        
        if not cut_rules:
            raise ValueError(f"No cut rules specified for {column_spec.get('name')}")
        
        # Find the DataFrame containing the source column
        source_df = self.find_column(source_str, source_data, target_df)
        source_col = source_str  # Column name is the same with renamed columns
        
        if source_col not in source_df.columns:
            raise ValueError(f"Column {source_col} not found in source dataset")
        
        # Start with the source column
        result_expr = pl.col(source_col)
        
        # Apply each cut rule using when/then chains
        for condition, label in cut_rules.items():
            # Parse condition and build expression
            if "<" in condition and ">=" not in condition and "<=" not in condition:
                # Simple less than
                if condition.startswith("<"):
                    threshold = float(condition[1:].strip())
                    result_expr = pl.when(pl.col(source_col) < threshold).then(label).otherwise(result_expr)
                else:
                    # Handle compound conditions like ">=18 and <65"
                    if " and " in condition:
                        parts = condition.split(" and ")
                        expr = None
                        for part in parts:
                            part = part.strip()
                            if ">=" in part:
                                threshold = float(part.split(">=")[1].strip())
                                part_expr = pl.col(source_col) >= threshold
                            elif "<=" in part:
                                threshold = float(part.split("<=")[1].strip())
                                part_expr = pl.col(source_col) <= threshold
                            elif ">" in part:
                                threshold = float(part.split(">")[1].strip())
                                part_expr = pl.col(source_col) > threshold
                            elif "<" in part:
                                threshold = float(part.split("<")[1].strip())
                                part_expr = pl.col(source_col) < threshold
                            else:
                                continue
                            
                            expr = part_expr if expr is None else expr & part_expr
                        
                        if expr is not None:
                            result_expr = pl.when(expr).then(label).otherwise(result_expr)
                    else:
                        # Single condition
                        threshold = float(condition.split("<")[1].strip())
                        result_expr = pl.when(pl.col(source_col) < threshold).then(label).otherwise(result_expr)
            
            elif ">=" in condition:
                if " and " in condition:
                    # Compound condition
                    parts = condition.split(" and ")
                    expr = None
                    for part in parts:
                        part = part.strip()
                        if ">=" in part:
                            threshold = float(part.split(">=")[1].strip())
                            part_expr = pl.col(source_col) >= threshold
                        elif "<=" in part:
                            threshold = float(part.split("<=")[1].strip())
                            part_expr = pl.col(source_col) <= threshold
                        elif ">" in part:
                            threshold = float(part.split(">")[1].strip())
                            part_expr = pl.col(source_col) > threshold
                        elif "<" in part:
                            threshold = float(part.split("<")[1].strip())
                            part_expr = pl.col(source_col) < threshold
                        else:
                            continue
                        
                        expr = part_expr if expr is None else expr & part_expr
                    
                    if expr is not None:
                        result_expr = pl.when(expr).then(label).otherwise(result_expr)
                else:
                    # Simple greater than or equal
                    threshold = float(condition[2:].strip())
                    result_expr = pl.when(pl.col(source_col) >= threshold).then(label).otherwise(result_expr)
            
            elif "<=" in condition:
                threshold = float(condition[2:].strip())
                result_expr = pl.when(pl.col(source_col) <= threshold).then(label).otherwise(result_expr)
            
            elif ">" in condition:
                threshold = float(condition[1:].strip())
                result_expr = pl.when(pl.col(source_col) > threshold).then(label).otherwise(result_expr)
            
            elif "==" in condition:
                value = condition[2:].strip()
                try:
                    value = float(value)
                except:
                    pass
                result_expr = pl.when(pl.col(source_col) == value).then(label).otherwise(result_expr)
            
            elif "!=" in condition:
                value = condition[2:].strip()
                try:
                    value = float(value)
                except:
                    pass
                result_expr = pl.when(pl.col(source_col) != value).then(label).otherwise(result_expr)
        
        # Apply the expression to get the result
        if isinstance(source_df, pl.DataFrame):
            result_df = source_df.select(result_expr.alias("result"))
            result = result_df["result"]
        else:
            # If source_df is actually target_df, we need to handle it
            result_df = pl.DataFrame({source_col: source_df[source_col]})
            result_df = result_df.select(result_expr.alias("result"))
            result = result_df["result"]
        
        logging.getLogger(__name__).info(f"Applied {len(cut_rules)} categorization rules")
        
        # Add to dataframe
        col_name = column_spec["name"]
        if target_df.height == 0:
            return pl.DataFrame({col_name: result})
        else:
            return target_df.with_columns(result.alias(col_name))