"""
Conditional derivation with when/then/else logic
"""

from typing import Any
import polars as pl
from .base import BaseDerivation, DerivationFactory


class ConditionalDerivation(BaseDerivation):
    """Derive values using conditional logic"""
    
    def derive(self, 
               source_data: dict[str, pl.DataFrame],
               target_df: pl.DataFrame,
               column_spec: dict[str, Any]) -> pl.Series:
        """
        Apply conditional derivation rules
        
        Args:
            source_data: Dictionary of source datasets
            target_df: Current target dataset
            column_spec: Column specification
        
        Returns:
            Series with conditionally derived values
        """
        derivation = column_spec.get("derivation", {})
        conditions = derivation.get("condition", [])
        
        if not conditions:
            raise ValueError(f"No conditions specified for {column_spec.get('name')}")
        
        # Determine result length
        if target_df.height > 0:
            n_rows = target_df.height
        elif "DM" in source_data:
            n_rows = source_data["DM"].height
        else:
            raise ValueError("Cannot determine number of rows for conditional derivation")
        
        # Initialize result with None
        result = pl.Series([None] * n_rows)
        
        # Build conditional expression
        expr = None
        
        # Process each condition in reverse order (so first conditions take precedence)
        for cond_spec in reversed(conditions):
            when_expr = cond_spec.get("when")
            then_spec = cond_spec.get("then")
            else_spec = cond_spec.get("else")
            
            if when_expr:
                # Evaluate the when condition
                mask_expr = self._build_condition_expr(when_expr, source_data, target_df)
                
                if then_spec:
                    # Get the value for the then branch
                    then_value = self._get_derivation_value(then_spec)
                    
                    if expr is None:
                        expr = pl.when(mask_expr).then(then_value)
                    else:
                        expr = pl.when(mask_expr).then(then_value).otherwise(expr)
            
            elif else_spec:
                # This is the else clause
                else_value = self._get_derivation_value(else_spec)
                if expr is None:
                    expr = pl.lit(else_value)
                else:
                    expr = expr.otherwise(else_value)
        
        # Apply the expression
        if expr is not None:
            if target_df.height > 0:
                # Create a temporary dataframe with necessary columns
                temp_df = target_df
                
                # Add columns from source data if needed
                for dataset_name, dataset in source_data.items():
                    if dataset_name in ["DM"] and "ARMCD" in dataset.columns:
                        if "USUBJID" in temp_df.columns and "USUBJID" in dataset.columns:
                            # Join to get ARMCD
                            temp_df = temp_df.join(
                                dataset.select(["USUBJID", "ARMCD"]).unique(),
                                on="USUBJID",
                                how="left"
                            )
                
                # Apply expression
                result_df = temp_df.select(expr.alias("result"))
                result = result_df["result"]
            else:
                # Use DM dataset if target is empty
                if "DM" in source_data:
                    temp_df = source_data["DM"]
                    result_df = temp_df.select(expr.alias("result"))
                    result = result_df["result"]
        
        assigned_count = result.drop_nulls().len()
        self.logger.info(f"Applied {len(conditions)} conditions, {assigned_count} rows assigned")
        
        return result
    
    def _build_condition_expr(self, expr_str: str, source_data: dict[str, pl.DataFrame], target_df: pl.DataFrame):
        """
        Build a Polars expression from condition string
        
        Args:
            expr_str: Expression string (e.g., "DM.ARMCD == 'PBO'")
            source_data: Dictionary of source datasets
            target_df: Current target dataset
        
        Returns:
            Polars expression
        """
        # Handle null checks
        if "is not null" in expr_str:
            col_ref = expr_str.replace("is not null", "").strip()
            if "." in col_ref:
                _, col_name = col_ref.split(".", 1)
            else:
                col_name = col_ref
            return pl.col(col_name).is_not_null()
        
        if "is null" in expr_str:
            col_ref = expr_str.replace("is null", "").strip()
            if "." in col_ref:
                _, col_name = col_ref.split(".", 1)
            else:
                col_name = col_ref
            return pl.col(col_name).is_null()
        
        # Handle equality
        if "==" in expr_str:
            parts = expr_str.split("==")
            col_ref = parts[0].strip()
            value = parts[1].strip().strip("'").strip('"')
            
            if "." in col_ref:
                _, col_name = col_ref.split(".", 1)
            else:
                col_name = col_ref
            
            return pl.col(col_name) == value
        
        if "!=" in expr_str:
            parts = expr_str.split("!=")
            col_ref = parts[0].strip()
            value = parts[1].strip().strip("'").strip('"')
            
            if "." in col_ref:
                _, col_name = col_ref.split(".", 1)
            else:
                col_name = col_ref
            
            return pl.col(col_name) != value
        
        # Default to false
        return pl.lit(False)
    
    def _get_derivation_value(self, spec: dict[str, Any]):
        """
        Get the value from a derivation specification
        
        Args:
            spec: Derivation specification (e.g., {"constant": "Y"})
        
        Returns:
            Value or expression
        """
        if "constant" in spec:
            return pl.lit(spec["constant"])
        
        # For other derivation types, return a placeholder
        # In a full implementation, we'd handle other derivation types
        return pl.lit(None)