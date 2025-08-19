"""Custom function derivation for complex calculations."""

from typing import Any
import polars as pl
import logging
from .base import BaseDerivation

logger = logging.getLogger(__name__)


class FunctionDerivation(BaseDerivation):
    """
    Handles custom function-based derivations.
    Used for complex calculations that can't be expressed as SQL.
    """
    
    def derive(self, 
               col_spec: dict[str, Any], 
               source_data: dict[str, pl.DataFrame],
               target_df: pl.DataFrame) -> pl.Series:
        """Derive column using custom function."""
        
        derivation = col_spec.get("derivation", {})
        function_name = derivation.get("function")
        
        if not function_name:
            raise ValueError("Function derivation requires 'function' field")
        
        # Get function arguments from specification
        args = {}
        for key, value in derivation.items():
            if key != "function":
                # Value could be a column name or a literal
                if isinstance(value, str) and value in target_df.columns:
                    args[key] = target_df[value]
                else:
                    args[key] = value
        
        # Execute the function
        try:
            result = self._execute_function(function_name, args, target_df)
            logger.info(f"Applied custom function {function_name}")
            return result
        except Exception as e:
            logger.error(f"Function {function_name} failed: {e}")
            return pl.Series([None] * target_df.height)
    
    def _execute_function(self, 
                         function_name: str, 
                         args: dict[str, Any],
                         target_df: pl.DataFrame) -> pl.Series:
        """Execute a custom function."""
        
        # Map of available functions
        functions = {
            "get_bmi": self._calculate_bmi,
            # Add more custom functions here as needed
        }
        
        if function_name not in functions:
            raise ValueError(f"Unknown function: {function_name}")
        
        return functions[function_name](args, target_df)
    
    def _calculate_bmi(self, args: dict[str, Any], target_df: pl.DataFrame) -> pl.Series:
        """Calculate BMI from height and weight."""
        
        height = args.get("height")
        weight = args.get("weight")
        
        if height is None or weight is None:
            raise ValueError("BMI calculation requires 'height' and 'weight' parameters")
        
        # Convert to Series if needed
        if not isinstance(height, pl.Series):
            height = pl.Series([height] * target_df.height)
        if not isinstance(weight, pl.Series):
            weight = pl.Series([weight] * target_df.height)
        
        # Calculate BMI: weight (kg) / (height (cm) / 100)^2
        # Handle nulls gracefully
        bmi = weight / ((height / 100) ** 2)
        
        # Round to 1 decimal place
        bmi = bmi.round(1)
        
        return bmi