"""
Custom function derivation for complex calculations
"""

from typing import Any
import polars as pl
from .base import BaseDerivation
import importlib
import importlib.util
import logging
from pathlib import Path


class CustomDerivation(BaseDerivation):
    """Derive values using custom functions"""
    
    def derive(self, 
               source_data: dict[str, pl.DataFrame],
               target_df: pl.DataFrame,
               column_spec: dict[str, Any]) -> pl.DataFrame:
        """
        Apply custom function to derive values
        
        Args:
            source_data: Dictionary of source datasets
            target_df: Current target dataset
            column_spec: Column specification
        
        Returns:
            Series with derived values
        """
        derivation = column_spec.get("derivation", {})
        function_name = derivation.get("function")
        
        if not function_name:
            raise ValueError(f"No function specified for {column_spec.get('name')}")
        
        # Get the custom function
        func = self._get_custom_function(function_name)
        
        # Prepare arguments for the function
        kwargs = {}
        for key, value in derivation.items():
            if key != "function":
                # Get the column data
                if value in target_df.columns:
                    kwargs[key] = target_df[value]
                elif "." in value:
                    # Reference to source dataset
                    dataset_name, col_name = value.split(".", 1)
                    if dataset_name in source_data:
                        kwargs[key] = source_data[dataset_name][col_name]
                    else:
                        logging.getLogger(__name__).warning(f"Dataset {dataset_name} not found for argument {key}")
                else:
                    # Use as literal value
                    kwargs[key] = value
        
        # Call the custom function
        try:
            result = func(**kwargs)
            
            # Ensure result is a Series
            if not isinstance(result, pl.Series):
                result = pl.Series(result)
            
            logging.getLogger(__name__).info(f"Applied custom function {function_name}")
            
            # Add to dataframe
            col_name = column_spec["name"]
            if target_df.height == 0:
                return pl.DataFrame({col_name: result})
            else:
                return target_df.with_columns(result.alias(col_name))
            
        except Exception as e:
            logging.getLogger(__name__).error(f"Custom function {function_name} failed: {e}")
            # Return dataframe with None values
            col_name = column_spec["name"]
            n_rows = target_df.height if target_df.height > 0 else source_data.get("DM", pl.DataFrame()).height or 1
            null_series = pl.Series([None] * n_rows)
            
            if target_df.height == 0:
                return pl.DataFrame({col_name: null_series})
            else:
                return target_df.with_columns(null_series.alias(col_name))
    
    def _get_custom_function(self, function_name: str):
        """Load custom function."""
        # Built-in functions
        if function_name == "get_bmi":
            return self._builtin_bmi
        
        # Try to import from functions folder
        functions_path = Path(__file__).parent.parent / "functions" / f"{function_name}.py"
        if functions_path.exists():
            spec = importlib.util.spec_from_file_location(function_name, functions_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return getattr(module, function_name)
        
        raise ValueError(f"Function {function_name} not found")
    
    def _builtin_bmi(self, height: pl.Series, weight: pl.Series) -> pl.Series:
        """
        Built-in BMI calculation function using Polars
        
        Args:
            height: Height in cm
            weight: Weight in kg
        
        Returns:
            BMI values (kg/m^2)
        """
        # Convert height from cm to m
        height_m = height / 100
        
        # Calculate BMI
        bmi = weight / (height_m ** 2)
        
        # Round to 1 decimal place
        bmi = bmi.round(1)
        
        return bmi