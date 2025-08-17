"""
Custom function derivation for complex calculations
"""

from typing import Any
import polars as pl
from .base import BaseDerivation
import importlib
import sys
from pathlib import Path


class CustomDerivation(BaseDerivation):
    """Derive values using custom functions"""
    
    def __init__(self):
        super().__init__()
        self._function_cache = {}
    
    def derive(self, 
               source_data: dict[str, pl.DataFrame],
               target_df: pl.DataFrame,
               column_spec: dict[str, Any]) -> pl.Series:
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
                        self.logger.warning(f"Dataset {dataset_name} not found for argument {key}")
                else:
                    # Use as literal value
                    kwargs[key] = value
        
        # Call the custom function
        try:
            result = func(**kwargs)
            
            # Ensure result is a Series
            if not isinstance(result, pl.Series):
                result = pl.Series(result)
            
            self.logger.info(f"Applied custom function {function_name}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Custom function {function_name} failed: {e}")
            # Return series of None values
            if target_df.height > 0:
                return pl.Series([None] * target_df.height)
            elif "DM" in source_data:
                return pl.Series([None] * source_data["DM"].height)
            else:
                return pl.Series([None])
    
    def _get_custom_function(self, function_name: str):
        """
        Load custom function from custom_functions module
        
        Args:
            function_name: Name of the function to load
        
        Returns:
            Function object
        """
        if function_name in self._function_cache:
            return self._function_cache[function_name]
        
        # Try to import from custom_functions module
        try:
            # Check if custom_functions.py exists in adam_derivation folder
            custom_functions_path = Path(__file__).parent.parent / "custom_functions.py"
            
            if custom_functions_path.exists():
                # Load the module
                spec = importlib.util.spec_from_file_location("custom_functions", custom_functions_path)
                custom_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(custom_module)
                
                # Get the function
                if hasattr(custom_module, function_name):
                    func = getattr(custom_module, function_name)
                    self._function_cache[function_name] = func
                    return func
            
            # If not in custom_functions, try built-in functions
            if function_name == "get_bmi":
                func = self._builtin_bmi
                self._function_cache[function_name] = func
                return func
            
            raise ValueError(f"Custom function {function_name} not found")
            
        except Exception as e:
            self.logger.error(f"Failed to load custom function {function_name}: {e}")
            # Return a dummy function that returns None
            return lambda **kwargs: None
    
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