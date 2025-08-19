"""Custom functions for ADaM derivations."""

import polars as pl


def get_bmi(weight, height):
    """
    Calculate BMI from weight and height.
    
    Args:
        weight: Weight in kg (Series or scalar)
        height: Height in cm (Series or scalar)
        
    Returns:
        BMI values as Series
    """
    # Ensure Series
    if not isinstance(weight, pl.Series):
        weight = pl.Series([weight])
    if not isinstance(height, pl.Series):
        height = pl.Series([height])
    
    # Calculate BMI: weight (kg) / (height (cm) / 100)^2
    bmi = weight / ((height / 100) ** 2)
    
    # Round to 1 decimal place
    return bmi.round(1)


def calculate_bsa(weight, height):
    """
    Calculate Body Surface Area using Mosteller formula.
    
    Args:
        weight: Weight in kg
        height: Height in cm
        
    Returns:
        BSA in m^2
    """
    if not isinstance(weight, pl.Series):
        weight = pl.Series([weight])
    if not isinstance(height, pl.Series):
        height = pl.Series([height])
    
    # Mosteller formula: sqrt((height * weight) / 3600)
    bsa = ((height * weight) / 3600) ** 0.5
    
    return bsa.round(2)


def categorize_age(age):
    """
    Categorize age into groups.
    
    Args:
        age: Age values
        
    Returns:
        Age categories
    """
    if not isinstance(age, pl.Series):
        age = pl.Series([age])
    
    return pl.when(age < 18).then("Pediatric").when(age < 65).then("Adult").otherwise("Elderly")