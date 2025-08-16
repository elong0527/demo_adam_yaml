"""
Validation functionality for ADaM specifications
"""

from typing import List
from .models import ADaMSpec, DataType, CoreType


class SpecValidator:
    """Validates ADaM specifications against defined rules"""
    
    @staticmethod
    def validate(spec: ADaMSpec) -> List[str]:
        """
        Validate specification against rules
        Returns list of validation errors
        """
        errors = []
        
        # Check required fields
        if not spec.domain:
            errors.append("Domain is required")
        
        if not spec.key:
            errors.append("Key variables must be specified")
        
        # Validate columns
        column_names = set()
        for col in spec.columns:
            # Check for duplicate column names
            if col.name in column_names:
                errors.append(f"Duplicate column name: {col.name}")
            column_names.add(col.name)
            
            # Validate data type
            if col.type and col.type not in [t.value for t in DataType]:
                errors.append(f"Invalid data type for {col.name}: {col.type}")
            
            # Validate core type
            if col.core and col.core not in [c.value for c in CoreType]:
                errors.append(f"Invalid core type for {col.name}: {col.core}")
            
            # Validate validation rules
            if col.validation:
                errors.extend(SpecValidator._validate_column_rules(col))
        
        # Check that key variables exist in columns
        for key_var in spec.key:
            if key_var not in column_names:
                errors.append(f"Key variable {key_var} not found in columns")
        
        return errors
    
    @staticmethod
    def _validate_column_rules(col) -> List[str]:
        """Validate column-specific validation rules"""
        errors = []
        
        if col.validation.maximum_missing_percentage is not None:
            if not 0 <= col.validation.maximum_missing_percentage <= 100:
                errors.append(f"Invalid missing percentage for {col.name}: must be between 0 and 100")
        
        if col.validation.min is not None and col.validation.max is not None:
            if col.validation.min > col.validation.max:
                errors.append(f"Invalid range for {col.name}: min ({col.validation.min}) > max ({col.validation.max})")
        
        if col.validation.length:
            if 'min' in col.validation.length and 'max' in col.validation.length:
                if col.validation.length['min'] > col.validation.length['max']:
                    errors.append(f"Invalid length range for {col.name}: min > max")
        
        return errors