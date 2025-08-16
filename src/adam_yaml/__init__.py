"""
ADaM YAML Specification Handler Module

A hierarchical YAML specification system for defining ADaM (Analysis Data Model) 
datasets with inheritance, validation, and merging capabilities.
"""

from .handler import (
    ADaMYAMLHandler,
    ADaMSpec,
    Column,
    Derivation,
    ValidationRule,
    CoreType,
    DataType
)
from .merger import YAMLMerger
from .loader import IncludeLoader

__all__ = [
    "ADaMYAMLHandler",
    "ADaMSpec",
    "Column",
    "Derivation",
    "ValidationRule",
    "CoreType",
    "DataType",
    "YAMLMerger",
    "IncludeLoader",
]