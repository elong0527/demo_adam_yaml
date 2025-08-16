"""
ADaM YAML Specification Handler - Simplified Module

A minimal, easy-to-maintain module for handling hierarchical YAML specifications
for ADaM (Analysis Data Model) datasets following CDISC standards.
"""

from .adam_yaml import (
    ADaMYAMLHandler,
    ADaMSpec,
    Column
)

__version__ = "0.2.1"
__author__ = "ADaM YAML Team"

__all__ = [
    "ADaMYAMLHandler",
    "ADaMSpec", 
    "Column"
]