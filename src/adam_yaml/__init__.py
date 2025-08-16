"""
ADaM YAML Specification Module - Single Class Implementation

A minimal, easy-to-maintain module for handling hierarchical YAML specifications
for ADaM (Analysis Data Model) datasets following CDISC standards.
"""

from .adam_spec import AdamSpec, Column

__version__ = "2.0.0"
__author__ = "ADaM YAML Team"

__all__ = [
    "AdamSpec",
    "Column"
]