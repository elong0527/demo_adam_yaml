#!/usr/bin/env python3
"""
Example usage of the ADaM YAML Handler module
"""

import sys
from pathlib import Path

# Add src to path for development
sys.path.insert(0, 'src')

from adam_yaml import AdamSpec

# create spec without schema validation 
adsl_spec = AdamSpec("spec/adsl_study.yaml")

# create spec with schema validation
adsl_spec = AdamSpec("spec/adsl_study.yaml", schema_path="spec/schema.yaml")

# store the combined YAML file
adsl_spec.save("spec/adsl_study_combined.yaml")
