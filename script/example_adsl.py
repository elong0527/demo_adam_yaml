#!/usr/bin/env python3
"""
Example usage of the ADaM YAML Handler module
"""

import sys
from pathlib import Path

# Add src to path for development
sys.path.insert(0, 'src')

from adam_yaml import ADaMYAMLHandler

handler = ADaMYAMLHandler(spec_dir=Path("spec"))
    
errors = handler.validate_spec(spec)
      
table_output = handler.display_spec(spec, format="table")