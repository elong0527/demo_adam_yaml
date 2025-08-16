"""
Specification Loader

Loads and validates ADaM specifications from YAML files.
"""

from pathlib import Path
from typing import Dict, Any, Optional
import sys

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from adam_spec import AdamSpec


class SpecLoader:
    """Load and validate ADaM specifications"""
    
    def __init__(self, spec_path: str, schema_path: Optional[str] = None):
        """
        Initialize specification loader
        
        Args:
            spec_path: Path to the YAML specification file
            schema_path: Optional path to schema for validation
        """
        self.spec_path = Path(spec_path)
        self.schema_path = Path(schema_path) if schema_path else None
        
        if not self.spec_path.exists():
            raise FileNotFoundError(f"Specification not found: {spec_path}")
        
        if self.schema_path and not self.schema_path.exists():
            raise FileNotFoundError(f"Schema not found: {schema_path}")
    
    def load_spec(self) -> Dict[str, Any]:
        """
        Load and merge the specification with validation
        
        Returns:
            Complete merged specification dictionary
        """
        # Use AdamSpec for loading and merging
        # AdamSpec will automatically find schema from the spec if not provided
        adam_spec = AdamSpec(
            str(self.spec_path),
            schema_path=str(self.schema_path) if self.schema_path else None
        )
        
        # Check validation
        if hasattr(adam_spec, '_errors') and adam_spec._errors:
            errors = adam_spec._errors
            error_msg = "\n".join([f"  - {e}" for e in errors])
            # Log warning but don't raise - allow derivation to continue
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Specification has validation warnings:\n{error_msg}")
        
        # Return the merged specification
        return adam_spec.to_dict()
    
    def get_column_specs(self) -> list:
        """
        Get column specifications from the loaded spec
        
        Returns:
            List of column specification dictionaries
        """
        spec = self.load_spec()
        return spec.get("columns", [])
    
    def get_domain(self) -> str:
        """
        Get the domain name from specification
        
        Returns:
            Domain name (e.g., 'ADSL')
        """
        spec = self.load_spec()
        return spec.get("domain", "")
    
    def get_key_variables(self) -> list:
        """
        Get key variables for the dataset
        
        Returns:
            List of key variable names
        """
        spec = self.load_spec()
        return spec.get("key", [])