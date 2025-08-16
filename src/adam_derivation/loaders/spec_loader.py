"""
Specification Loader

Loads and validates ADaM specifications from YAML files.
"""

from pathlib import Path
from typing import Dict, Any, Optional, Union, List
import sys

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from adam_spec import AdamSpec


class SpecLoader:
    """Load and validate ADaM specifications"""
    
    def __init__(self, spec_path: str):
        """
        Initialize specification loader
        
        Args:
            spec_path: Path to the YAML specification file
        """
        self.spec_path = Path(spec_path)
        
        if not self.spec_path.exists():
            raise FileNotFoundError(f"Specification not found: {spec_path}")
        
        # Load spec once during initialization
        self._spec = None
        self._load_and_cache_spec()
    
    def _load_and_cache_spec(self) -> None:
        """
        Load and cache the specification (called once during initialization)
        """
        # Use AdamSpec for loading and merging
        # AdamSpec will automatically find schema from the spec
        adam_spec = AdamSpec(str(self.spec_path))
        
        # Check validation
        if adam_spec._errors:
            errors = adam_spec._errors
            error_msg = "\n".join([f"  - {e}" for e in errors])
            raise ValueError(f"Specification validation failed with {len(errors)} errors:\n{error_msg}")
        
        # Cache the merged specification
        self._spec = adam_spec.to_dict()
    
    def load_spec(self) -> Dict[str, Any]:
        """
        Return the cached specification
        
        Returns:
            Complete merged specification dictionary
        """
        return self._spec
    
    def get_column_specs(self, names: Optional[Union[str, List[str]]] = None) -> Union[List[Dict[str, Any]], Dict[str, Any], None]:
        """
        Get column specifications from the loaded spec
        
        Args:
            names: Optional column name(s) to retrieve specific column(s)
                   Can be a single string or a list of strings
        
        Returns:
            If names is a string: Column spec dict for that column, or None if not found
            If names is a list: List of column spec dicts for those columns (skips not found)
            If names is None: List of all column specification dictionaries
        """
        columns = self._spec.get("columns", [])
        
        if names is not None:
            # Handle single name (string)
            if isinstance(names, str):
                for col in columns:
                    if col.get("name") == names:
                        return col
                return None
            
            # Handle multiple names (list)
            elif isinstance(names, list):
                result = []
                for name in names:
                    for col in columns:
                        if col.get("name") == name:
                            result.append(col)
                            break
                return result
        
        # Return all columns
        return columns
    
    @property
    def domain(self) -> str:
        """
        Get the domain name from specification
        
        Returns:
            Domain name (e.g., 'ADSL')
        """
        return self._spec.get("domain", "")
    
    @property
    def keys(self) -> list:
        """
        Get key variables for the dataset
        
        Returns:
            List of key variable names
        """
        return self._spec.get("key", [])
    
    @property
    def columns(self) -> List[str]:
        """
        Get list of column names from the specification
        
        Returns:
            List of column names
        """
        cols = self._spec.get("columns", [])
        return [col.get("name") for col in cols if "name" in col]