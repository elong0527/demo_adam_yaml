"""
Custom YAML loader with !include tag support
"""

import yaml
from pathlib import Path


class IncludeLoader(yaml.SafeLoader):
    """Custom YAML loader that handles !include tags"""
    
    def __init__(self, stream):
        self._root = Path(stream.name).parent if hasattr(stream, 'name') else Path.cwd()
        super().__init__(stream)
        
    def include(self, node):
        """Handle !include tags"""
        filename = self.construct_scalar(node)
        filepath = self._root / filename
        
        if not filepath.exists():
            raise FileNotFoundError(f"Include file not found: {filepath}")
            
        with open(filepath, 'r') as f:
            return yaml.load(f, IncludeLoader)


# Add constructor for !include tag
IncludeLoader.add_constructor('!include', IncludeLoader.include)