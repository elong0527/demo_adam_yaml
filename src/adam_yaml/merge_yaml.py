import yaml
from pathlib import Path
from typing import Dict, List, Any, Union
from copy import deepcopy


def merge_yaml(paths: List[Union[str, Path]]) -> Dict[str, Any]:
    """
    Merge multiple YAML files in order with deep merging
    
    Args:
        paths: List of YAML file paths to merge in order
    
    Returns:
        Merged dictionary from all YAML files
    
    Example:
        result = merge_yaml(["file1.yaml", "file2.yaml", "file3.yaml"])
        
    Merge Rules:
        - Later files override earlier files
        - Dictionaries are deep merged
        - Lists are replaced (not concatenated)
        - Scalars are replaced
    """
    def deep_merge(base: Any, override: Any) -> Any:
        """Deep merge two values"""
        if isinstance(base, dict) and isinstance(override, dict):
            result = deepcopy(base)
            for key, value in override.items():
                if key in result:
                    result[key] = deep_merge(result[key], value)
                else:
                    result[key] = deepcopy(value)
            return result
        else:
            return deepcopy(override)
    
    # Start with empty dict
    merged = {}
    
    # Merge each file in order
    for path in paths:
        with open(path, 'r') as f:
            content = yaml.safe_load(f) or {}
        merged = deep_merge(merged, content)
    
    return merged