"""
YAML specification merging functionality
"""

from typing import Dict, List, Any
from copy import deepcopy


class YAMLMerger:
    """Handles hierarchical merging of YAML specifications"""
    
    @staticmethod
    def merge_columns(base_columns: List[Dict], override_columns: List[Dict]) -> List[Dict]:
        """
        Merge column specifications with override logic
        Override columns can:
        - Add new columns
        - Override existing column properties
        - Merge nested properties (like validation rules)
        - Drop columns with drop: true flag
        """
        # Convert to dict for easier lookup
        base_dict = {col['name']: col for col in base_columns}
        
        for override_col in override_columns:
            col_name = override_col['name']
            
            # Handle drop flag
            if override_col.get('drop', False):
                base_dict.pop(col_name, None)
                continue
            
            if col_name in base_dict:
                # Merge existing column
                base_col = base_dict[col_name]
                merged_col = YAMLMerger._deep_merge(base_col, override_col)
                base_dict[col_name] = merged_col
            else:
                # Add new column
                base_dict[col_name] = override_col
        
        return list(base_dict.values())
    
    @staticmethod
    def _deep_merge(base: Any, override: Any) -> Any:
        """
        Deep merge two dictionaries
        - For dicts: merge recursively
        - For lists: override completely
        - For other types: override value
        """
        if isinstance(base, dict) and isinstance(override, dict):
            result = deepcopy(base)
            for key, value in override.items():
                if key in result:
                    result[key] = YAMLMerger._deep_merge(result[key], value)
                else:
                    result[key] = deepcopy(value)
            return result
        else:
            # For non-dict types, override completely
            return deepcopy(override)
    
    @staticmethod
    def merge_specs(base_spec: Dict, override_spec: Dict) -> Dict:
        """Merge two complete specifications"""
        result = deepcopy(base_spec)
        
        # Merge simple fields
        for field in ['domain', 'key']:
            if field in override_spec:
                result[field] = override_spec[field]
        
        # Merge columns with special logic
        if 'columns' in override_spec:
            base_columns = result.get('columns', [])
            result['columns'] = YAMLMerger.merge_columns(base_columns, override_spec['columns'])
        
        return result