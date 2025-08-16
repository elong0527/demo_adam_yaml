"""
ADaM YAML Specification Handler - Simplified Implementation
Handles hierarchical YAML specifications with validation and merging
"""

import yaml
import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field, asdict
from copy import deepcopy
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# Data Models
# ============================================================================

@dataclass
class Column:
    """ADaM column specification"""
    name: str
    label: Optional[str] = None
    type: Optional[str] = None
    core: Optional[str] = None
    derivation: Optional[Dict[str, Any]] = None
    validation: Optional[Dict[str, Any]] = None
    drop: Optional[bool] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary, excluding None values"""
        result = {}
        for key, value in asdict(self).items():
            if value is not None:
                result[key] = value
        return result


@dataclass  
class ADaMSpec:
    """Complete ADaM specification"""
    domain: str = "ADSL"
    key: List[str] = field(default_factory=list)
    columns: List[Column] = field(default_factory=list)
    parents: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary format"""
        result = {
            "domain": self.domain,
            "key": self.key,
            "columns": [col.to_dict() for col in self.columns]
        }
        if self.parents:
            result["parents"] = self.parents
        return result


# ============================================================================
# Main Handler Class
# ============================================================================

class ADaMYAMLHandler:
    """
    Main handler for ADaM YAML specifications
    
    Features:
    - Hierarchical inheritance through parents field
    - Smart merging of specifications
    - Validation of specifications
    - Multiple output formats

    """
    
    def __init__(self, spec_dir: Path = None):
        """Initialize with specification directory"""
        self.spec_dir = spec_dir or Path.cwd() / "spec"
        
    def load_yaml(self, filepath: Path) -> Dict:
        """Load YAML file"""
        with open(filepath, 'r') as f:
            return yaml.safe_load(f)
    
    def build_full_spec(self, study_yaml: str) -> ADaMSpec:
        """
        Build complete specification from study YAML file
        Handles hierarchical inheritance through parents field
        """
        study_path = self.spec_dir / study_yaml
        if not study_path.exists():
            raise FileNotFoundError(f"Study YAML not found: {study_path}")
        
        # Load study spec
        study_spec = self.load_yaml(study_path)
        
        # Process parents inheritance
        final_spec = {}
        if 'parents' in study_spec:
            for parent_file in study_spec['parents']:
                parent_path = self.spec_dir / parent_file
                if parent_path.exists():
                    parent_spec = self.load_yaml(parent_path)
                    final_spec = self._merge_specs(final_spec, parent_spec)
                else:
                    logger.warning(f"Parent file not found: {parent_path}")
        
        # Merge study-specific overrides
        final_spec = self._merge_specs(final_spec, study_spec)
        
        # Convert to ADaMSpec object
        return self._dict_to_spec(final_spec)
    
    def _merge_specs(self, base: Dict, override: Dict) -> Dict:
        """Merge two specifications with deep merging"""
        result = deepcopy(base)
        
        # Merge simple fields
        for field in ['domain', 'key', 'parents']:
            if field in override:
                result[field] = override[field]
        
        # Merge columns with special logic
        if 'columns' in override:
            base_columns = result.get('columns', [])
            result['columns'] = self._merge_columns(base_columns, override['columns'])
        
        return result
    
    def _merge_columns(self, base_columns: List[Dict], override_columns: List[Dict]) -> List[Dict]:
        """
        Merge column specifications
        - Add new columns
        - Override existing column properties
        - Drop columns with drop: true
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
                # Deep merge existing column
                base_dict[col_name] = self._deep_merge(base_dict[col_name], override_col)
            else:
                # Add new column
                base_dict[col_name] = override_col
        
        return list(base_dict.values())
    
    def _deep_merge(self, base: Any, override: Any) -> Any:
        """Deep merge two values (dicts are merged, others are replaced)"""
        if isinstance(base, dict) and isinstance(override, dict):
            result = deepcopy(base)
            for key, value in override.items():
                if key in result:
                    result[key] = self._deep_merge(result[key], value)
                else:
                    result[key] = deepcopy(value)
            return result
        else:
            return deepcopy(override)
    
    def _dict_to_spec(self, spec_dict: Dict) -> ADaMSpec:
        """Convert dictionary to ADaMSpec object"""
        columns = []
        for col_dict in spec_dict.get('columns', []):
            if not col_dict.get('drop', False):
                columns.append(Column(**{k: v for k, v in col_dict.items() if k != 'drop'}))
        
        return ADaMSpec(
            domain=spec_dict.get('domain', 'ADSL'),
            key=spec_dict.get('key', []),
            columns=columns,
            parents=spec_dict.get('parents', [])
        )
    
    def validate_spec(self, spec: ADaMSpec) -> List[str]:
        """
        Validate specification against basic rules
        Returns list of validation errors
        """
        errors = []
        
        # Check required fields
        if not spec.domain:
            errors.append("Domain is required")
        
        if not spec.key:
            errors.append("Key variables must be specified")
        
        # Check for duplicate column names
        column_names = set()
        for col in spec.columns:
            if col.name in column_names:
                errors.append(f"Duplicate column name: {col.name}")
            column_names.add(col.name)
            
            # Basic type validation
            if col.type and col.type not in ['str', 'int', 'float', 'date', 'datetime', 'bool']:
                errors.append(f"Invalid data type for {col.name}: {col.type}")
            
            # Core type validation
            if col.core and col.core not in ['cdisc-required', 'company-required', 'optional', 'conditional']:
                errors.append(f"Invalid core type for {col.name}: {col.core}")
            
            # Validation rules checks
            if col.validation:
                if 'min' in col.validation and 'max' in col.validation:
                    if col.validation['min'] > col.validation['max']:
                        errors.append(f"Invalid range for {col.name}: min > max")
        
        # Check that key variables exist in columns
        for key_var in spec.key:
            if key_var not in column_names:
                errors.append(f"Key variable {key_var} not found in columns")
        
        return errors
    
    def display_spec(self, spec: ADaMSpec, format: str = "yaml") -> str:
        """Display specification in requested format (yaml, json, table)"""
        if format == "yaml":
            spec_dict = spec.to_dict()
            return yaml.dump(spec_dict, default_flow_style=False, sort_keys=False)
        
        elif format == "json":
            spec_dict = spec.to_dict()
            return json.dumps(spec_dict, indent=2)
        
        elif format == "table":
            lines = []
            lines.append(f"Domain: {spec.domain}")
            lines.append(f"Key Variables: {', '.join(spec.key)}")
            if spec.parents:
                lines.append(f"Parent Files: {', '.join(spec.parents)}")
            lines.append("\nColumns:")
            lines.append("-" * 80)
            
            for col in spec.columns:
                lines.append(f"\nName: {col.name}")
                if col.label:
                    lines.append(f"  Label: {col.label}")
                if col.type:
                    lines.append(f"  Type: {col.type}")
                if col.core:
                    lines.append(f"  Core: {col.core}")
                if col.derivation:
                    lines.append(f"  Derivation: {col.derivation}")
                if col.validation:
                    lines.append(f"  Validation: {col.validation}")
            
            return "\n".join(lines)
        
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def save_merged_spec(self, spec: ADaMSpec, output_path: Path):
        """Save merged specification to YAML file"""
        spec_dict = spec.to_dict()
        # Don't save parents field in merged output
        if 'parents' in spec_dict:
            del spec_dict['parents']
            
        with open(output_path, 'w') as f:
            yaml.dump(spec_dict, f, default_flow_style=False, sort_keys=False)
        logger.info(f"Saved merged specification to {output_path}")