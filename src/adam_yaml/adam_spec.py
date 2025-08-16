"""
ADaM YAML Specification - Single Class Implementation
All validation and merging happens at initialization
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


class AdamSpec:
    """
    ADaM Specification with automatic loading, merging, and validation
    
    Usage:
        spec = AdamSpec(path="spec/adsl_study.yaml")
        print(spec.domain)
        print(spec.columns)
    """
    
    def __init__(self, path: str):
        """
        Initialize and build complete specification from YAML file
        
        Args:
            path: Path to the study YAML file
        """
        self.path = Path(path)
        self.domain: str = "ADSL"
        self.key: List[str] = []
        self.columns: List[Column] = []
        self.parents: List[str] = []
        self._errors: List[str] = []
        
        # Build, merge, and validate at initialization
        self._build_spec()
        self._validate()
        
        if self._errors:
            logger.warning(f"Validation errors: {self._errors}")
    
    def _load_yaml(self, filepath: Path) -> Dict:
        """Load YAML file"""
        with open(filepath, 'r') as f:
            return yaml.safe_load(f)
    
    def _build_spec(self):
        """Build complete specification with inheritance"""
        if not self.path.exists():
            raise FileNotFoundError(f"YAML file not found: {self.path}")
        
        # Get the directory containing the study file
        spec_dir = self.path.parent
        
        # Load study spec
        study_spec = self._load_yaml(self.path)
        
        # Process parents inheritance
        final_spec = {}
        if 'parents' in study_spec:
            self.parents = study_spec['parents']
            for parent_file in self.parents:
                parent_path = spec_dir / parent_file
                if parent_path.exists():
                    parent_spec = self._load_yaml(parent_path)
                    final_spec = self._merge_specs(final_spec, parent_spec)
                else:
                    logger.warning(f"Parent file not found: {parent_path}")
        
        # Merge study-specific overrides
        final_spec = self._merge_specs(final_spec, study_spec)
        
        # Set attributes from merged spec
        self.domain = final_spec.get('domain', [])
        self.key = final_spec.get('key', [])
        
        # Process columns
        columns = []
        for col_dict in final_spec.get('columns', []):
            if not col_dict.get('drop', False):
                columns.append(Column(**{k: v for k, v in col_dict.items() if k != 'drop'}))
        self.columns = columns
    
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
    
    def _validate(self):
        """Validate specification and populate _errors list"""
        self._errors = []
        
        # Check required fields
        if not self.domain:
            self._errors.append("Domain is required")
        
        if not self.key:
            self._errors.append("Key variables must be specified")
        
        # Check for duplicate column names
        column_names = set()
        for col in self.columns:
            if col.name in column_names:
                self._errors.append(f"Duplicate column name: {col.name}")
            column_names.add(col.name)
            
            # Basic type validation
            if col.type and col.type not in ['str', 'int', 'float', 'date', 'datetime', 'bool']:
                self._errors.append(f"Invalid data type for {col.name}: {col.type}")
            
            # Core type validation
            if col.core and col.core not in ['cdisc-required', 'company-required', 'optional', 'conditional']:
                self._errors.append(f"Invalid core type for {col.name}: {col.core}")
            
            # Validation rules checks
            if col.validation:
                if 'min' in col.validation and 'max' in col.validation:
                    if col.validation['min'] > col.validation['max']:
                        self._errors.append(f"Invalid range for {col.name}: min > max")
        
        # Check that key variables exist in columns
        for key_var in self.key:
            if key_var not in column_names:
                self._errors.append(f"Key variable {key_var} not found in columns")
    
    @property
    def is_valid(self) -> bool:
        """Check if specification is valid"""
        return len(self._errors) == 0
    
    @property
    def validation_errors(self) -> List[str]:
        """Get validation errors"""
        return self._errors
    
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
    
    def to_yaml(self) -> str:
        """Convert to YAML string"""
        spec_dict = self.to_dict()
        # Remove parents from output (it's metadata)
        spec_dict.pop('parents', None)
        return yaml.dump(spec_dict, default_flow_style=False, sort_keys=False)
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        spec_dict = self.to_dict()
        spec_dict.pop('parents', None)
        return json.dumps(spec_dict, indent=2)
    
    def to_table(self) -> str:
        """Convert to table format"""
        lines = []
        lines.append(f"Domain: {self.domain}")
        lines.append(f"Key Variables: {', '.join(self.key)}")
        if self.parents:
            lines.append(f"Parent Files: {', '.join(self.parents)}")
        lines.append("\nColumns:")
        lines.append("-" * 80)
        
        for col in self.columns:
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
    
    def save(self, output_path: str):
        """Save merged specification to YAML file"""
        output = Path(output_path)
        with open(output, 'w') as f:
            f.write(self.to_yaml())
        logger.info(f"Saved merged specification to {output}")
    
    def __repr__(self) -> str:
        """String representation"""
        return f"AdamSpec(domain='{self.domain}', columns={len(self.columns)}, valid={self.is_valid})"
    
    def __str__(self) -> str:
        """Human-readable string"""
        return self.to_table()