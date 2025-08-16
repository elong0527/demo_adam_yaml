"""
Main handler for ADaM YAML specifications
"""

import yaml
import json
from pathlib import Path
from typing import Dict, List, Optional
import logging

from .models import ADaMSpec, Column, CoreType, DataType, Derivation, ValidationRule
from .loader import IncludeLoader
from .merger import YAMLMerger
from .validator import SpecValidator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ADaMYAMLHandler:
    """Main handler for ADaM YAML specifications"""
    
    def __init__(self, spec_dir: Path = None):
        """Initialize handler with specification directory"""
        self.spec_dir = spec_dir or Path.cwd() / "spec"
        self.merger = YAMLMerger()
        self.validator = SpecValidator()
        
    def load_yaml(self, filepath: Path) -> Dict:
        """Load YAML file with !include support"""
        with open(filepath, 'r') as f:
            return yaml.load(f, IncludeLoader)
    
    def build_full_spec(self, study_yaml: str) -> ADaMSpec:
        """
        Build complete specification from study YAML file
        Handles hierarchical inheritance through config field
        """
        study_path = self.spec_dir / study_yaml
        if not study_path.exists():
            raise FileNotFoundError(f"Study YAML not found: {study_path}")
        
        # Load study spec
        study_spec = self.load_yaml(study_path)
        
        # Process config inheritance
        final_spec = {}
        if 'config' in study_spec:
            for config_ref in study_spec['config']:
                # Handle !include references
                if isinstance(config_ref, dict):
                    # Already loaded by IncludeLoader
                    final_spec = self.merger.merge_specs(final_spec, config_ref)
                elif isinstance(config_ref, str):
                    # Load referenced file
                    config_path = self.spec_dir / config_ref
                    if config_path.exists():
                        config_spec = self.load_yaml(config_path)
                        final_spec = self.merger.merge_specs(final_spec, config_spec)
        
        # Merge study-specific overrides
        final_spec = self.merger.merge_specs(final_spec, study_spec)
        
        # Convert to ADaMSpec object
        return self._dict_to_spec(final_spec)
    
    def _dict_to_spec(self, spec_dict: Dict) -> ADaMSpec:
        """Convert dictionary to ADaMSpec object"""
        columns = []
        for col_dict in spec_dict.get('columns', []):
            # Skip dropped columns
            if not col_dict.get('drop', False):
                columns.append(Column.from_dict(col_dict))
        
        return ADaMSpec(
            domain=spec_dict.get('domain', 'ADSL'),
            key=spec_dict.get('key', []),
            columns=columns,
            config=spec_dict.get('config', [])
        )
    
    def validate_spec(self, spec: ADaMSpec) -> List[str]:
        """
        Validate specification against rules
        Returns list of validation errors
        """
        return self.validator.validate(spec)
    
    def display_spec(self, spec: ADaMSpec, format: str = "yaml") -> str:
        """
        Display specification in requested format
        Supported formats: yaml, json, table
        """
        if format == "yaml":
            return yaml.dump(spec.to_dict(), default_flow_style=False, sort_keys=False)
        elif format == "json":
            return json.dumps(spec.to_dict(), indent=2)
        elif format == "table":
            return self._format_as_table(spec)
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def _format_as_table(self, spec: ADaMSpec) -> str:
        """Format specification as a readable table"""
        lines = []
        lines.append(f"Domain: {spec.domain}")
        lines.append(f"Key Variables: {', '.join(spec.key)}")
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
                lines.append(f"  Derivation: {col.derivation.to_dict()}")
            if col.validation:
                lines.append(f"  Validation: {col.validation.to_dict()}")
        
        return "\n".join(lines)
    
    def save_merged_spec(self, spec: ADaMSpec, output_path: Path):
        """Save merged specification to file"""
        with open(output_path, 'w') as f:
            yaml.dump(spec.to_dict(), f, default_flow_style=False, sort_keys=False)
        logger.info(f"Saved merged specification to {output_path}")