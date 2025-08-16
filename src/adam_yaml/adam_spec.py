import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, asdict
from copy import deepcopy
import logging
from .merge_yaml import merge_yaml
from .schema_validator import SchemaValidator, ValidationResult

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)




@dataclass
class Column:
    """ADaM column specification"""
    name: str
    type: str
    label: Optional[str] = None
    core: Optional[str] = None
    derivation: Optional[Dict[str, Any]] = None
    validation: Optional[Dict[str, Any]] = None
    drop: Optional[bool] = None
    
    def __post_init__(self):
        """Post-initialization processing"""
        # Set label to name if not provided
        if self.label is None:
            self.label = self.name
    
    def to_dict(self) -> Dict:
        """Convert to dictionary, excluding None values and drop flag"""
        result = {}
        for key, value in asdict(self).items():
            if value is not None and key != 'drop':
                result[key] = value
        return result


class AdamSpec:
    """
    ADaM Specification with automatic loading, merging, and validation
    
    Usage:
        spec = AdamSpec(path="spec/adsl_study.yaml")
        print(spec.domain)
        print(spec.columns)
        
    Attributes:
        path: Path to the YAML file
        domain: Dataset domain (e.g., 'ADSL', 'ADAE')
        key: List of key variables
        columns: List of Column objects
        parents: List of parent YAML files
        
    Properties:
        is_valid: Whether the specification is valid
        validation_errors: List of validation errors if any
    """
    
    def __init__(self, path: Union[str, Path], validate: bool = True, 
                 schema_path: Optional[Union[str, Path]] = None):
        """
        Initialize and build complete specification from YAML file
        
        Args:
            path: Path to the study YAML file
            validate: Whether to validate the specification (default: True)
            schema_path: Optional path to schema file for validation
            
        Raises:
            FileNotFoundError: If YAML file or parent files not found
            ValueError: If validation fails and validate=True
        """
        self.path = Path(path)
        self.schema_path = Path(schema_path) if schema_path else None
        self.domain: str = ""  
        self.key: List[str] = []
        self.columns: List[Column] = []
        self.parents: List[str] = []
        self._errors: List[str] = []
        self._warnings: List[str] = []
        self._raw_spec: Dict = {}
        self._schema_results: List[ValidationResult] = []
        
        # Build and optionally validate
        self._build_spec()
        
        if validate:
            # Use schema validation if path provided
            if self.schema_path:
                self._validate_with_schema()
            else:
                # Basic validation inline
                if not self.domain:
                    self._errors.append("Domain is required")
                if not self.columns:
                    self._errors.append("No columns defined")
            
            if self._errors:
                error_msg = f"Validation errors: {self._errors}"
                logger.error(error_msg)
                raise ValueError(error_msg)
    
    def _build_spec(self) -> None:
        """Build complete specification with inheritance"""
        if not self.path.exists():
            raise FileNotFoundError(f"YAML file not found: {self.path}")
        
        # Load study spec to get parents
        try:
            with open(self.path, 'r') as f:
                study_spec = yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in {self.path}: {e}")
        
        # Build list of all YAML files to merge
        yaml_files = self._collect_yaml_files(study_spec)
        
        # Use merge_yaml function with appropriate strategy
        final_spec = merge_yaml(
            yaml_files,
            list_merge_strategy="merge_by_key",
            list_merge_keys={"columns": "name"}
        )
        
        # Store raw spec and extract fields
        self._raw_spec = final_spec
        self._extract_fields(final_spec)
    
    def _collect_yaml_files(self, study_spec: Dict) -> List[str]:
        """Collect all YAML files to merge including parents"""
        yaml_files = []
        spec_dir = self.path.parent
        
        # Add parent files if they exist
        if 'parents' in study_spec:
            self.parents = study_spec['parents'] if isinstance(study_spec['parents'], list) else [study_spec['parents']]
            
            for parent_file in self.parents:
                parent_path = spec_dir / parent_file
                if not parent_path.exists():
                    raise FileNotFoundError(f"Parent file not found: {parent_path}")
                yaml_files.append(str(parent_path))
        
        # Add the study file itself (last, so it overrides)
        yaml_files.append(str(self.path))
        return yaml_files
    
    def _extract_fields(self, spec: Dict) -> None:
        """Extract fields from merged specification"""
        # Extract standard fields
        self.domain = spec.get('domain', '')
        self.key = spec.get('key', [])
        
        # Process columns
        raw_columns = spec.get('columns', [])
        processed_columns = self._process_columns(raw_columns)
        
        self.columns = []
        for col_dict in processed_columns:
            try:
                # Ensure required fields
                if 'type' not in col_dict:
                    self._errors.append(f"Column {col_dict.get('name', 'unknown')} missing required 'type' field")
                    continue
                    
                # Create Column object
                col = Column(**{k: v for k, v in col_dict.items() if k != 'drop'})
                self.columns.append(col)
            except TypeError as e:
                self._errors.append(f"Invalid column specification: {e}")
    
    def _process_columns(self, columns: List[Dict]) -> List[Dict]:
        """Process columns, handling drop flags and defaults"""
        result = []
        dropped_names = set()
        
        # Collect dropped column names
        for col in columns:
            if col.get('drop', False):
                dropped_names.add(col['name'])
                logger.debug(f"Column marked for drop: {col['name']}")
        
        # Return non-dropped columns
        for col in columns:
            if col.get('name') not in dropped_names and not col.get('drop', False):
                # Ensure label defaults to name
                if 'label' not in col or col['label'] is None:
                    col['label'] = col['name']
                result.append(col)
        
        return result
    
    def _validate_with_schema(self) -> None:
        """Validate using SchemaValidator"""
        try:
            validator = SchemaValidator(self.schema_path)
            self._schema_results = validator.validate(self._raw_spec)
            
            # Use SchemaValidator's built-in methods
            for error in validator.get_errors():
                self._errors.append(f"[{error.rule}] {error.message}")
            
            for warning in validator.get_warnings():
                self._warnings.append(f"[{warning.rule}] {warning.message}")
            
            logger.info(f"Schema validation complete: {len(validator.get_errors())} errors, {len(validator.get_warnings())} warnings")
        except Exception as e:
            logger.error(f"Schema validation failed: {e}")
            self._errors.append(f"Schema validation error: {e}")

    def to_dict(self, include_parents: bool = False) -> Dict:
        """
        Convert to dictionary format
        
        Args:
            include_parents: Whether to include parent files in output
        """
        result = deepcopy(self._raw_spec)
        
        # Update with current values
        result["domain"] = self.domain
        result["key"] = self.key
        result["columns"] = [col.to_dict() for col in self.columns]
        
        # Optionally remove parents
        if not include_parents:
            result.pop('parents', None)
        
        return result
    
    def to_yaml(self, include_parents: bool = False) -> str:
        """Convert to YAML string"""
        return yaml.dump(
            self.to_dict(include_parents),
            default_flow_style=False,
            sort_keys=False
        )
    
    def save(self, output_path: Union[str, Path]) -> None:
        """
        Save specification to file
        
        Args:
            output_path: Path to save file
            format: Output format ('yaml', 'json')
        """
        output = Path(output_path)
        
        with open(output, 'w') as f:
            f.write(self.to_yaml())
        
        logger.info(f"Saved YAML specification to {output}")