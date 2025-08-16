# ADaM YAML Handler Module

A Python module for handling hierarchical YAML specifications for ADaM (Analysis Data Model) datasets following CDISC standards.

## Installation

```bash
# For development
pip install -e .

# For production
pip install adam-yaml-handler
```

## Quick Start

```python
from adam_yaml import ADaMYAMLHandler
from pathlib import Path

# Initialize handler
handler = ADaMYAMLHandler(spec_dir=Path("spec"))

# Build specification from YAML
spec = handler.build_full_spec("adsl_study.yaml")

# Validate
errors = handler.validate_spec(spec)
if not errors:
    print("Specification is valid!")

# Display
print(handler.display_spec(spec, format="table"))
```

## Module Structure

```
adam_yaml/
├── __init__.py       # Module exports
├── models.py         # Data models (Column, ADaMSpec, etc.)
├── loader.py         # Custom YAML loader with !include support
├── merger.py         # Hierarchical merging logic
├── validator.py      # Specification validation
├── handler.py        # Main handler class
├── schema.yaml       # YAML schema definition
└── tests/           # Unit tests
```

## Core Components

### ADaMYAMLHandler
Main class for handling YAML specifications:
- `build_full_spec()`: Build complete specification with inheritance
- `validate_spec()`: Validate against schema rules
- `display_spec()`: Display in various formats (yaml, json, table)
- `save_merged_spec()`: Save merged specification to file

### Data Models
- `ADaMSpec`: Complete ADaM specification
- `Column`: Column/variable specification
- `Derivation`: Derivation rules
- `ValidationRule`: Validation constraints
- `CoreType`: CDISC core types
- `DataType`: Supported data types

### YAMLMerger
Handles hierarchical merging:
- Deep merge for nested properties
- Smart column merging with override logic
- Support for dropping inherited columns

### SpecValidator
Validates specifications:
- Required fields validation
- Data type checking
- Range and constraint validation
- Key variable verification

## Features

### Hierarchical Inheritance
```yaml
# Study-specific YAML
config:
  - !include adsl_common.yaml
  - !include adsl_project.yaml

columns:
  - name: STUDYID
    derivation:
      constant: "study-999"
```

### Smart Merging
- Child specifications override parent values
- Nested properties are deep merged
- Columns can be added, modified, or dropped

### Comprehensive Validation
- Data type validation
- Range constraints
- Required field checking
- Pattern matching
- Uniqueness constraints

### Multiple Output Formats
- YAML: Human-readable specification
- JSON: Machine-readable format
- Table: Summary view

## Usage Examples

### Building Specifications
```python
from adam_yaml import ADaMYAMLHandler

handler = ADaMYAMLHandler(spec_dir=Path("spec"))
spec = handler.build_full_spec("adsl_study.yaml")
```

### Validation
```python
errors = handler.validate_spec(spec)
if errors:
    for error in errors:
        print(f"Error: {error}")
```

### Accessing Column Details
```python
for column in spec.columns:
    print(f"Column: {column.name}")
    if column.validation:
        print(f"  Validation: {column.validation.to_dict()}")
```

### Saving Merged Specifications
```python
handler.save_merged_spec(spec, Path("merged_spec.yaml"))
```

## Testing

Run tests with:
```bash
PYTHONPATH=src python -m unittest adam_yaml.tests.test_models -v
PYTHONPATH=src python -m unittest adam_yaml.tests.test_merger -v
PYTHONPATH=src python -m unittest adam_yaml.tests.test_validator -v
```

## Schema Definition

The module includes a comprehensive YAML schema (`schema.yaml`) that defines:
- Specification structure
- Column properties
- Derivation rules
- Validation constraints
- Data types and enumerations

## Dependencies

- Python >= 3.8
- PyYAML >= 6.0

## License

MIT License - See LICENSE file for details