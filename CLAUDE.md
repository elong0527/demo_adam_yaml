# Project Context for Claude

## Important Guidelines
- **Use ASCII characters only** - No emojis or special Unicode characters in code or commits

## Project Overview
This project provides a Python module for handling YAML-based specifications for ADaM (Analysis Data Model) datasets in clinical trials, following CDISC standards.

## Project Structure
```
demo_adam_yaml/
+-- src/
|   +-- adam_spec/           # Main specification module
|       +-- __init__.py
|       +-- adam_spec.py     # Core specification class
|       +-- merge_yaml.py    # YAML merging utilities
|       +-- schema_validator.py  # Schema validation
|       +-- tests/           # Unit tests
|           +-- test_adam_spec.py
|           +-- test_merge_yaml.py
|           +-- test_schema_validator.py
+-- spec/                    # YAML specifications
|   +-- adsl_common.yaml    # Common variables
|   +-- adsl_project.yaml   # Project-level specs
|   +-- adsl_study.yaml     # Study-specific specs
|   +-- schema.yaml         # Validation schema
+-- data/                    # CDISC pilot study data
|   +-- sdtm/               # SDTM datasets (22 files)
|   +-- adam/               # ADaM datasets (10 files)
+-- run_tests.py            # Test runner script
+-- README_adam_spec.md     # Module documentation
+-- CODE_REVIEW.md          # Code review findings
```

## Key Technologies
- **Python 3.7+**: Core programming language
- **PyYAML**: YAML parsing and generation
- **Pathlib**: Modern path handling
- **Dataclasses**: Structured data representation
- **Type hints**: Static type checking support
- **unittest**: Built-in testing framework

## Module Features

### 1. AdamSpec Class
- Load and merge hierarchical YAML specifications
- Automatic parent file inheritance
- Schema-based validation
- Multiple export formats (YAML, JSON, dict)
- Column management and access methods

### 2. merge_yaml Function
- General-purpose YAML merging
- Multiple merge strategies (replace, append, merge_by_key)
- Deep merging support
- Column-specific merge handling

### 3. SchemaValidator Class
- Pattern-based validation (regex)
- Type checking
- Required field validation
- Cross-field validation rules
- Detailed error reporting

## Usage Examples

### Basic Usage
```python
from adam_spec import AdamSpec

# Load specification with schema validation
spec = AdamSpec("spec/adsl_study.yaml", schema_path="spec/schema.yaml")

# Access specification data
print(spec.domain)  # "ADSL"
print(spec.key)     # ['DOMAIN', 'USUBJID']
print(len(spec.columns))  # Number of columns

# Get specific column
usubjid = spec.get_column("USUBJID")
print(usubjid.type)   # "str"
print(usubjid.label)  # "Unique Subject Identifier"
```

### YAML Merging
```python
from adam_spec import merge_yaml

# Merge multiple YAML files
merged = merge_yaml(
    paths=["base.yaml", "override.yaml"],
    list_merge_strategy="merge_by_key",
    list_merge_keys={"columns": "name"}
)
```

### Schema Validation
```python
from adam_spec import SchemaValidator

validator = SchemaValidator("spec/schema.yaml")
results = validator.validate(spec_dict)

if validator.is_valid():
    print("Specification is valid")
else:
    for error in validator.get_errors():
        print(f"ERROR: {error.message}")
```

## YAML Specification Structure

### Inheritance Model
1. **adsl_common.yaml**: Base template with common variables
2. **adsl_project.yaml**: Project-level specifications (inherits from common)
3. **adsl_study.yaml**: Study-specific specifications (inherits from project)

### Example Specification
```yaml
parents:
  - adsl_common.yaml
  - adsl_project.yaml

domain: ADSL
key: [DOMAIN, USUBJID]

columns:
  - name: USUBJID
    type: str
    label: Unique Subject Identifier
    core: cdisc-required
    derivation:
      source: DM.USUBJID
```

## Schema Rules

### Domain Naming
- Must start with "AD" prefix
- Maximum 8 characters
- Pattern: `^AD[A-Z0-9]{0,6}$`

### Column Naming
- Uppercase letters, numbers, underscore
- Maximum 8 characters
- Pattern: `^[A-Z][A-Z0-9_]{0,7}$`

### Required Fields
- Domain name
- Column specifications
- Column name, type, and derivation

## Testing

### Run All Tests
```bash
# From project root
uv run python run_tests.py

# Or using unittest
uv run python -m unittest discover -s src/adam_yaml/tests
```

### Test Coverage
- AdamSpec: 7 tests (loading, inheritance, validation, export)
- merge_yaml: 5 tests (strategies, edge cases)
- SchemaValidator: 6 tests (validation rules, patterns)

## Development Commands

```bash
# Install dependencies
uv sync

# Run tests
uv run python run_tests.py

# Test specific module
uv run python -m unittest adam_spec.tests.test_adam_spec

# Check for non-ASCII characters
find . -name "*.py" -exec grep -l "[^\x00-\x7F]" {} \;
```

## Important Notes

### ASCII-Only Policy
- All code uses ASCII characters only
- No emojis in code, comments, or commits
- Use [OK], [FAIL], [X], [!], [i] for status indicators
- Use +-- and | for tree structures

### Error Handling
- FileNotFoundError for missing files
- ValueError for invalid YAML
- Detailed validation error messages
- Warning logs for skipped validation

### Best Practices
- Always provide schema_path for validation
- Use hierarchical YAML structure for DRY principle
- Follow CDISC naming conventions
- Test all changes with unit tests

## API Summary

### Classes
- **AdamSpec**: Main specification handler
- **Column**: Column specification dataclass
- **SchemaValidator**: Schema-based validator
- **ValidationResult**: Validation result dataclass

### Functions
- **merge_yaml()**: Merge multiple YAML files with strategies

### Properties
- **spec.domain**: Dataset domain name
- **spec.columns**: List of Column objects
- **spec.validation_errors**: List of validation errors
- **spec.is_valid**: Boolean validation status

## References
- [CDISC ADaM v2.1](https://www.cdisc.org/standards/foundational/adam)
- [CDISC Pilot Project](https://github.com/cdisc-org/sdtm-adam-pilot-project)
- [ADaM Implementation Guide](https://www.cdisc.org/standards/foundational/adam/adam-implementation-guide-v1-3)