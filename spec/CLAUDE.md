# YAML Specifications Context

## Overview
This directory contains YAML specification files for ADaM datasets following CDISC standards.

## Files Structure

### Core Specifications
- **adsl_common.yaml**: Base template with common ADaM variables shared across all datasets
- **adsl_project.yaml**: Project-level specifications that inherit from common
- **adsl_study.yaml**: Study-specific specifications that inherit from project

### Schema
- **schema.yaml**: Validation schema defining rules and constraints for specifications

## Inheritance Model
```
adsl_common.yaml (base)
    +-- adsl_project.yaml (inherits common)
        +-- adsl_study.yaml (inherits project)
```

## Key Concepts

### Column Specifications
Each column in the YAML files includes:
- **name**: Variable name (8 chars max, uppercase)
- **type**: Data type (str, int, float, date, datetime)
- **label**: Human-readable description
- **core**: CDISC requirement level
- **derivation**: Source and transformation logic

### Core Values
- **cdisc-required**: Required by CDISC standards
- **org-required**: Required by organization
- **expected**: Conditionally expected
- **permissible**: Optional but allowed

### Derivation Types
- **source**: Direct mapping from SDTM
- **constant**: Fixed values

## Validation Rules
- Domain names must start with "AD" (e.g., ADSL, ADAE)
- Column names: uppercase, max 8 characters
- Required fields: domain, columns, column.name, column.type

