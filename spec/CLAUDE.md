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

## Update final spec 

```python
# create spec without schema validation 
adsl_spec = AdamSpec("spec/adsl_study.yaml")

# create spec with schema validation
adsl_spec = AdamSpec("spec/adsl_study.yaml", schema_path="spec/schema.yaml")

# store the combined YAML file
adsl_spec.save("spec/adsl_study_combined.yaml")
```

# ADSL Variable Derivation Methods 

## Method 1: constant 
   - apply **constant** to all values 

## Method 2: source  
    - apply **filter** if exists
    - apply **source** using **key** for row identifier 
    - validate **source** has one and only one value per unique **key** value 

## Method 3: source then apply mapping 
    - apply **filter** if exists
    - apply **source** using **key** for row identifier 
    - mapping values based on **mapping** 
    - validate **source** has one and only one value per unique **key** value 

## Method 4: source then apply aggregation 
    - apply **filter** if exists 
    - apply **source** using **key** for row identifier 
    - aggregate values based on specified function and its argument
    - validate **source** has one and only one value per unique **key** value 

## Method 5: general function with arguments
    - apply general function with arguments