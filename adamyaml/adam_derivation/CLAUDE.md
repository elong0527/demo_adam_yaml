# adam_derivation Module Documentation

## Module Purpose
The `adam_derivation` module is a **simplified derivation engine** for ADaM dataset generation. It executes data transformations based on YAML specifications to produce ADaM datasets from SDTM source data.

## Architecture Overview

### Simplified Design Principles
1. **Minimal Abstraction**: Direct operations without unnecessary layers
2. **Column Renaming**: Source columns renamed to `{DOMAIN}.{column}` format at load time
3. **Unified Interface**: All derivations return DataFrames, not Series
4. **No Complex Joins**: Assumes one row per key combination
5. **Clear Separation**: Engine orchestrates, derivations transform

## Core Components

### 1. AdamDerivation Engine (engine.py)
Simplified orchestration engine that:
- Loads YAML specifications via AdamSpec
- Manages source data loading with column renaming
- Iterates through column specifications
- Delegates to appropriate derivation classes
- Returns final Polars DataFrame

Key methods:
```python
def build() -> pl.DataFrame:
    """Build the ADaM dataset"""
    
def _derive_column(col_spec: dict) -> None:
    """Derive single column using factory pattern"""
```

### 2. SDTMLoader (loaders/sdtm_loader.py)
Data loading with automatic column renaming:
- Loads SDTM datasets from parquet files
- Extracts DOMAIN value from dataset
- Renames columns to `{DOMAIN}.{column}` format
- Preserves key variables without renaming

Key feature:
```python
def load_dataset(dataset_name: str, 
                rename_columns: bool = False,
                preserve_keys: list[str] | None = None) -> pl.DataFrame:
    """Load dataset with optional column renaming"""
```

### 3. BaseDerivation (derivations/base.py)
Abstract base class defining the derivation interface:
- Single `derive()` method that returns DataFrame
- Helper methods for source dataset access and filtering
- No separate compute/derive split

Interface:
```python
@abstractmethod
def derive(source_data: dict[str, pl.DataFrame],
          target_df: pl.DataFrame,
          column_spec: dict[str, Any]) -> pl.DataFrame:
    """Derive column and return updated dataframe"""
```

### 4. DerivationFactory (derivations/base.py)
Simple dispatch mechanism:
- Examines derivation specification keys
- Returns appropriate derivation class instance
- No complex registration or plugin system

## Derivation Types

### SourceDerivation (source.py)
Direct column mapping from source to target:
- Handles renamed columns (`DM.AGE`)
- Supports value mapping/recoding
- Simple copy operation

### ConstantDerivation (constant.py)
Assigns constant values:
- Single value for all rows
- Used for study/domain identifiers

### AggregationDerivation (aggregation.py)
Summarizes multiple records per subject:
- Functions: first, last, mean, median, min, max, sum, count, closest
- Supports filtering before aggregation
- Handles "closest to baseline" logic

### CustomDerivation (custom.py)
User-defined functions:
- Built-in functions (e.g., BMI calculation)
- External function loading
- Flexible argument passing

### CategorizationDerivation (categorization.py)
Cut-based categorical variables:
- Range-based rules
- Multiple condition support
- Automatic type conversion

### ConditionalDerivation (condition.py)
When/then/else logic:
- Multiple condition branches
- Complex condition expressions
- Default value handling

## Data Flow

### Column Renaming Strategy
```
Original SDTM:
  DM: [USUBJID, AGE, SEX, RACE, ...]
  VS: [USUBJID, VSTESTCD, VSORRES, ...]

After Loading (renamed):
  DM: [USUBJID, DM.AGE, DM.SEX, DM.RACE, ...]
  VS: [USUBJID, VS.VSTESTCD, VS.VSORRES, ...]

In Specification:
  derivation:
    source: DM.AGE  # References renamed column directly
```

### Data Caching Optimization
- Source data loaded **once** with renaming at pipeline start
- Key variables preserved without renaming (e.g., USUBJID, SUBJID)
- Renamed data cached and reused throughout derivation
- No duplicate loading - efficient memory usage

### Processing Flow
1. Load specification from YAML
2. Load source datasets with renaming
3. Build base dataset from key variables
4. For each column specification:
   - Factory creates derivation instance
   - Derivation processes and returns DataFrame
   - Target DataFrame updated with new column
5. Return final DataFrame

## Key Simplifications from Previous Design

### Removed Complexity
- **No _add_series_to_df**: Derivations return DataFrames directly
- **No _needs_joining**: Assumes proper data structure
- **No separate compute/derive**: Single derive method
- **No original vs renamed data**: Only renamed data used
- **No complex joining logic**: Direct operations

### Benefits
- Easier to understand and maintain
- Fewer potential error points
- Better performance (fewer operations)
- Clearer data flow

## Usage Example

```python
from adamyaml.adam_derivation import AdamDerivation

# Initialize engine with specification
engine = AdamDerivation("spec/adsl_study.yaml")

# Build ADaM dataset
adam_df = engine.build()

# Save result
adam_df.write_parquet("output/adsl.parquet")
```

## Testing Approach

### Unit Tests
Each derivation type tested independently:
- Mock source data
- Verify transformations
- Edge case handling

### Integration Tests
Full pipeline testing:
- Real SDTM data
- Complete specifications
- Result validation

## Error Handling

### Common Issues
- **Missing source columns**: Clear error messages
- **Type mismatches**: Automatic conversion where possible
- **Invalid specifications**: Caught during validation
- **Missing data**: Appropriate null handling

### Logging
- INFO: Major operations (loading, deriving)
- DEBUG: Detailed operations (filtering, joining)
- WARNING: Non-fatal issues (missing optional data)
- ERROR: Fatal issues (invalid specifications)

## Performance Considerations

### Optimizations
- Polars for fast DataFrame operations
- Column renaming at load time (once per dataset)
- Direct DataFrame updates (no intermediate Series)
- Lazy evaluation where possible

### Scalability
- Handles typical clinical trial data sizes
- Memory efficient with Polars
- Suitable for datasets with millions of rows

## Future Enhancements

### Potential Improvements
- Parallel column derivation
- Caching of intermediate results
- More built-in aggregation functions
- Expression-based derivations

### Maintaining Simplicity
Any enhancements should:
- Not add unnecessary abstraction
- Maintain clear data flow
- Keep derivations independent
- Preserve the simple interface

## ASCII-Only Policy
- All code uses ASCII characters only
- No emojis or Unicode in code/comments
- Status indicators: [OK], [FAIL], [INFO]
- Clear, descriptive naming

## Summary

The `adam_derivation` module provides a **simple, efficient engine** for ADaM dataset generation. By using column renaming at load time and having derivations return DataFrames directly, the architecture remains clean and performant while handling complex clinical trial data transformations.