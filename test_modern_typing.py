#!/usr/bin/env python
"""Test that modern typing works correctly"""

print("Testing modern typing in adamyaml package")
print("=" * 60)

# Test imports
from adamyaml import AdamSpec, AdamDerivation, DataValidator

# Test AdamSpec with modern typing
spec = AdamSpec("spec/adsl_study1.yaml")
print(f"[OK] AdamSpec loaded: {spec.domain}")

# Test type annotations work
columns: list[dict] = spec.get_column_specs()
print(f"[OK] Got {len(columns)} columns")

# Test specific column
col_spec: dict | None = spec.get_column_specs("AGE")
if col_spec:
    print(f"[OK] Got AGE column: {col_spec['type']}")

# Test AdamDerivation
engine = AdamDerivation("spec/adsl_study1.yaml")
print(f"[OK] AdamDerivation created")

# Test data dependencies
deps: list[dict] = spec.get_data_dependency()
print(f"[OK] Found {len(deps)} dependencies")

print("\n" + "=" * 60)
print("All modern typing tests passed!")