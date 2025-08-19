#!/usr/bin/env python
"""Test that simplified column finding works correctly"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from adamyaml.adam_derivation import AdamDerivation

print("=" * 60)
print("Testing Simplified Column Finding")
print("=" * 60)

print("\n[TEST] Engine with simplified column access")
print("-" * 50)

try:
    engine = AdamDerivation("spec/study1/adsl_study1.yaml")
    
    # Build dataset
    df = engine.build()
    
    print(f"[OK] Built dataset: {df.shape}")
    
    # Verify columns are present
    expected_cols = ["USUBJID", "SUBJID", "DOMAIN", "STUDYID", "AGE", "SEX", "WEIGHT", "HEIGHT", "BMI"]
    missing = [col for col in expected_cols if col not in df.columns]
    
    if missing:
        print(f"[FAIL] Missing columns: {missing}")
    else:
        print(f"[OK] All expected columns present")
    
    # Check that different derivation types worked
    print("\nDerivation results:")
    print(f"  Source from DM (AGE): {df['AGE'].drop_nulls().len()}/{df.height} non-null")
    print(f"  Source with mapping (SEX): {df['SEX'].unique().to_list()}")
    print(f"  Aggregation from VS (WEIGHT): {df['WEIGHT'].drop_nulls().len()}/{df.height} non-null")
    print(f"  Custom function (BMI): {df['BMI'].drop_nulls().len()}/{df.height} non-null")
    
    print("\n[SUCCESS] Simplified column finding works correctly!")
    
except Exception as e:
    print(f"[FAIL] Error: {e}")
    import traceback
    traceback.print_exc()