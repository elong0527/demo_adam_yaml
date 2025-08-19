#!/usr/bin/env python
"""Check how columns are structured after renaming"""

from adamyaml.adam_derivation.loaders import SDTMLoader

loader = SDTMLoader("data/sdtm")

# Load DM with renaming
dm = loader.load_dataset("DM", rename_columns=True, preserve_keys=["USUBJID", "SUBJID"])
print("DM columns after renaming:")
print(dm.columns[:10])

# Load VS with renaming  
vs = loader.load_dataset("VS", rename_columns=True, preserve_keys=["USUBJID", "SUBJID"])
print("\nVS columns after renaming:")
print(vs.columns[:10])

# Check for a specific column
print(f"\nIs 'DM.AGE' in DM columns? {'DM.AGE' in dm.columns}")
print(f"Is 'AGE' in DM columns? {'AGE' in dm.columns}")
print(f"Is 'USUBJID' in DM columns? {'USUBJID' in dm.columns}")
