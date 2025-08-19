#!/usr/bin/env python
"""Test full join approach to see if it simplifies things"""

import polars as pl
from adamyaml.adam_derivation.loaders import SDTMLoader

# Load data with renaming
loader = SDTMLoader("data/sdtm")
key_vars = ["USUBJID", "SUBJID"]

# Load DM and VS with renaming
dm = loader.load_dataset("DM", rename_columns=True, preserve_keys=key_vars)
vs = loader.load_dataset("VS", rename_columns=True, preserve_keys=key_vars)

print("DM shape:", dm.shape)
print("DM columns (first 10):", dm.columns[:10])
print("\nVS shape:", vs.shape)
print("VS columns (first 10):", vs.columns[:10])

# Get unique subjects from each
dm_key_cols = [col for col in key_vars if col in dm.columns]
vs_key_cols = [col for col in key_vars if col in vs.columns]

dm_subjects = dm.select(dm_key_cols).unique()
vs_subjects = vs.select(vs_key_cols).unique()

print(f"\nUnique subjects in DM: {dm_subjects.height}")
print(f"Unique subjects in VS: {vs_subjects.height}")

# Try full join approach
print("\n" + "="*60)
print("Testing Full Join Approach")
print("="*60)

# Start with DM as base (has one row per subject)
base_df = dm.select(dm_key_cols).unique()
print(f"\nBase DataFrame: {base_df.shape}")

# Join DM data
base_df = base_df.join(dm, on=dm_key_cols, how="left")
print(f"After joining DM: {base_df.shape}")

# For VS, we need to aggregate first (multiple rows per subject)
# Let's aggregate WEIGHT (VSORRES where VSTESTCD='WEIGHT')
vs_weight = vs.filter(pl.col("VS.VSTESTCD") == "WEIGHT")
if "VS.VSORRES" in vs_weight.columns:
    vs_weight = vs_weight.with_columns(
        pl.col("VS.VSORRES").cast(pl.Float64, strict=False).alias("WEIGHT")
    )
    vs_weight_agg = vs_weight.group_by(vs_key_cols).agg(
        pl.col("WEIGHT").first()
    )
    print(f"\nAggregated WEIGHT: {vs_weight_agg.shape}")
    
    # Join aggregated VS data (use common key columns)
    join_keys = list(set(dm_key_cols) & set(vs_key_cols))
    base_df = base_df.join(vs_weight_agg, on=join_keys, how="left")
    print(f"After joining VS WEIGHT: {base_df.shape}")

print(f"\nFinal shape: {base_df.shape}")
print(f"Columns available: {len(base_df.columns)} columns")

# Check if we can access all columns directly
print("\n" + "="*60)
print("Direct Column Access Test")
print("="*60)

# Now all columns are in one DataFrame
if "DM.AGE" in base_df.columns:
    print(f"[OK] Can access DM.AGE directly: {base_df['DM.AGE'].drop_nulls().len()} non-null")
    
if "WEIGHT" in base_df.columns:
    print(f"[OK] Can access WEIGHT directly: {base_df['WEIGHT'].drop_nulls().len()} non-null")

print("\nConclusion:")
print("- With full join, all columns are in one DataFrame")
print("- No need to search for columns across multiple DataFrames")
print("- Direct column access is possible")
print("- BUT: Need to aggregate multi-row datasets first")
print("- BUT: May create very wide DataFrames")