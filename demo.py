#!/usr/bin/env python3
"""
Demo script for the ADaM YAML system
Shows how to load specifications and create derivation engines
"""

import sys
from pathlib import Path

# Add the current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

def main():
    print("=" * 60)
    print("ADaM YAML System Demonstration")
    print("=" * 60)
    
    try:
        # Import the ADaM components
        from adamyaml.adam_spec import AdamSpec
        from adamyaml.adam_derivation import AdamDerivation
        
        print("\n1. Loading ADaM Specification...")
        spec_path = "spec/study1/final_adsl_study1.yaml"
        spec = AdamSpec(spec_path)
        
        print(f"   ✓ Successfully loaded specification for domain: {spec.domain}")
        print(f"   ✓ Number of columns: {len(spec.columns)}")
        print(f"   ✓ Key variables: {spec.key}")
        
        print("\n2. Creating Derivation Engine...")
        engine = AdamDerivation(spec_path)
        
        print(f"   ✓ Successfully created derivation engine")
        print(f"   ✓ SDTM data directory: {engine.spec.sdtm_dir}")
        print(f"   ✓ ADaM output directory: {engine.spec.adam_dir}")
        
        print("\n3. Column Information:")
        for i, col in enumerate(spec.columns[:5], 1):  # Show first 5 columns
            print(f"   {i}. {col.name} ({col.type}) - {col.label}")
            if col.derivation:
                if 'source' in col.derivation:
                    print(f"      Source: {col.derivation['source']}")
                if 'constant' in col.derivation:
                    print(f"      Constant: {col.derivation['constant']}")
        
        if len(spec.columns) > 5:
            print(f"   ... and {len(spec.columns) - 5} more columns")
        
        print("\n4. Data Dependencies:")
        dependencies = spec.get_data_dependency()
        print(f"   ✓ Found {len(dependencies)} data dependencies")
        
        # Group by SDTM dataset
        sdtm_datasets = {}
        for dep in dependencies:
            sdtm_dataset = dep['sdtm_data']
            if sdtm_dataset not in sdtm_datasets:
                sdtm_datasets[sdtm_dataset] = []
            sdtm_datasets[sdtm_dataset].append(dep['adam_variable'])
        
        for sdtm_dataset, adam_vars in sdtm_datasets.items():
            print(f"   - {sdtm_dataset}: {', '.join(adam_vars[:3])}{'...' if len(adam_vars) > 3 else ''}")
        
        print("\n" + "=" * 60)
        print("Demo completed successfully!")
        print("The system is ready to generate ADaM datasets from SDTM data.")
        print("=" * 60)
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Make sure you're running this from the project root with the virtual environment activated.")
        print("Run: source .venv/bin/activate && python demo.py")
        return 1
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
