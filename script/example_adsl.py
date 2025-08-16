#!/usr/bin/env python3
"""
Example usage of the ADaM YAML Handler module
"""

import sys
from pathlib import Path

# Add src to path for development
sys.path.insert(0, 'src')

from adam_yaml import ADaMYAMLHandler


def main():
    """Demonstrate usage of the ADaM YAML Handler"""
    
    # Initialize handler with spec directory
    handler = ADaMYAMLHandler(spec_dir=Path("spec"))
    
    print("=" * 80)
    print("ADaM YAML Handler - Example Usage")
    print("=" * 80)
    
    try:
        # Build full specification from study YAML
        print("\n1. Building specification from adsl_study.yaml...")
        spec = handler.build_full_spec("adsl_study.yaml")
        print(f"   - Domain: {spec.domain}")
        print(f"   - Key variables: {', '.join(spec.key)}")
        print(f"   - Number of columns: {len(spec.columns)}")
        
        # Validate specification
        print("\n2. Validating specification...")
        errors = handler.validate_spec(spec)
        if errors:
            print("   Validation errors found:")
            for error in errors:
                print(f"   - {error}")
        else:
            print("   [SUCCESS] Specification is valid!")
        
        # Display in different formats
        print("\n3. Display formats:")
        
        # Table format
        print("\n   a) Table format:")
        print("   " + "-" * 76)
        table_output = handler.display_spec(spec, format="table")
        for line in table_output.split('\n')[:20]:  # First 20 lines
            print("   " + line)
        print("   ...")
        
        # JSON format (first few lines)
        print("\n   b) JSON format (excerpt):")
        json_output = handler.display_spec(spec, format="json")
        for line in json_output.split('\n')[:10]:  # First 10 lines
            print("   " + line)
        print("   ...")
        
        # Save merged specification
        print("\n4. Saving merged specification...")
        output_path = Path("spec/merged_adsl_example.yaml")
        handler.save_merged_spec(spec, output_path)
        print(f"   [SUCCESS] Saved to {output_path}")
        
        # Demonstrate column access
        print("\n5. Accessing column details:")
        for col in spec.columns[:3]:  # First 3 columns
            print(f"\n   Column: {col.name}")
            if col.label:
                print(f"     Label: {col.label}")
            if col.type:
                print(f"     Type: {col.type}")
            if col.core:
                print(f"     Core: {col.core}")
        
        print("\n" + "=" * 80)
        print("Example completed successfully!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n[ERROR] {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()