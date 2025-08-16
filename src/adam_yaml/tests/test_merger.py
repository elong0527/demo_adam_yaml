"""
Test cases for YAML merging functionality
"""

import unittest
from ..merger import YAMLMerger


class TestYAMLMerger(unittest.TestCase):
    """Test YAML merging functionality"""
    
    def test_merge_columns_add_new(self):
        """Test adding new columns"""
        base = [{"name": "COL1", "type": "str"}]
        override = [{"name": "COL2", "type": "int"}]
        
        result = YAMLMerger.merge_columns(base, override)
        
        self.assertEqual(len(result), 2)
        names = [col["name"] for col in result]
        self.assertIn("COL1", names)
        self.assertIn("COL2", names)
    
    def test_merge_columns_override_existing(self):
        """Test overriding existing column properties"""
        base = [{"name": "AGE", "type": "int", "validation": {"min": 0, "max": 120}}]
        override = [{"name": "AGE", "validation": {"min": 18, "max": 80}}]
        
        result = YAMLMerger.merge_columns(base, override)
        
        self.assertEqual(len(result), 1)
        age_col = result[0]
        self.assertEqual(age_col["name"], "AGE")
        self.assertEqual(age_col["type"], "int")  # Preserved from base
        self.assertEqual(age_col["validation"]["min"], 18)  # Overridden
        self.assertEqual(age_col["validation"]["max"], 80)  # Overridden
    
    def test_merge_columns_with_drop(self):
        """Test dropping columns with drop flag"""
        base = [
            {"name": "COL1", "type": "str"},
            {"name": "COL2", "type": "int"},
            {"name": "COL3", "type": "float"}
        ]
        override = [
            {"name": "COL2", "drop": True},
            {"name": "COL4", "type": "date"}
        ]
        
        result = YAMLMerger.merge_columns(base, override)
        
        names = [col["name"] for col in result]
        self.assertIn("COL1", names)  # Preserved
        self.assertNotIn("COL2", names)  # Dropped
        self.assertIn("COL3", names)  # Preserved
        self.assertIn("COL4", names)  # Added
    
    def test_deep_merge_nested_dicts(self):
        """Test deep merging of nested dictionaries"""
        base = {
            "name": "VAR1",
            "validation": {"min": 0, "max": 100, "unique": True}
        }
        override = {
            "name": "VAR1",
            "validation": {"min": 10, "allowed_values": [10, 20, 30]}
        }
        
        result = YAMLMerger._deep_merge(base, override)
        
        self.assertEqual(result["validation"]["min"], 10)  # Overridden
        self.assertEqual(result["validation"]["max"], 100)  # Preserved
        self.assertTrue(result["validation"]["unique"])  # Preserved
        self.assertEqual(result["validation"]["allowed_values"], [10, 20, 30])  # Added
    
    def test_deep_merge_list_override(self):
        """Test that lists are completely overridden, not merged"""
        base = {
            "name": "VAR1",
            "allowed": ["A", "B", "C"]
        }
        override = {
            "name": "VAR1",
            "allowed": ["X", "Y"]
        }
        
        result = YAMLMerger._deep_merge(base, override)
        
        self.assertEqual(result["allowed"], ["X", "Y"])  # Completely replaced
    
    def test_merge_specs(self):
        """Test merging complete specifications"""
        base_spec = {
            "domain": "ADSL",
            "key": ["USUBJID"],
            "columns": [
                {"name": "USUBJID", "type": "str"},
                {"name": "AGE", "type": "int", "validation": {"min": 0}}
            ]
        }
        
        override_spec = {
            "key": ["STUDYID", "USUBJID"],  # Override key
            "columns": [
                {"name": "AGE", "validation": {"min": 18}},  # Override AGE
                {"name": "SEX", "type": "str"}  # Add SEX
            ]
        }
        
        result = YAMLMerger.merge_specs(base_spec, override_spec)
        
        self.assertEqual(result["domain"], "ADSL")  # Preserved
        self.assertEqual(result["key"], ["STUDYID", "USUBJID"])  # Overridden
        self.assertEqual(len(result["columns"]), 3)  # USUBJID, AGE, SEX
        
        # Check AGE was properly merged
        age_col = next(col for col in result["columns"] if col["name"] == "AGE")
        self.assertEqual(age_col["type"], "int")  # Preserved
        self.assertEqual(age_col["validation"]["min"], 18)  # Overridden
    
    def test_merge_empty_base(self):
        """Test merging with empty base spec"""
        base_spec = {}
        override_spec = {
            "domain": "ADAE",
            "key": ["USUBJID", "AESEQ"],
            "columns": [{"name": "AETERM", "type": "str"}]
        }
        
        result = YAMLMerger.merge_specs(base_spec, override_spec)
        
        self.assertEqual(result["domain"], "ADAE")
        self.assertEqual(result["key"], ["USUBJID", "AESEQ"])
        self.assertEqual(len(result["columns"]), 1)
    
    def test_merge_complex_derivation(self):
        """Test merging complex derivation rules"""
        base = [{
            "name": "BMI",
            "derivation": {
                "function": "calculate_bmi",
                "height": "HEIGHT",
                "weight": "WEIGHT"
            }
        }]
        
        override = [{
            "name": "BMI",
            "derivation": {
                "function": "custom_bmi_calc",
                "unit": "kg/m2"
            }
        }]
        
        result = YAMLMerger.merge_columns(base, override)
        
        bmi_col = result[0]
        self.assertEqual(bmi_col["derivation"]["function"], "custom_bmi_calc")  # Overridden
        self.assertEqual(bmi_col["derivation"]["height"], "HEIGHT")  # Preserved
        self.assertEqual(bmi_col["derivation"]["weight"], "WEIGHT")  # Preserved
        self.assertEqual(bmi_col["derivation"]["unit"], "kg/m2")  # Added


if __name__ == "__main__":
    unittest.main()