"""
Test cases for validation functionality
"""

import unittest
from ..models import ADaMSpec, Column, ValidationRule
from ..validator import SpecValidator


class TestSpecValidator(unittest.TestCase):
    """Test specification validation"""
    
    def test_validate_valid_spec(self):
        """Test validating a valid specification"""
        spec = ADaMSpec(
            domain="ADSL",
            key=["USUBJID"],
            columns=[
                Column(name="USUBJID", type="str", core="cdisc-required")
            ]
        )
        
        errors = SpecValidator.validate(spec)
        self.assertEqual(len(errors), 0)
    
    def test_validate_missing_domain(self):
        """Test validation with missing domain"""
        spec = ADaMSpec(
            domain="",
            key=["USUBJID"],
            columns=[Column(name="USUBJID", type="str")]
        )
        
        errors = SpecValidator.validate(spec)
        self.assertIn("Domain is required", errors)
    
    def test_validate_missing_key(self):
        """Test validation with missing key variables"""
        spec = ADaMSpec(
            domain="ADSL",
            key=[],
            columns=[Column(name="USUBJID", type="str")]
        )
        
        errors = SpecValidator.validate(spec)
        self.assertIn("Key variables must be specified", errors)
    
    def test_validate_duplicate_columns(self):
        """Test validation with duplicate column names"""
        spec = ADaMSpec(
            domain="ADSL",
            key=["USUBJID"],
            columns=[
                Column(name="VAR1", type="str"),
                Column(name="VAR1", type="int")  # Duplicate
            ]
        )
        
        errors = SpecValidator.validate(spec)
        self.assertTrue(any("Duplicate column name: VAR1" in e for e in errors))
    
    def test_validate_invalid_data_type(self):
        """Test validation with invalid data type"""
        spec = ADaMSpec(
            domain="ADSL",
            key=["USUBJID"],
            columns=[
                Column(name="USUBJID", type="str"),
                Column(name="VAR1", type="invalid_type")
            ]
        )
        
        errors = SpecValidator.validate(spec)
        self.assertTrue(any("Invalid data type" in e for e in errors))
    
    def test_validate_invalid_core_type(self):
        """Test validation with invalid core type"""
        spec = ADaMSpec(
            domain="ADSL",
            key=["USUBJID"],
            columns=[
                Column(name="USUBJID", type="str"),
                Column(name="VAR1", core="invalid_core")
            ]
        )
        
        errors = SpecValidator.validate(spec)
        self.assertTrue(any("Invalid core type" in e for e in errors))
    
    def test_validate_key_not_in_columns(self):
        """Test validation when key variable not in columns"""
        spec = ADaMSpec(
            domain="ADSL",
            key=["USUBJID", "NONEXISTENT"],
            columns=[
                Column(name="USUBJID", type="str")
            ]
        )
        
        errors = SpecValidator.validate(spec)
        self.assertTrue(any("NONEXISTENT not found in columns" in e for e in errors))
    
    def test_validate_invalid_missing_percentage(self):
        """Test validation with invalid missing percentage"""
        spec = ADaMSpec(
            domain="ADSL",
            key=["USUBJID"],
            columns=[
                Column(name="USUBJID", type="str"),
                Column(
                    name="VAR1",
                    validation=ValidationRule(maximum_missing_percentage=150)
                )
            ]
        )
        
        errors = SpecValidator.validate(spec)
        self.assertTrue(any("Invalid missing percentage" in e for e in errors))
    
    def test_validate_invalid_range(self):
        """Test validation with min > max"""
        spec = ADaMSpec(
            domain="ADSL",
            key=["USUBJID"],
            columns=[
                Column(name="USUBJID", type="str"),
                Column(
                    name="AGE",
                    type="int",
                    validation=ValidationRule(min=100, max=50)  # min > max
                )
            ]
        )
        
        errors = SpecValidator.validate(spec)
        self.assertTrue(any("min (100) > max (50)" in e for e in errors))
    
    def test_validate_invalid_length_range(self):
        """Test validation with invalid length range"""
        spec = ADaMSpec(
            domain="ADSL",
            key=["USUBJID"],
            columns=[
                Column(name="USUBJID", type="str"),
                Column(
                    name="VAR1",
                    type="str",
                    validation=ValidationRule(length={'min': 10, 'max': 5})
                )
            ]
        )
        
        errors = SpecValidator.validate(spec)
        self.assertTrue(any("Invalid length range" in e for e in errors))
    
    def test_validate_multiple_errors(self):
        """Test that multiple errors are collected"""
        spec = ADaMSpec(
            domain="",  # Missing domain
            key=[],  # Missing key
            columns=[
                Column(name="VAR1", type="invalid"),  # Invalid type
                Column(name="VAR1", type="str"),  # Duplicate
            ]
        )
        
        errors = SpecValidator.validate(spec)
        self.assertGreater(len(errors), 3)  # Should have multiple errors
    
    def test_validate_complex_valid_spec(self):
        """Test validating a complex but valid specification"""
        spec = ADaMSpec(
            domain="ADSL",
            key=["STUDYID", "USUBJID"],
            columns=[
                Column(
                    name="STUDYID",
                    type="str",
                    core="cdisc-required",
                    validation=ValidationRule(maximum_missing_percentage=0)
                ),
                Column(
                    name="USUBJID",
                    type="str",
                    core="cdisc-required",
                    validation=ValidationRule(unique=True, maximum_missing_percentage=0)
                ),
                Column(
                    name="AGE",
                    type="int",
                    core="cdisc-required",
                    validation=ValidationRule(min=18, max=80, maximum_missing_percentage=5)
                ),
                Column(
                    name="SEX",
                    type="str",
                    core="cdisc-required",
                    validation=ValidationRule(allowed_values=["M", "F", "U"])
                )
            ]
        )
        
        errors = SpecValidator.validate(spec)
        self.assertEqual(len(errors), 0)


if __name__ == "__main__":
    unittest.main()