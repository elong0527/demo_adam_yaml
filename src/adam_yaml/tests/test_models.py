"""
Test cases for data models
"""

import unittest
from ..models import Column, Derivation, ValidationRule, ADaMSpec


class TestColumn(unittest.TestCase):
    """Test Column dataclass"""
    
    def test_column_creation(self):
        """Test creating a Column object"""
        col = Column(
            name="USUBJID",
            label="Unique Subject Identifier",
            type="str",
            core="cdisc-required"
        )
        
        self.assertEqual(col.name, "USUBJID")
        self.assertEqual(col.label, "Unique Subject Identifier")
        self.assertEqual(col.type, "str")
        self.assertEqual(col.core, "cdisc-required")
    
    def test_column_from_dict(self):
        """Test creating Column from dictionary"""
        data = {
            "name": "AGE",
            "type": "int",
            "derivation": {"source": "DM.AGE"},
            "validation": {"min": 0, "max": 120}
        }
        
        col = Column.from_dict(data)
        
        self.assertEqual(col.name, "AGE")
        self.assertEqual(col.type, "int")
        self.assertIsNotNone(col.derivation)
        self.assertEqual(col.derivation.source, "DM.AGE")
        self.assertIsNotNone(col.validation)
        self.assertEqual(col.validation.min, 0)
        self.assertEqual(col.validation.max, 120)
    
    def test_column_to_dict(self):
        """Test converting Column to dictionary"""
        col = Column(
            name="WEIGHT",
            type="float",
            derivation=Derivation(source="VS.VSORRES", filter="VS.TESTCD == 'WEIGHT'"),
            validation=ValidationRule(min=0, max=500)
        )
        
        result = col.to_dict()
        
        self.assertEqual(result["name"], "WEIGHT")
        self.assertEqual(result["type"], "float")
        self.assertIn("derivation", result)
        self.assertEqual(result["derivation"]["source"], "VS.VSORRES")
        self.assertIn("validation", result)
        self.assertEqual(result["validation"]["min"], 0)
    
    def test_column_with_drop_flag(self):
        """Test column with drop flag"""
        col = Column(name="TEMP", drop=True)
        result = col.to_dict()
        
        self.assertTrue(result.get("drop"))


class TestValidationRule(unittest.TestCase):
    """Test ValidationRule dataclass"""
    
    def test_validation_rule_creation(self):
        """Test creating validation rules"""
        rule = ValidationRule(
            maximum_missing_percentage=5,
            allowed_values=["A", "B", "C"],
            min=0,
            max=100
        )
        
        self.assertEqual(rule.maximum_missing_percentage, 5)
        self.assertEqual(rule.allowed_values, ["A", "B", "C"])
        self.assertEqual(rule.min, 0)
        self.assertEqual(rule.max, 100)
    
    def test_validation_rule_to_dict(self):
        """Test converting to dict excludes None values"""
        rule = ValidationRule(min=0, max=100)
        result = rule.to_dict()
        
        self.assertIn("min", result)
        self.assertIn("max", result)
        self.assertNotIn("unique", result)  # Should be excluded as it's None
        self.assertNotIn("pattern", result)  # Should be excluded as it's None


class TestDerivation(unittest.TestCase):
    """Test Derivation dataclass"""
    
    def test_derivation_creation(self):
        """Test creating derivation rules"""
        deriv = Derivation(
            source="DM.AGE",
            filter="AGE >= 18"
        )
        
        self.assertEqual(deriv.source, "DM.AGE")
        self.assertEqual(deriv.filter, "AGE >= 18")
    
    def test_derivation_constant(self):
        """Test derivation with constant value"""
        deriv = Derivation(constant="ADSL")
        result = deriv.to_dict()
        
        self.assertEqual(result["constant"], "ADSL")
        self.assertNotIn("source", result)  # Should be excluded as it's None
    
    def test_derivation_function(self):
        """Test derivation with function"""
        deriv = Derivation(
            function="calculate_bmi",
            height="HEIGHT",
            weight="WEIGHT"
        )
        result = deriv.to_dict()
        
        self.assertEqual(result["function"], "calculate_bmi")
        self.assertEqual(result["height"], "HEIGHT")
        self.assertEqual(result["weight"], "WEIGHT")


class TestADaMSpec(unittest.TestCase):
    """Test ADaMSpec dataclass"""
    
    def test_spec_creation(self):
        """Test creating an ADaMSpec object"""
        spec = ADaMSpec(
            domain="ADSL",
            key=["STUDYID", "USUBJID"],
            columns=[
                Column(name="STUDYID", type="str"),
                Column(name="USUBJID", type="str")
            ]
        )
        
        self.assertEqual(spec.domain, "ADSL")
        self.assertEqual(len(spec.key), 2)
        self.assertEqual(len(spec.columns), 2)
    
    def test_spec_to_dict(self):
        """Test converting ADaMSpec to dictionary"""
        spec = ADaMSpec(
            domain="ADAE",
            key=["USUBJID", "AESEQ"],
            columns=[
                Column(name="USUBJID", type="str"),
                Column(name="AESEQ", type="int")
            ]
        )
        
        result = spec.to_dict()
        
        self.assertEqual(result["domain"], "ADAE")
        self.assertEqual(result["key"], ["USUBJID", "AESEQ"])
        self.assertEqual(len(result["columns"]), 2)
    
    def test_spec_default_values(self):
        """Test default values for ADaMSpec"""
        spec = ADaMSpec()
        
        self.assertEqual(spec.domain, "ADSL")
        self.assertEqual(spec.key, [])
        self.assertEqual(spec.columns, [])
        self.assertEqual(spec.config, [])


if __name__ == "__main__":
    unittest.main()