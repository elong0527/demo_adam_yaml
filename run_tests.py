#!/usr/bin/env python3
"""
Run all unit tests for adam_yaml module
"""

import sys
import unittest
from pathlib import Path

# Add src to path
sys.path.insert(0, 'src')

# Import test modules
from adam_yaml.tests import test_adam_spec, test_merge_yaml, test_schema_validator

# Create test suite
loader = unittest.TestLoader()
suite = unittest.TestSuite()

# Add tests
suite.addTests(loader.loadTestsFromModule(test_adam_spec))
suite.addTests(loader.loadTestsFromModule(test_merge_yaml))
suite.addTests(loader.loadTestsFromModule(test_schema_validator))

# Run tests
if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    if result.wasSuccessful():
        print("\n[SUCCESS] All tests passed!")
    else:
        print("\n[FAILURE] Some tests failed")
        sys.exit(1)