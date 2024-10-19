import unittest
import sys
import os

# Set the PYTHONPATH to include the src directory
sys.path.insert(0, os.path.abspath("src"))

if __name__ == "__main__":
    # Discover tests in the 'tests' directory
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover('tests')

    # Run the tests
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
