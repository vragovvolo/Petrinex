"""
Simple tests for the process module.
"""

from petrinex.process import process_csvs_to_delta
from petrinex.config import PetrinexConfig, DatasetConfig, IngestConfig


class TestProcessModule:
    """Test that the process module is properly structured"""

    def test_process_function_exists(self):
        """Test that the process function can be imported"""
        assert callable(process_csvs_to_delta)

    def test_process_function_signature(self):
        """Test that the process function has the correct signature"""
        import inspect

        sig = inspect.signature(process_csvs_to_delta)
        expected_params = ["config", "dataset", "csv_directory", "spark"]
        actual_params = list(sig.parameters.keys())
        assert actual_params == expected_params

    def test_process_function_docstring(self):
        """Test that the process function has proper documentation"""
        assert process_csvs_to_delta.__doc__ is not None
        assert (
            "Process CSV files from download directory" in process_csvs_to_delta.__doc__
        )
