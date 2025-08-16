"""
Main derivation engine for ADaM dataset generation using Polars
"""

import polars as pl
from pathlib import Path
from typing import Dict, Any, Optional
import logging

from .loaders import SDTMLoader, SpecLoader
from .derivations import DerivationFactory
from .utils.logger import DerivationLogger

# Import from separate adam_validation module
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from adam_validation import DataValidator


class AdamDerivation:
    """
    Main engine for deriving ADaM datasets from SDTM data using YAML specifications
    """
    
    def __init__(self, spec_path: str, sdtm_dir: str):
        """
        Initialize the derivation engine
        
        Args:
            spec_path: Path to YAML specification file
            sdtm_dir: Directory containing SDTM parquet files
        """
        self.spec_path = Path(spec_path)
        self.sdtm_dir = Path(sdtm_dir)
        
        # Initialize components
        self.spec_loader = SpecLoader(str(self.spec_path))
        self.sdtm_loader = SDTMLoader(str(self.sdtm_dir))
        
        # Load specification
        self.spec = self.spec_loader.load_spec()
        self.domain = self.spec.get("domain", "UNKNOWN")
        
        # Initialize logger
        self.logger = DerivationLogger(self.domain)
        self.python_logger = logging.getLogger(__name__)
        
        # Initialize validator
        self.validator = DataValidator()
    
    def derive_dataset(self) -> pl.DataFrame:
        """
        Main method to derive the ADaM dataset
        
        Returns:
            DataFrame containing the derived ADaM dataset
        """
        self.python_logger.info(f"Starting derivation for {self.domain}")
        
        # Load required SDTM datasets
        source_data = self.sdtm_loader.get_required_datasets(self.spec)
        self.python_logger.info(f"Loaded {len(source_data)} source datasets")
        
        # Initialize target dataset as empty DataFrame
        target_df = pl.DataFrame()
        
        # Get column specifications
        columns = self.spec.get("columns", [])
        
        # Process each column
        for col_spec in columns:
            col_name = col_spec.get("name")
            
            # Skip dropped columns
            if col_spec.get("drop", False):
                self.python_logger.info(f"Skipping dropped column: {col_name}")
                continue
            
            try:
                # Get appropriate derivation class
                derivation_obj = DerivationFactory.get_derivation(col_spec)
                
                # Derive the column
                self.python_logger.info(f"Deriving {col_name} using {derivation_obj.__class__.__name__}")
                derived_values = derivation_obj.derive(source_data, target_df, col_spec)
                
                # Add to target dataset
                if target_df.height == 0:
                    # First column - initialize DataFrame
                    target_df = pl.DataFrame({col_name: derived_values})
                else:
                    # Add column to existing DataFrame
                    target_df = target_df.with_columns(derived_values.alias(col_name))
                
                # Log successful derivation
                derivation_info = col_spec.get("derivation", {})
                source = derivation_info.get("source", derivation_info.get("constant", "custom"))
                self.logger.log_derivation(
                    column=col_name,
                    method=derivation_obj.__class__.__name__,
                    source=source,
                    records=len(derived_values)
                )
                
            except Exception as e:
                # Log error but continue
                self.python_logger.error(f"Failed to derive {col_name}: {e}")
                self.logger.log_error(
                    column=col_name,
                    method="unknown",
                    error=str(e)
                )
                
                # Add empty column to maintain structure
                if target_df.height > 0:
                    target_df = target_df.with_columns(pl.lit(None).alias(col_name))
        
        # Validate the dataset
        self.python_logger.info("Validating derived dataset")
        validation_results = self.validator.validate_dataset(target_df, self.spec)
        
        for result in validation_results:
            if result["status"] == "error":
                self.logger.log_error(
                    column=result.get("column", "dataset"),
                    method="validation",
                    error=result["message"]
                )
            elif result["status"] == "warning":
                self.python_logger.warning(result["message"])
        
        # Report summary
        summary = self.logger.get_summary()
        self.python_logger.info(f"Derivation complete: {summary['columns_derived']} columns derived, {summary['errors']} errors")
        
        if self.logger.has_errors():
            self.python_logger.warning("Derivation completed with errors. Check logs for details.")
        
        return target_df
    
    def save_dataset(self, output_path: str, df: Optional[pl.DataFrame] = None):
        """
        Save the derived dataset to parquet file
        
        Args:
            output_path: Path to save the dataset
            df: DataFrame to save (if None, derive first)
        """
        if df is None:
            df = self.derive_dataset()
        
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        df.write_parquet(output_path)
        self.python_logger.info(f"Dataset saved to {output_path}")
    
    def get_derivation_log(self) -> Dict[str, Any]:
        """
        Get the derivation log summary
        
        Returns:
            Dictionary containing derivation log
        """
        return self.logger.get_summary()