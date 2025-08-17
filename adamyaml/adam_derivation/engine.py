"""
Main derivation engine for ADaM dataset generation using Polars
"""

import polars as pl
from pathlib import Path
from typing import Any
import logging

from .loaders import SDTMLoader
from .derivations import DerivationFactory
from .utils.logger import DerivationLogger
from ..adam_spec import AdamSpec
from ..adam_validation import DataValidator


class AdamDerivation:
    """
    Main engine for deriving ADaM datasets from SDTM data using YAML specifications
    """
    
    def __init__(self, spec_path: str):
        """
        Initialize the derivation engine
        
        Args:
            spec_path: Path to YAML specification file
        """
        self.spec_path = Path(spec_path)
        self.spec = AdamSpec(self.spec_path)
        
        # Check for validation errors
        if self.spec._errors:
            error_msg = "\n".join([f"  - {e}" for e in self.spec._errors])
            raise ValueError(f"Specification validation failed with {len(self.spec._errors)} errors:\n{error_msg}")
        
        # Initialize SDTM loader using sdtm_dir from spec
        self.sdtm_loader = SDTMLoader(self.spec.sdtm_dir)
        
        # Initialize logger
        self.logger = DerivationLogger(self.spec.domain)
        self.python_logger = logging.getLogger(__name__)
        
        # Initialize validator
        self.validator = DataValidator()
    
    def _build_keys(self) -> pl.DataFrame:
        """Build base dataset with key variables."""
        key_vars = self.spec.key
        self.python_logger.info(f"Building base dataset with key variables: {key_vars}")
        
        dependencies = self.spec.get_data_dependency()
        key_deps = [dep for dep in dependencies if dep['adam_variable'] in key_vars]
        
        source_dataset = key_deps[0]['sdtm_data']
        key_columns_map = {
            dep['adam_variable']: dep['sdtm_variable'] 
            for dep in key_deps
        }
        
        source_df = self.sdtm_loader.load_dataset(source_dataset)
        self.python_logger.info(f"Using source dataset {source_dataset}")
        
        columns_to_select = list(key_columns_map.values())
        base_df = source_df.select(columns_to_select)
        
        rename_map = {source_col: key_var 
                      for key_var, source_col in key_columns_map.items() 
                      if source_col != key_var}
        if rename_map:
            base_df = base_df.rename(rename_map)
            self.python_logger.info(f"Renamed columns: {rename_map}")
        
        # Check for duplicates
        n_rows = base_df.height
        n_unique = base_df.unique(subset=key_vars).height
        
        if n_rows != n_unique:
            n_duplicates = n_rows - n_unique
            self.python_logger.error(
                f"ERROR: Found {n_duplicates} duplicate key combinations. "
                f"Total: {n_rows}, Unique: {n_unique}"
            )
            self.logger.log_error(
                column=", ".join(key_vars),
                method="_build_keys",
                error=f"Duplicate key combinations: {n_duplicates} duplicates"
            )
            
            duplicated = base_df.filter(
                base_df.select(key_vars).is_duplicated()
            ).head(5)
            self.python_logger.error(f"Sample duplicates:\n{duplicated}")
            
            base_df = base_df.unique(subset=key_vars, keep="first")
            self.python_logger.warning(f"Continuing with {base_df.height} unique records")
        else:
            self.python_logger.info(f"Base dataset has {base_df.height} unique rows")
        
        return base_df
    
    def build(self) -> pl.DataFrame:
        """Build the ADaM dataset."""
        self.python_logger.info(f"Starting derivation for {self.spec.domain}")
        
        dependencies = self.spec.get_data_dependency()
        required_datasets = list(set(dep['sdtm_data'] for dep in dependencies))
        required_datasets = [ds for ds in required_datasets if ds != self.spec.domain]
        
        source_data = self.sdtm_loader.load_datasets(required_datasets)
        self.python_logger.info(f"Loaded {len(source_data)} source datasets: {list(source_data.keys())}")
        
        columns = self.spec.get_column_specs()
        
        # Build base dataset with key variables
        key_vars = self.spec.key
        if key_vars:
            target_df = self._build_keys()
            key_columns_derived = key_vars
        else:
            self.python_logger.warning("No key variables defined")
            target_df = pl.DataFrame()
            key_columns_derived = []
        
        for col_spec in columns:
            col_name = col_spec.get("name")
            
            if col_name in key_columns_derived:
                continue
            
            if col_spec.get("drop", False):
                self.python_logger.info(f"Skipping dropped column: {col_name}")
                continue
            
            try:
                derivation_obj = DerivationFactory.get_derivation(col_spec)
                self.python_logger.info(f"Deriving {col_name} using {derivation_obj.__class__.__name__}")
                derived_values = derivation_obj.derive(source_data, target_df, col_spec)
                
                if target_df.height == 0:
                    target_df = pl.DataFrame({col_name: derived_values})
                else:
                    if isinstance(derived_values, pl.Series):
                        if len(derived_values) != target_df.height:
                            self.python_logger.warning(
                                f"Column {col_name}: {len(derived_values)} values, target: {target_df.height} rows"
                            )
                            if len(derived_values) < target_df.height:
                                padding = [None] * (target_df.height - len(derived_values))
                                derived_values = pl.concat([derived_values, pl.Series(padding)])
                            else:
                                derived_values = derived_values[:target_df.height]
                    
                    target_df = target_df.with_columns(derived_values.alias(col_name))
                
                derivation_info = col_spec.get("derivation", {})
                source = derivation_info.get("source", derivation_info.get("constant", "custom"))
                self.logger.log_derivation(
                    column=col_name,
                    method=derivation_obj.__class__.__name__,
                    source=source,
                    records=target_df.height if target_df.height > 0 else len(derived_values)
                )
                
            except Exception as e:
                self.python_logger.error(f"Failed to derive {col_name}: {e}")
                self.logger.log_error(
                    column=col_name,
                    method="unknown",
                    error=str(e)
                )
                
                if target_df.height > 0:
                    target_df = target_df.with_columns(pl.lit(None).alias(col_name))
        
        self.python_logger.info("Validating derived dataset")
        validation_results = self.validator.validate_dataset(target_df, self.spec.to_dict())
        
        for result in validation_results:
            if result["status"] == "error":
                self.logger.log_error(
                    column=result.get("column", "dataset"),
                    method="validation",
                    error=result["message"]
                )
            elif result["status"] == "warning":
                self.python_logger.warning(result["message"])
        
        summary = self.logger.get_summary()
        self.python_logger.info(f"Derivation complete: {summary['columns_derived']} columns, {summary['errors']} errors")
        
        if self.logger.has_errors():
            self.python_logger.warning("Completed with errors")
        
        return target_df
    
    def save(self) -> Path:
        """Save dataset to parquet file."""
        df = self.build()
        
        adam_dir = Path(self.spec.adam_dir)
        adam_dir.mkdir(parents=True, exist_ok=True)
        
        output_path = adam_dir / f"{self.spec.domain.lower()}.parquet"
        df.write_parquet(output_path)
        self.python_logger.info(f"Saved to {output_path}")
        
        return output_path
    
    def get_derivation_log(self) -> dict[str, Any]:
        """Get derivation log summary."""
        return self.logger.get_summary()