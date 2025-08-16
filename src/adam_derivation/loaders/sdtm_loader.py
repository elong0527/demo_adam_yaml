"""
SDTM Data Loader

Loads SDTM datasets from parquet files with caching and lazy loading support.
"""

import polars as pl
from pathlib import Path
from typing import Dict, Optional
import logging


class SDTMLoader:
    """Load and cache SDTM datasets"""
    
    def __init__(self, sdtm_dir: str):
        """
        Initialize SDTM loader
        
        Args:
            sdtm_dir: Directory containing SDTM parquet files
        """
        self.sdtm_dir = Path(sdtm_dir)
        if not self.sdtm_dir.exists():
            raise FileNotFoundError(f"SDTM directory not found: {sdtm_dir}")
        
        self._cache: Dict[str, pl.DataFrame] = {}
        self.logger = logging.getLogger(__name__)
    
    def load_dataset(self, dataset_name: str) -> pl.DataFrame:
        """
        Load a single SDTM dataset with caching
        
        Args:
            dataset_name: Name of dataset (e.g., 'DM', 'VS', 'EX')
        
        Returns:
            DataFrame containing the dataset
        """
        dataset_name = dataset_name.upper()
        
        # Return from cache if available
        if dataset_name in self._cache:
            self.logger.debug(f"Returning {dataset_name} from cache")
            return self._cache[dataset_name]
        
        # Load from file
        file_path = self.sdtm_dir / f"{dataset_name.lower()}.parquet"
        if not file_path.exists():
            raise FileNotFoundError(f"SDTM dataset not found: {file_path}")
        
        self.logger.info(f"Loading {dataset_name} from {file_path}")
        df = pl.read_parquet(file_path)
        
        # Cache the dataset
        self._cache[dataset_name] = df
        
        return df
    
    def load_datasets(self, dataset_names: list) -> Dict[str, pl.DataFrame]:
        """
        Load multiple SDTM datasets
        
        Args:
            dataset_names: List of dataset names
        
        Returns:
            Dictionary mapping dataset names to DataFrames
        """
        datasets = {}
        for name in dataset_names:
            try:
                datasets[name.upper()] = self.load_dataset(name)
            except FileNotFoundError as e:
                self.logger.warning(f"Could not load {name}: {e}")
        
        return datasets
    
    def get_required_datasets(self, spec: dict) -> Dict[str, pl.DataFrame]:
        """
        Load all datasets required by a specification
        
        Args:
            spec: ADaM specification dictionary
        
        Returns:
            Dictionary of required datasets
        """
        required = set()
        
        # Parse columns for source datasets
        for column in spec.get("columns", []):
            derivation = column.get("derivation", {})
            
            # Check source field
            source = derivation.get("source", "")
            if "." in source:
                dataset, _ = source.split(".", 1)
                required.add(dataset)
            
            # Check filter field
            filter_expr = derivation.get("filter", "")
            for part in filter_expr.split():
                if "." in part:
                    dataset = part.split(".")[0]
                    if dataset.upper() in ["DM", "VS", "EX", "AE", "LB", "CM", "MH", "DS", "SC", "QS"]:
                        required.add(dataset)
            
            # Check aggregation target
            agg = derivation.get("aggregation", {})
            target = agg.get("target", "")
            if "." in target:
                dataset, _ = target.split(".", 1)
                required.add(dataset)
            
            # Check condition expressions
            conditions = derivation.get("condition", [])
            for cond in conditions:
                when_expr = cond.get("when", "")
                for part in when_expr.split():
                    if "." in part:
                        dataset = part.split(".")[0]
                        if dataset.upper() in ["DM", "VS", "EX", "AE", "LB", "CM", "MH", "DS", "SC", "QS"]:
                            required.add(dataset)
        
        self.logger.info(f"Required datasets: {required}")
        return self.load_datasets(list(required))
    
    def clear_cache(self):
        """Clear the dataset cache"""
        self._cache.clear()
        self.logger.debug("Cleared SDTM cache")