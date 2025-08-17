import polars as pl
from pathlib import Path
from typing import TYPE_CHECKING
import logging

# Avoid circular import at runtime
if TYPE_CHECKING:
    from ...adam_spec import AdamSpec


class SDTMLoader:
    """Load and cache SDTM datasets"""
    
    def __init__(self, sdtm_dir: str, spec: 'AdamSpec'):
        """
        Initialize SDTM loader
        
        Args:
            sdtm_dir: Directory containing SDTM parquet files
            spec: AdamSpec instance containing the specification
        """
        self.sdtm_dir = Path(sdtm_dir)
        if not self.sdtm_dir.exists():
            raise FileNotFoundError(f"SDTM directory not found: {sdtm_dir}")
        
        self.spec = spec
        self._cache: dict[str, pl.DataFrame] = {}
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
    
    def load_datasets(self, dataset_names: list) -> dict[str, pl.DataFrame]:
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
    
    def get_required_datasets(self) -> dict[str, pl.DataFrame]:
        """Load all datasets required by the specification."""
        # Use AdamSpec's get_data_dependency method to get XX.YYYY patterns
        dependencies = self.spec.get_data_dependency()
        
        # Extract unique dataset names
        required = set()
        for dep in dependencies:
            dataset = dep['sdtm_data']
            # Filter out current dataset references
            if dataset != self.spec.domain:
                required.add(dataset)
        
        self.logger.info(f"Required datasets: {required}")
        return self.load_datasets(list(required))
    
    def clear_cache(self):
        """Clear the dataset cache"""
        self._cache.clear()
        self.logger.debug("Cleared SDTM cache")