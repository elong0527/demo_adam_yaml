import polars as pl
from pathlib import Path
import logging


class SDTMLoader:
    """Load and cache SDTM datasets."""
    
    def __init__(self, sdtm_dir: str):
        """
        Initialize SDTM loader.
        
        Args:
            sdtm_dir: Directory containing SDTM parquet files
        """
        self.sdtm_dir = Path(sdtm_dir)
        if not self.sdtm_dir.exists():
            raise FileNotFoundError(f"SDTM directory not found: {sdtm_dir}")
        
        self._cache: dict[str, pl.DataFrame] = {}
        self.logger = logging.getLogger(__name__)
    
    def load_dataset(self, dataset_name: str) -> pl.DataFrame:
        """
        Load a single SDTM dataset with caching.
        
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
    
    def load_datasets(self, dataset_names: list[str]) -> dict[str, pl.DataFrame]:
        """
        Load multiple SDTM datasets.
        
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
    
    def clear_cache(self):
        """Clear the dataset cache."""
        self._cache.clear()
        self.logger.debug("Cleared SDTM cache")