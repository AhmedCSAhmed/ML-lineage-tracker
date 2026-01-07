from .db import DatabaseConnection
from core.dataset import Dataset
from typing import Optional, Tuple
from postgrest.exceptions import APIError

class DatasetRepository:
    """Repository for managing datasets in the storage system.
    
    This repository provides methods to create, read, and delete datasets
    from the persistent storage. It abstracts database operations and provides
    a clean interface for dataset management operations.
    
    Attributes:
        DATASET_TABLE: The name of the table where datasets are stored.
        db_connection: The database connection instance.
    """
    
    
    DATASET_TABLE = "Dataset"
    
    def __init__(self, db_connection: DatabaseConnection):
        """Initialize a DatasetRepository instance.
        
        Args:
            db_connection: A DatabaseConnection instance for database operations.
        """
        self.db_connection = db_connection
    
    def add_dataset(self, dataset: Dataset) -> None:
        """Add a new dataset to the storage system.
        
        Converts the dataset to a record format and inserts it into the database.
        
        Args:
            dataset: The Dataset instance to add to storage.
            
        Raises:
            RuntimeError: If the database operation fails.
        """
        record = dataset.to_record()
        self.db_connection.insert(self.DATASET_TABLE, record)
    
    
    def get_dataset(self, name: str, version: str) -> Optional[Dataset]:
        """Retrieve a dataset by name and version.

        Args:
            name: The name of the dataset.
            version: The version of the dataset.
            
        Returns:
            Optional[Dataset]: The retrieved dataset or None if not found.
            
        Raises:
            RuntimeError: If the database operation fails.
        """
        try:
            response = (
                self.db_connection
                .table(self.DATASET_TABLE)
                .select("*")
                .eq("name", name)
                .eq("version", version)
                .execute())
        except Exception as e:
            raise RuntimeError(f"Error retrieving dataset: {str(e)}")
            
        if not response.data:
            return None
                
        record = response.data[0]
        return Dataset.from_record(record)
    
    def get_dataset_id(self, name: str, version: str) -> Optional[str]:
        """Get the UUID of a dataset by name and version.
        
        This is useful when you need the dataset UUID for creating related entities
        (e.g., creating a Run that uses this dataset).
        
        Args:
            name: The name of the dataset.
            version: The version of the dataset.
            
        Returns:
            Optional[str]: The UUID of the dataset, or None if not found.
            
        Raises:
            RuntimeError: If the database operation fails.
        """
        try:
            response = (
                self.db_connection
                .table(self.DATASET_TABLE)
                .select("dataset_id")
                .eq("name", name)
                .eq("version", version)
                .execute())
        except APIError as e:
            raise RuntimeError(f"Error retrieving dataset ID: {e.message}")
            
        if not response.data:
            return None
            
        return response.data[0].get("dataset_id")
    
    
    def delete_dataset(self, name: str, version: str) -> int:
        """Delete a dataset by name and version.
        
        Args:
            name: The name of the dataset to delete.
            version: The version of the dataset to delete.
            
        Returns:
            int: The number of records deleted (typically 0 or 1).
            
        Raises:
            ValueError: If name or version is empty or None.
            RuntimeError: If the database operation fails.
        """
        if not name or not version:
            raise ValueError("Both name and version are required to delete a dataset.")
        
        try:
            response = (
                self.db_connection
                .table(self.DATASET_TABLE)
                .delete()
                .eq("name", name)
                .eq("version", version)
                .execute()
            )
        except Exception as e:
            raise RuntimeError(f"Error deleting dataset: {str(e)}")
        
        return len(response.data or [])
    
        
    
    
    def batch_get(self, names_versions: list[tuple[str, str]]) -> tuple[list[Dataset], int]:
        """Retrieve multiple datasets by their names and versions.
        
        Args:
            names_versions: A list of tuples containing (name, version) pairs.
            
        Returns:
            tuple[list[Dataset], int]: A tuple containing:
                - List of retrieved Dataset instances (empty entries are skipped)
                - Count of successfully retrieved datasets
                
        Raises:
            RuntimeError: If any database operation fails.
        """
        datasets = []
        for name, version in names_versions:
            if not name or not version:
                # Skip invalid entries
                continue
            dataset = self.get_dataset(name, version)
            if dataset:
                datasets.append(dataset)
        
        return (datasets, len(datasets))
    
    
    
    
    def batch_create(self, datasets: list[Dataset]) -> None:
        """Add multiple datasets to the storage system in a batch operation.
        
        Args:
            datasets: A list of Dataset instances to add to storage.
            
        Raises:
            RuntimeError: If any database operation fails.
        """
        for dataset in datasets:
            self.add_dataset(dataset)
        

    
            
    def batch_delete(self, name_versions: list[tuple[str, str]]) -> int:
        """Delete multiple datasets by their names and versions in a batch operation.
        
        Args:
            name_versions: A list of tuples containing (name, version) pairs.
                          Invalid entries (empty name or version) are skipped.
            
        Returns:
            int: Total number of datasets deleted across all operations.
            
        Raises:
            RuntimeError: If any database operation fails.
        """
        total_deleted_datasets = 0
        for name, version in name_versions:
            if not name or not version:
                # Skip invalid entries
                continue
            deleted_count = self.delete_dataset(name, version)
            total_deleted_datasets += deleted_count
            
        return total_deleted_datasets
