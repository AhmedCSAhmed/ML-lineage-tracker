from .db import DatabaseConnection
from core.dataset import Dataset
from typing import Optional 

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
        response = (
            self.db_connection
            .table(self.DATASET_TABLE)
            .select("*")
            .eq("name", name)
            .eq("version", version)
            .execute())
            
        if response.error:
            raise RuntimeError(f"Error retrieving dataset: {response.error.message}")
            
        if  not response.data:
            return None
                
        record = response.data[0]
        return Dataset.from_record(record)
    
    
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
        
        response = (
            self.db_connection
            .table(self.DATASET_TABLE)
            .delete()
            .eq("name", name)
            .eq("version", version)
            .execute()
            )
        
        if response.error:
            raise RuntimeError(f"Error deleting dataset: {response.error.message}")
        
        return len(response.data or [])
    
        
        
        
       

            
            
            
