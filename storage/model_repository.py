from .db import DatabaseConnection
from core.model import Model 
from typing import Optional, Tuple

class ModelRepository:
    """Repository for managing models in the storage system.
    
    This repository provides methods to create, read, update, and delete models
    from the persistent storage. It abstracts database operations and provides
    a clean interface for model management operations.
    
    Attributes:
        MODEL_TABLE: The name of the table where models are stored.
        db_connection: The database connection instance.
    """
    MODEL_TABLE = "Model"
    
    def __init__(self, db_connection: DatabaseConnection):
        """Initialize a ModelRepository instance.
        
        Args:
            db_connection: A DatabaseConnection instance for database operations.
        """
        self.db_connection = db_connection
        
        
    
    def add_model(self, model: Model) -> None:
        """Add a new model to the storage system.
        
        Converts the model to a record format and inserts it into the database.
        
        Args:
            model: The Model instance to add to storage.
            
        Raises:
            RuntimeError: If the database operation fails.
        """
        record = model.to_record()
        try:
            self.db_connection.insert(self.MODEL_TABLE, record)
        except APIError as e:
            raise RuntimeError(f"Failed to add model: {e.message}")
        
    
    def get_model(self, model_name: str) -> Optional[Model]:
        """Retrieve a model by its name.
        
        Args:
            model_name: The name of the model to retrieve.
            
        Returns:
            Optional[Model]: The retrieved model or None if not found.
            
        Raises:
            ValueError: If model_name is empty or None.
            RuntimeError: If the database operation fails.
        """
        if not model_name:
            raise ValueError("Model name is required")
        try:
            response = self.db_connection.table(self.MODEL_TABLE).select("*").eq("model_name", model_name).execute()
        except Exception as e:
            raise RuntimeError(f"Failed to retrieve model: {str(e)}")
        
        if not response.data:
            return None
        
        model_data = response.data[0]
        
        record = Model.from_record(model_data)
        
        return record
    
    
    def update_model(self, model: Model) -> None:
        """Update an existing model in the storage system.
        
        Converts the model to a record format and updates it in the database
        based on the model_name.
        
        Args:
            model: The Model instance to update in storage.
            
        Raises:
            ValueError: If model_name is missing from the model.
            RuntimeError: If the model is not found or the database operation fails.
        """
        if not model.model_name:
            raise ValueError("Model is required for update")
        record = model.to_record()
        model_name = model.model_name
        try:
            response = (
                self.db_connection
                .table(self.MODEL_TABLE)
                .update(record)
                .eq("model_name", model_name)
                .execute() 
            )
        except Exception as e:
            raise RuntimeError(f"Failed to update model: {str(e)}")
        
        if not response.data:
            raise RuntimeError("No model found to update")
        
    def delete_model(self, model_name: str) -> None:
        """Delete a model by its name.
        
        Args:
            model_name: The name of the model to delete.
            
        Raises:
            ValueError: If model_name is empty or None.
            RuntimeError: If the model is not found or the database operation fails.
        """
        if not model_name:
            raise ValueError("Model name is required for deletion")
        
        try:
            response = (
                self.db_connection
                .table(self.MODEL_TABLE)
                .delete()
                .eq("model_name", model_name)
                .execute()
            )
        except Exception as e:
            raise RuntimeError(f"Failed to delete model: {str(e)}")
        if not response.data:
            raise RuntimeError("No model found to delete")
    
    def get_artifact_uri(self, model_name: str) -> Optional[str]:
        """Get the artifact URI for a model by its name.
        
        Args:
            model_name: The name of the model.
            
        Returns:
            Optional[str]: The artifact URI of the model, or None if the model is not found.
            
        Raises:
            ValueError: If model_name is empty or None.
            RuntimeError: If the database operation fails.
        """
        model = self.get_model(model_name)
        if model is None:
            return None
        return model.artifact_uri
    
    def list_models(self) -> list[Model]:
        """List all models in the storage system.
        
        Returns:
            list[Model]: A list of all Model instances in the database.
            
        Raises:
            RuntimeError: If the database operation fails.
        """
        try:
            response = self.db_connection.table(self.MODEL_TABLE).select("*").execute()
        except Exception as e:
            raise RuntimeError(f"Failed to list models: {str(e)}")
        
        models = [Model.from_record(record) for record in response.data]
        return models 
    
    def batch_get(self, model_names: list[str]) -> list[Model]:
        """Retrieve multiple models by their names.
        
        Args:
            model_names: A list of model names to retrieve.
                        Invalid entries (empty names or non-strings) are skipped.
            
        Returns:
            list[Model]: List of retrieved Model instances (skips models that are not found).
            
        Raises:
            ValueError: If any model name is not a string.
            RuntimeError: If any database operation fails.
        """
        if not model_names:
            return []
        models = []
        for name in model_names:
            if not name:
                continue
            if not isinstance(name, str):
                raise ValueError("Model names must be strings")
            record = self.get_model(name)
            if record is None:
                continue
            models.append(record)
        
        return models
        
    def model_exists(self, model_name: str) -> bool:
        """Check if a model exists in the storage system.
        
        Args:
            model_name: The name of the model to check.
            
        Returns:
            bool: True if the model exists, False otherwise.
            
        Raises:
            ValueError: If model_name is empty or None.
            RuntimeError: If the database operation fails.
        """
        model = self.get_model(model_name)
        return model is not None
                        