from typing import Optional
from utils.identity import Identity
from utils.hashing import Hashing

class Dataset:
    """Represents a versioned dataset used for training models.
    
    A Dataset tracks metadata about a collection of data, including its name,
    version, source location, creator (actor), and optional description. This
    enables traceability and reproducibility in the ML lineage tracking system.
    
    Attributes:
        name: The name of the dataset.
        version: The version identifier of the dataset.
        source: The source or path where the dataset is located.
        actor: The identity of who created or registered this dataset.
        description: Optional description of the dataset.
    """
        
    def __init__(self, name: str, version: str, source: str, actor: str, description: str | Optional[str] = None) -> None:
        """Initialize a Dataset instance.
        
        Args:
            name: The name of the dataset.
            version: The version identifier of the dataset.
            source: The source or path where the dataset is located (e.g., S3 URI, file path).
            actor: The identity of who created or registered this dataset.
            description: Optional description of the dataset.
            
        Raises:
            ValueError: If any required field (name, version, source, actor) is missing.
        """ 
        self.name = name
        self.version = version
        self.source = source
        self.actor = actor 
        self.description = description
        self._enforce_required_fields()
        
        
    def _enforce_required_fields(self) -> None:
        """Validate that all required fields are present.
        
        Raises:
            ValueError: If any required field is missing or empty.
        """
        if not self.name:
            raise ValueError("Name is required")
        if not self.version:
            raise ValueError("Version is required")
        if not self.source:
            raise ValueError("Source is required")
        if not self.actor:
            raise ValueError("Actor is required")
        
        
    def to_record(self) -> dict:
        """Convert the dataset to a dictionary record for storage.
        
        Returns:
            dict: A dictionary containing all dataset fields suitable for
                  persistence in the metadata store.
        """
        return {
            "name": self.name,
            "version": self.version,
            "source": self.source,
            "actor": self.actor,
            "description": self.description
        }
    
    
    @classmethod
    def from_record(cls, record: dict) -> "Dataset":
        """Create a Dataset instance from a dictionary record.
        
        Args:
            record: Dictionary containing dataset fields.
            
        Returns:
            Dataset: A Dataset instance created from the record.
        """
        return cls(
            name=record["name"],
            version=record["version"],
            source=record["source"],
            actor=record["actor"],
            description=record.get("description")
        )
