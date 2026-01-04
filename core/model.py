class Model:
    """Represents a trained model artifact produced by a run.
    
    A Model tracks metadata about a trained artifact, including its storage location,
    associated training run, lifecycle stage, and name. This enables traceability
    and safe model promotion across the ML lifecycle.
    
    Attributes:
        artifact_uri: The URI or path where the model artifact is stored (e.g., S3 URI, file path).
        model_name: The name of the model.
        associated_run_id: The identifier of the run that produced this model.
        lifecycle_stage: The current lifecycle stage of the model (registered, staging, production, or archived).
        ALLOWED_STAGES: Set of valid lifecycle stages for a model.
    """
    ALLOWED_STAGES = {"registered", "staging", "production", "archived"}
    
    def __init__(self, artifact_uri: str, model_name: str ,associated_run_id: str, lifecycle_stage: str):
        """Initialize a Model instance.
        
        Args:
            artifact_uri: The URI or path where the model artifact is stored (e.g., S3 URI, file path).
            model_name: The name of the model.
            associated_run_id: The identifier of the run that produced this model.
            lifecycle_stage: The current lifecycle stage of the model. Must be one of:
                           "registered", "staging", "production", or "archived".
            
        Raises:
            ValueError: If any required field is missing or lifecycle_stage is invalid.
        """
        self.artifact_uri = artifact_uri
        
        self.associated_run_id = associated_run_id
        self.lifecycle_stage = lifecycle_stage
        self.model_name = model_name
        
        self._enforce_required_fields()
    
    
    
    def _enforce_required_fields(self) -> None:
        """Validate that all required fields are present.
        
        Raises:
            ValueError: If any required field is missing or empty.
        """
        if not self.artifact_uri:
            raise ValueError("Artifact URI is required")
        if not self.associated_run_id:
            raise ValueError("Associated run is required")
        if not self.model_name:
            raise ValueError("Model name is required")
        if not self.lifecycle_stage or self.lifecycle_stage not in self.ALLOWED_STAGES:
            raise ValueError("Invalid lifecycle stage")
    
        
    def to_record(self) -> dict:
        """Convert the Model instance to a dictionary representation.
        
        Returns:
            dict: Dictionary containing all model metadata.
        """
        return {
            "artifact_uri": self.artifact_uri,
            "model_name": self.model_name,
            "associated_run_id": self.associated_run_id,
            "lifecycle_stage": self.lifecycle_stage
        }