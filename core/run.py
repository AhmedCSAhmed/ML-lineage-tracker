from typing import Optional
from datetime import datetime
class Run:
    """Represents a single execution of a training attempt (experiment run).
    
    A Run tracks metadata about a training execution, including which dataset
    was used, hyperparameters, metrics, timing information, and optional code
    references. This enables reproducibility and traceability of experiments
    in the ML lineage tracking system.
    
    Attributes:
        dataset_id: The identifier of the dataset used for this training run.
        actor: The identity of who executed this run.
        parameters: Dictionary of hyperparameters and configuration used.
        code_reference: Optional reference to the code version (e.g., git commit hash).
        metrics: Dictionary of metrics recorded during the run (defaults to empty dict).
        start_time: Timestamp when the run started.
        end_time: Optional timestamp when the run ended.
    """
    def __init__(self, dataset_id: str, run_name: str, actor: str, parameters: dict, start_time: datetime, code_reference: str | None = None, metrics: dict | None = None, end_time: datetime | None = None) -> None:
        """Initialize a Run instance.
        
        Args:
            dataset_id: The identifier of the dataset used for this training run.
            run_name: The name of this run.
            actor: The identity of who executed this run.
            parameters: Dictionary of hyperparameters and configuration parameters.
            start_time: Timestamp when the run started.
            code_reference: Optional reference to the code version (e.g., git commit hash).
            metrics: Optional dictionary of metrics recorded during the run.
                     If None, defaults to an empty dictionary.
            end_time: Optional timestamp when the run ended.
            
        Raises:
            ValueError: If required fields are missing, parameters is not a dict,
                       or end_time is before start_time.
        """
        self.dataset_id = dataset_id
        self.actor = actor
        self.parameters = parameters
        self.code_reference = code_reference
        self.run_name = run_name
        if metrics is None:
            self.metrics = {}
        else:
            self.metrics = metrics
        self.start_time = start_time
        self.end_time = end_time
        self._enforce_required_fields()
        
        
    def _enforce_required_fields(self) -> None:
        """Validate that all required fields are present and valid.
        
        Raises:
            ValueError: If any required field is missing, parameters is not a dict,
                       or end_time is before start_time.
        """
        if not self.dataset_id:
            raise ValueError("Dataset is required")
        if not self.actor:
            raise ValueError("Actor is required")
        if not self.run_name:
            raise ValueError("Run name is required")
        if self.parameters is None or not isinstance(self.parameters, dict):
            raise ValueError("Parameters are required")
        if not self.start_time:
            raise ValueError("Start time is required")
        if self.end_time and self.end_time < self.start_time:
            raise ValueError("End time must be greater than start time")
    
    
    def to_record(self) -> dict:
        """Convert the Run instance to a dictionary representation.
        
        Returns:
            dict: Dictionary containing all run metadata.
        """
        return {
            "dataset_id": self.dataset_id,
            "run_name": self.run_name,
            "actor": self.actor,
            "parameters": self.parameters,
            "code_reference": self.code_reference,
            "metrics": self.metrics,
            "start_time": self.start_time.timestamp(),
            "end_time": self.end_time.timestamp() if self.end_time else None
        }
    
    
    @classmethod
    def from_record(cls, record: dict) -> "Run":
        """Create a Run instance from a dictionary record.
        
        Args:
            record: Dictionary containing run fields.
            
        Returns:
            Run: A Run instance created from the record.
        """
        return cls(
            dataset_id=record["dataset_id"],
            run_name=record["run_name"],
            actor=record["actor"],
            parameters=record["parameters"],
            start_time=record["start_time"],
            code_reference=record.get("code_reference"),
            metrics=record.get("metrics"),
            end_time=record.get("end_time")
        )
