from .db import DatabaseConnection
from core.run import Run
from datetime import datetime, timezone
from typing import Optional, Tuple

class RunRepository:
    """Repository for managing runs (experiment executions) in the storage system.
    
    This repository provides methods to create, read, update, and delete runs
    from the persistent storage. It abstracts database operations and provides
    a clean interface for run management operations.
    
    Attributes:
        RUN_TABLE: The name of the table where runs are stored.
        db_connection: The database connection instance.
    """
    RUN_TABLE = "Run"
    
    def __init__(self, db_connection: DatabaseConnection):
        """Initialize a RunRepository instance.
        
        Args:
            db_connection: A DatabaseConnection instance for database operations.
        """
        self.db_connection = db_connection
    
    
    def add_run(self, run: Run) -> None:
        """Add a new run to the storage system.
        
        Converts the run to a record format and inserts it into the database.
        The start_time in the record is overwritten with the current timestamp.
        
        Args:
            run: The Run instance to add to storage.
            
        Raises:
            RuntimeError: If the database operation fails.
        """
        record = run.to_record()
        now = datetime.now(timezone.utc)
        now_naive_utc = now.replace(tzinfo=None)
        record["start_time"] = now_naive_utc
        self.db_connection.insert(self.RUN_TABLE, record)
    
    
    def get_run(self, run_name: str) -> Optional[Run]:
        """Retrieve a run by its name.
        
        Args:
            run_name: The name of the run to retrieve.
            
        Returns:
            Optional[Run]: The retrieved run or None if not found.
            
        Raises:
            ValueError: If run_name is empty or None.
            RuntimeError: If the database operation fails.
        """
        if not run_name:
            raise ValueError("Run name is required to retrieve a run")
        
        response = (
            self.db_connection
            .table(self.RUN_TABLE)
            .select("*")
            .eq("run_name", run_name)
            .execute()
        )
        if response.error:
            raise RuntimeError(f"Error retrieving run: {response.error.message}")
        if not response.data:
            return None
        record = response.data[0]
        return Run.from_record(record)
    
    
    
    def batch_get(self, run_names: list[str]) -> tuple[list[Run], int]:
        """Retrieve multiple runs by their names.
        
        Args:
            run_names: A list of run names to retrieve.
            
        Returns:
            tuple[list[Run], int]: A tuple containing:
                - List of retrieved Run instances (skips runs that cause errors)
                - Count of successfully retrieved runs
                
        Note:
            If a run retrieval fails with a RuntimeError, it is logged and skipped
            rather than failing the entire batch operation.
        """
        collected_runs = []
        if not run_names:
            return collected_runs, 0
        
        for run_name in run_names:
            try:
                record = self.get_run(run_name)
                if record:
                    collected_runs.append(record)
            except RuntimeError as e:
                # Log the error and just continue don't want to fail the whole batch
                continue
        return collected_runs, len(collected_runs)
    
    
    
    def update_run(self, run: Run) -> None:
        """Update an existing run in the storage system.
        
        Converts the run to a record format and updates it in the database
        based on the run_name.
        
        Args:
            run: The Run instance to update in storage.
            
        Raises:
            ValueError: If run_name is missing from the run.
            RuntimeError: If the database operation fails.
        """
        record = run.to_record()
        run_name = record.get("run_name")
        if not run_name:
            raise ValueError("Run name is required to update a run")
        
        self.db_connection.table(self.RUN_TABLE).update(record).eq("run_name", run_name).execute()
        
            
            
    
    def end_run(self, run_name: str) -> None:
        """Mark a run as ended by setting its end_time to the current timestamp.
        
        Args:
            run_name: The name of the run to end.
            
        Raises:
            ValueError: If run_name is empty, None, or the run has already been ended.
            RuntimeError: If the run is not found or the database operation fails.
        """
        if not run_name:
            raise ValueError("Run name is required to end a run")
        
      
        record = self.get_run(run_name)  
        end_time = record.end_time
        
        if end_time:
            raise ValueError("Run has already been ended")
        
        now = datetime.now(timezone.utc)
        now_naive_utc = now.replace(tzinfo=None)
        self.db_connection.table(self.RUN_TABLE).update({"end_time": now_naive_utc}).eq("run_name", run_name).execute()
        

            
        
    def batch_create(self, runs: list[Run]) -> None:
        """Add multiple runs to the storage system in a batch operation.
        
        Args:
            runs: A list of Run instances to add to storage.
                  Invalid entries (runs without run_name) are skipped.
                  
        Raises:
            RuntimeError: If any database operation fails.
        """
        for run in runs:
            if not run.run_name:
                continue
            else:
                self.add_run(run)
    
    
    def delete_run(self, run_name: str) -> int:
        """Delete a run by its name.
        
        Args:
            run_name: The name of the run to delete.
            
        Returns:
            int: The number of deleted records (0 or 1).
        """
        if not run_name:
            raise ValueError("Run name is required to delete a run")
        
        response = (
            self.db_connection
            .table(self.RUN_TABLE)
            .delete()
            .eq("run_name", run_name)
            .execute()
        )
        
        if response.error:
            raise RuntimeError(f"Error deleting run: {response.error.message}")
        
        return response.count if response.count else 0
                    