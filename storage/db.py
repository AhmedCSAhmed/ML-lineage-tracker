import os
from supabase import create_client, Client
from postgrest.exceptions import APIError

SUPABASE_URL: str | None = os.environ.get("ML_LINEAGE_SUPABASE_URL")
SUPABASE_KEY: str | None = os.environ.get("ML_LINEAGE_SUPABASE_KEY")


class DatabaseConnection:
    """Wrapper around Supabase client for database operations.
    
    This class provides a simplified interface for database operations,
    abstracting the Supabase client implementation.
    
    Attributes:
        client: The underlying Supabase client instance.
    """
    
    def __init__(self, client: Client):
        """Initialize DatabaseConnection with a Supabase client.
        
        Args:
            client: A Supabase Client instance.
        """
        self.client = client
    
    def insert(self, table: str, record: dict) -> dict:
        """Insert a record into the specified table.
        
        Args:
            table: The name of the table to insert into.
            record: Dictionary containing the record data.
        
        Returns:
            dict: The inserted record with generated fields (e.g., UUIDs).
        
        Raises:
            RuntimeError: If the database operation fails.
        """
        try:
            response = self.client.table(table).insert(record).execute()
            if response.data and len(response.data) > 0:
                return response.data[0]
            return record
        except APIError as e:
            raise RuntimeError(f"Failed to insert into {table}: {e.message}")
    
    def table(self, table: str):
        """Get a query builder for the specified table.
        
        Args:
            table: The name of the table.
            
        Returns:
            The Supabase table query builder.
        """
        return self.client.table(table)


def connect_to_supabase() -> Client:
    """Establish a connection to the database.

    Returns:
        Client: An instance of the Supabase client connected to the database.
    """
    if not SUPABASE_URL or not SUPABASE_KEY: # fail fast if env vars are not set
        raise RuntimeError("Supabase credentials are not set in environment variables")
    return create_client(SUPABASE_URL, SUPABASE_KEY)