import os
from supabase import create_client, Client

SUPABASE_URL: str | None = os.environ.get("ML_LINEAGE_SUPABASE_URL")
SUPABASE_KEY: str | None = os.environ.get("ML_LINEAGE_SUPABASE_KEY")



def connect_to_supabase() -> Client:
    """Establish a connection to the database.

    Returns:
        Client: An instance of the Supabase client connected to the database.
    """
    if not SUPABASE_URL or not SUPABASE_KEY: # fail fast if env vars are not set
        raise RuntimeError("Supabase credentials are not set in environment variables")
    return create_client(SUPABASE_URL, SUPABASE_KEY)