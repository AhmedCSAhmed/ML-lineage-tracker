from hashlib import sha256


class Hashing:
    """Provides hashing functionality for identity and other data.
    
    This class can be used to generate SHA-256 hashes of strings, making it
    reusable for hashing identities and other metadata in the ML lineage tracker.
    """
    def __init__(self) -> None:
        """Initialize a Hashing instance with empty hash_name."""
        self.hash_name = None
        
    
    def set_hash_name_from_env_(self, name:str) -> None:
        """Generate and store a SHA-256 hash of the provided name.
        
        Args:
            name: The string to hash.
        """
        self.hash_name = sha256(name.encode()).hexdigest()
    
    
    def get_hash(self) -> str:
        """Get the stored hash value.
        
        Returns:
            str: The hex digest of the hash, or None if no hash has been set.
        """
        return self.hash_name