import getpass
import requests
from typing import Dict, Optional
class Identity:
    """Represents a user identity with name and GitHub username.
    
    This class manages identity information that can be derived from the local
    environment. The identity can be hashed later for use in tracking datasets,
    runs, and models.
    """
    def __init__(self) -> None:
        """Initialize an Identity instance with empty name and GitHub username."""
        self.name = None
        self.github_username = None
    
    
    def get_identity(self) -> dict[str, str]:
        """Get the identity as a dictionary mapping name to GitHub username.
        
        Automatically sets name and GitHub username from environment if not already set.
        
        Returns:
            dict[str, str]: Dictionary with name as key and GitHub username as value.
        """
        if not self.name:
            self.set_identity_name_from_env_()
        if not self.github_username:
            self.set_identity_github_username_from_env_()
        return {self.name:self.github_username}
    
    def set_identity_name_from_env_(self) -> None:
        """Set the identity name from the system username using getpass.getuser()."""
        self.name = getpass.getuser()
    
    def set_identity_github_username_from_env_(self) -> None:
        """Set the GitHub username by querying GitHub's API.
        
        Attempts to fetch the GitHub username from the authenticated user endpoint.
        If the request fails (e.g., no authentication), sets github_username to None.
        """
        try:
            self.github_username = requests.get("https://api.github.com/user").json()["login"]
        except Exception as e:
            print(f"Error setting GitHub username: {e}")
            self.github_username = None
    
    