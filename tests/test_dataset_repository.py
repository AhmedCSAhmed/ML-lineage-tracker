import pytest
from unittest.mock import Mock, MagicMock
from storage.dataset_repository import DatasetRepository
from core.dataset import Dataset


class TestDatasetRepository:
    """Test suite for DatasetRepository class."""
    
    @pytest.fixture
    def mock_db_connection(self):
        """Create a mock database connection."""
        return Mock()
    
    @pytest.fixture
    def repository(self, mock_db_connection):
        """Create a DatasetRepository instance with mocked database connection."""
        return DatasetRepository(mock_db_connection)
    
    @pytest.fixture
    def sample_dataset(self):
        """Create a sample dataset for testing."""
        return Dataset(
            name="test_dataset",
            version="1.0.0",
            source="s3://bucket/data.csv",
            actor="test_user",
            description="Test dataset"
        )
    
    def test_init(self, mock_db_connection):
        """Test DatasetRepository initialization."""
        repo = DatasetRepository(mock_db_connection)
        assert repo.db_connection == mock_db_connection
        assert repo.DATASET_TABLE == "Dataset"
    
    def test_add_dataset_success(self, repository, sample_dataset, mock_db_connection):
        """Test successfully adding a dataset to the repository."""
        repository.add_dataset(sample_dataset)
        
        mock_db_connection.insert.assert_called_once()
        call_args = mock_db_connection.insert.call_args
        assert call_args[0][0] == "Dataset"
        assert call_args[0][1] == sample_dataset.to_record()
    
    def test_add_dataset_calls_to_record(self, repository, sample_dataset, mock_db_connection):
        """Test that add_dataset converts dataset to record format."""
        repository.add_dataset(sample_dataset)
        
        expected_record = {
            "name": "test_dataset",
            "version": "1.0.0",
            "source": "s3://bucket/data.csv",
            "actor": "test_user",
            "description": "Test dataset"
        }
        mock_db_connection.insert.assert_called_once_with("Dataset", expected_record)
    
    def test_get_dataset_success(self, repository, mock_db_connection):
        """Test successfully retrieving a dataset by name and version."""
        mock_response = MagicMock()
        mock_response.error = None
        mock_response.data = [{
            "name": "test_dataset",
            "version": "1.0.0",
            "source": "s3://bucket/data.csv",
            "actor": "test_user",
            "description": "Test dataset"
        }]
        
        mock_query = MagicMock()
        mock_query.execute.return_value = mock_response
        mock_query.eq.return_value = mock_query
        mock_query.select.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        result = repository.get_dataset("test_dataset", "1.0.0")
        
        assert result is not None
        assert isinstance(result, Dataset)
        assert result.name == "test_dataset"
        assert result.version == "1.0.0"
        assert result.source == "s3://bucket/data.csv"
        assert result.actor == "test_user"
        assert result.description == "Test dataset"
        
        mock_db_connection.table.assert_called_once_with("Dataset")
        mock_query.select.assert_called_once_with("*")
        mock_query.eq.assert_any_call("name", "test_dataset")
        mock_query.eq.assert_any_call("version", "1.0.0")
        mock_query.execute.assert_called_once()
    
    def test_get_dataset_not_found(self, repository, mock_db_connection):
        """Test retrieving a dataset that doesn't exist returns None."""
        mock_response = MagicMock()
        mock_response.error = None
        mock_response.data = []
        
        mock_query = MagicMock()
        mock_query.execute.return_value = mock_response
        mock_query.eq.return_value = mock_query
        mock_query.select.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        result = repository.get_dataset("nonexistent", "1.0.0")
        
        assert result is None
    
    def test_get_dataset_with_error(self, repository, mock_db_connection):
        """Test that get_dataset raises RuntimeError when database returns an error."""
        mock_error = MagicMock()
        mock_error.message = "Database connection failed"
        
        mock_response = MagicMock()
        mock_response.error = mock_error
        mock_response.data = None
        
        mock_query = MagicMock()
        mock_query.execute.return_value = mock_response
        mock_query.eq.return_value = mock_query
        mock_query.select.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        with pytest.raises(RuntimeError) as exc_info:
            repository.get_dataset("test_dataset", "1.0.0")
        
        assert "Error retrieving dataset: Database connection failed" in str(exc_info.value)
    
    def test_get_dataset_query_chain(self, repository, mock_db_connection):
        """Test that get_dataset builds the correct query chain."""
        mock_response = MagicMock()
        mock_response.error = None
        mock_response.data = []
        
        mock_query = MagicMock()
        mock_query.execute.return_value = mock_response
        mock_query.eq.return_value = mock_query
        mock_query.select.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        repository.get_dataset("my_dataset", "2.0.0")
        
        mock_db_connection.table.assert_called_once_with("Dataset")
        mock_query.select.assert_called_once_with("*")
        assert mock_query.eq.call_count == 2
        calls = [call[0] for call in mock_query.eq.call_args_list]
        assert ("name", "my_dataset") in calls
        assert ("version", "2.0.0") in calls
        mock_query.execute.assert_called_once()
    
    def test_add_dataset_with_minimal_dataset(self, repository, mock_db_connection):
        """Test adding a dataset with only required fields."""
        minimal_dataset = Dataset(
            name="minimal",
            version="1.0",
            source="/path/to/data",
            actor="user"
        )
        
        repository.add_dataset(minimal_dataset)
        
        expected_record = {
            "name": "minimal",
            "version": "1.0",
            "source": "/path/to/data",
            "actor": "user",
            "description": None
        }
        mock_db_connection.insert.assert_called_once_with("Dataset", expected_record)
    
    def test_delete_dataset_success(self, repository, mock_db_connection):
        """Test successfully deleting a dataset by name and version."""
        mock_response = MagicMock()
        mock_response.error = None
        mock_response.data = [{"name": "test_dataset", "version": "1.0.0"}]
        
        mock_query = MagicMock()
        mock_query.execute.return_value = mock_response
        mock_query.eq.return_value = mock_query
        mock_query.delete.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        result = repository.delete_dataset("test_dataset", "1.0.0")
        
        assert result == 1
        mock_db_connection.table.assert_called_once_with("Dataset")
        mock_query.delete.assert_called_once()
        mock_query.eq.assert_any_call("name", "test_dataset")
        mock_query.eq.assert_any_call("version", "1.0.0")
        mock_query.execute.assert_called_once()
    
    def test_delete_dataset_not_found(self, repository, mock_db_connection):
        """Test deleting a dataset that doesn't exist returns 0."""
        mock_response = MagicMock()
        mock_response.error = None
        mock_response.data = []
        
        mock_query = MagicMock()
        mock_query.execute.return_value = mock_response
        mock_query.eq.return_value = mock_query
        mock_query.delete.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        result = repository.delete_dataset("nonexistent", "1.0.0")
        
        assert result == 0
    
    def test_delete_dataset_with_error(self, repository, mock_db_connection):
        """Test that delete_dataset raises RuntimeError when database returns an error."""
        mock_error = MagicMock()
        mock_error.message = "Database connection failed"
        
        mock_response = MagicMock()
        mock_response.error = mock_error
        mock_response.data = None
        
        mock_query = MagicMock()
        mock_query.execute.return_value = mock_response
        mock_query.eq.return_value = mock_query
        mock_query.delete.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        with pytest.raises(RuntimeError) as exc_info:
            repository.delete_dataset("test_dataset", "1.0.0")
        
        assert "Error deleting dataset: Database connection failed" in str(exc_info.value)
    
    def test_delete_dataset_missing_name(self, repository):
        """Test that delete_dataset raises ValueError when name is missing."""
        with pytest.raises(ValueError) as exc_info:
            repository.delete_dataset("", "1.0.0")
        
        assert "Both name and version are required" in str(exc_info.value)
    
    def test_delete_dataset_missing_version(self, repository):
        """Test that delete_dataset raises ValueError when version is missing."""
        with pytest.raises(ValueError) as exc_info:
            repository.delete_dataset("test_dataset", "")
        
        assert "Both name and version are required" in str(exc_info.value)
    
    def test_delete_dataset_missing_both(self, repository):
        """Test that delete_dataset raises ValueError when both name and version are missing."""
        with pytest.raises(ValueError) as exc_info:
            repository.delete_dataset("", "")
        
        assert "Both name and version are required" in str(exc_info.value)
    
    def test_delete_dataset_none_values(self, repository):
        """Test that delete_dataset raises ValueError when name or version is None."""
        with pytest.raises(ValueError):
            repository.delete_dataset(None, "1.0.0")
        
        with pytest.raises(ValueError):
            repository.delete_dataset("test_dataset", None)
    
    def test_delete_dataset_query_chain(self, repository, mock_db_connection):
        """Test that delete_dataset builds the correct query chain."""
        mock_response = MagicMock()
        mock_response.error = None
        mock_response.data = [{"name": "my_dataset", "version": "2.0.0"}]
        
        mock_query = MagicMock()
        mock_query.execute.return_value = mock_response
        mock_query.eq.return_value = mock_query
        mock_query.delete.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        repository.delete_dataset("my_dataset", "2.0.0")
        
        mock_db_connection.table.assert_called_once_with("Dataset")
        mock_query.delete.assert_called_once()
        assert mock_query.eq.call_count == 2
        calls = [call[0] for call in mock_query.eq.call_args_list]
        assert ("name", "my_dataset") in calls
        assert ("version", "2.0.0") in calls
        mock_query.execute.assert_called_once()

