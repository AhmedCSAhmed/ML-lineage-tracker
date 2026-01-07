import pytest
from unittest.mock import Mock, MagicMock
from storage.model_repository import ModelRepository
from core.model import Model


class TestModelRepository:
    """Test suite for ModelRepository class."""
    
    @pytest.fixture
    def mock_db_connection(self):
        """Create a mock database connection."""
        return Mock()
    
    @pytest.fixture
    def repository(self, mock_db_connection):
        """Create a ModelRepository instance with mocked database connection."""
        return ModelRepository(mock_db_connection)
    
    @pytest.fixture
    def sample_model(self):
        """Create a sample model for testing."""
        return Model(
            artifact_uri="s3://bucket/model.pkl",
            model_name="test_model",
            associated_run_id="run_123",
            lifecycle_stage="registered"
        )
    
    def test_init(self, mock_db_connection):
        """Test ModelRepository initialization."""
        repo = ModelRepository(mock_db_connection)
        assert repo.db_connection == mock_db_connection
        assert repo.MODEL_TABLE == "Model"
    
    def test_add_model_success(self, repository, sample_model, mock_db_connection):
        """Test successfully adding a model to the repository."""
        repository.add_model(sample_model)
        
        mock_db_connection.insert.assert_called_once()
        call_args = mock_db_connection.insert.call_args
        assert call_args[0][0] == "Model"
        assert call_args[0][1] == sample_model.to_record()
    
    def test_get_model_success(self, repository, mock_db_connection):
        """Test successfully retrieving a model by name."""
        mock_response = MagicMock()
        mock_response.error = None
        mock_response.data = [{
            "artifact_uri": "s3://bucket/model.pkl",
            "model_name": "test_model",
            "associated_run_id": "run_123",
            "lifecycle_stage": "registered"
        }]
        
        mock_query = MagicMock()
        mock_query.execute.return_value = mock_response
        mock_query.eq.return_value = mock_query
        mock_query.select.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        result = repository.get_model("test_model")
        
        assert result is not None
        assert isinstance(result, Model)
        assert result.model_name == "test_model"
        assert result.artifact_uri == "s3://bucket/model.pkl"
        assert result.associated_run_id == "run_123"
        assert result.lifecycle_stage == "registered"
        
        mock_db_connection.table.assert_called_once_with("Model")
        mock_query.select.assert_called_once_with("*")
        mock_query.eq.assert_called_once_with("model_name", "test_model")
        mock_query.execute.assert_called_once()
    
    def test_get_model_not_found(self, repository, mock_db_connection):
        """Test retrieving a model that doesn't exist returns None."""
        mock_response = MagicMock()
        mock_response.error = None
        mock_response.data = []
        
        mock_query = MagicMock()
        mock_query.execute.return_value = mock_response
        mock_query.eq.return_value = mock_query
        mock_query.select.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        result = repository.get_model("nonexistent")
        
        assert result is None
    
    def test_get_model_with_error(self, repository, mock_db_connection):
        """Test that get_model raises RuntimeError when database returns an error."""
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
            repository.get_model("test_model")
        
        assert "Failed to retrieve model: Database connection failed" in str(exc_info.value)
    
    def test_get_model_missing_name(self, repository):
        """Test that get_model raises ValueError when model_name is missing."""
        with pytest.raises(ValueError) as exc_info:
            repository.get_model("")
        
        assert "Model name is required" in str(exc_info.value)
    
    def test_update_model_success(self, repository, sample_model, mock_db_connection):
        """Test successfully updating a model."""
        mock_response = MagicMock()
        mock_response.error = None
        mock_response.data = [{"model_name": "test_model"}]
        
        mock_query = MagicMock()
        mock_query.execute.return_value = mock_response
        mock_query.eq.return_value = mock_query
        mock_query.update.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        repository.update_model(sample_model)
        
        mock_db_connection.table.assert_called_once_with("Model")
        mock_query.update.assert_called_once()
        mock_query.eq.assert_called_once_with("model_name", "test_model")
        mock_query.execute.assert_called_once()
    
    def test_update_model_missing_model_name(self, repository, mock_db_connection):
        """Test update_model raises ValueError when model_name is missing."""
        mock_model = MagicMock()
        mock_model.model_name = ""
        mock_model.to_record.return_value = {}
        
        with pytest.raises(ValueError) as exc_info:
            repository.update_model(mock_model)
        
        assert "Model is required for update" in str(exc_info.value)
    
    def test_update_model_not_found(self, repository, sample_model, mock_db_connection):
        """Test update_model raises RuntimeError when model is not found."""
        mock_response = MagicMock()
        mock_response.error = None
        mock_response.data = []
        
        mock_query = MagicMock()
        mock_query.execute.return_value = mock_response
        mock_query.eq.return_value = mock_query
        mock_query.update.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        with pytest.raises(RuntimeError) as exc_info:
            repository.update_model(sample_model)
        
        assert "No model found to update" in str(exc_info.value)
    
    def test_update_model_with_error(self, repository, sample_model, mock_db_connection):
        """Test update_model raises RuntimeError when database returns an error."""
        mock_error = MagicMock()
        mock_error.message = "Database connection failed"
        
        mock_response = MagicMock()
        mock_response.error = mock_error
        mock_response.data = None
        
        mock_query = MagicMock()
        mock_query.execute.return_value = mock_response
        mock_query.eq.return_value = mock_query
        mock_query.update.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        with pytest.raises(RuntimeError) as exc_info:
            repository.update_model(sample_model)
        
        assert "Failed to update model: Database connection failed" in str(exc_info.value)
    
    def test_delete_model_success(self, repository, mock_db_connection):
        """Test successfully deleting a model."""
        mock_response = MagicMock()
        mock_response.error = None
        mock_response.data = [{"model_name": "test_model"}]
        
        mock_query = MagicMock()
        mock_query.execute.return_value = mock_response
        mock_query.eq.return_value = mock_query
        mock_query.delete.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        repository.delete_model("test_model")
        
        mock_db_connection.table.assert_called_once_with("Model")
        mock_query.delete.assert_called_once()
        mock_query.eq.assert_called_once_with("model_name", "test_model")
        mock_query.execute.assert_called_once()
    
    def test_delete_model_not_found(self, repository, mock_db_connection):
        """Test delete_model raises RuntimeError when model is not found."""
        mock_response = MagicMock()
        mock_response.error = None
        mock_response.data = []
        
        mock_query = MagicMock()
        mock_query.execute.return_value = mock_response
        mock_query.eq.return_value = mock_query
        mock_query.delete.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        with pytest.raises(RuntimeError) as exc_info:
            repository.delete_model("nonexistent")
        
        assert "No model found to delete" in str(exc_info.value)
    
    def test_delete_model_with_error(self, repository, mock_db_connection):
        """Test delete_model raises RuntimeError when database returns an error."""
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
            repository.delete_model("test_model")
        
        assert "Failed to delete model: Database connection failed" in str(exc_info.value)
    
    def test_delete_model_missing_name(self, repository):
        """Test delete_model raises ValueError when model_name is missing."""
        with pytest.raises(ValueError) as exc_info:
            repository.delete_model("")
        
        assert "Model name is required for deletion" in str(exc_info.value)
    
    def test_get_artifact_uri_success(self, repository, mock_db_connection):
        """Test successfully getting artifact URI for a model."""
        mock_response = MagicMock()
        mock_response.error = None
        mock_response.data = [{
            "artifact_uri": "s3://bucket/model.pkl",
            "model_name": "test_model",
            "associated_run_id": "run_123",
            "lifecycle_stage": "registered"
        }]
        
        mock_query = MagicMock()
        mock_query.execute.return_value = mock_response
        mock_query.eq.return_value = mock_query
        mock_query.select.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        result = repository.get_artifact_uri("test_model")
        
        assert result == "s3://bucket/model.pkl"
    
    def test_get_artifact_uri_not_found(self, repository, mock_db_connection):
        """Test get_artifact_uri returns None when model is not found."""
        mock_response = MagicMock()
        mock_response.error = None
        mock_response.data = []
        
        mock_query = MagicMock()
        mock_query.execute.return_value = mock_response
        mock_query.eq.return_value = mock_query
        mock_query.select.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        result = repository.get_artifact_uri("nonexistent")
        
        assert result is None
    
    def test_list_models_success(self, repository, mock_db_connection):
        """Test successfully listing all models."""
        mock_response = MagicMock()
        mock_response.error = None
        mock_response.data = [
            {
                "artifact_uri": "s3://bucket/model1.pkl",
                "model_name": "model1",
                "associated_run_id": "run_1",
                "lifecycle_stage": "registered"
            },
            {
                "artifact_uri": "s3://bucket/model2.pkl",
                "model_name": "model2",
                "associated_run_id": "run_2",
                "lifecycle_stage": "staging"
            }
        ]
        
        mock_query = MagicMock()
        mock_query.execute.return_value = mock_response
        mock_query.select.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        result = repository.list_models()
        
        assert len(result) == 2
        assert all(isinstance(model, Model) for model in result)
        assert result[0].model_name == "model1"
        assert result[1].model_name == "model2"
        
        mock_db_connection.table.assert_called_once_with("Model")
        mock_query.select.assert_called_once_with("*")
        mock_query.execute.assert_called_once()
    
    def test_list_models_empty(self, repository, mock_db_connection):
        """Test listing models when database is empty."""
        mock_response = MagicMock()
        mock_response.error = None
        mock_response.data = []
        
        mock_query = MagicMock()
        mock_query.execute.return_value = mock_response
        mock_query.select.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        result = repository.list_models()
        
        assert result == []
    
    def test_list_models_with_error(self, repository, mock_db_connection):
        """Test list_models raises RuntimeError when database returns an error."""
        mock_error = MagicMock()
        mock_error.message = "Database connection failed"
        
        mock_response = MagicMock()
        mock_response.error = mock_error
        mock_response.data = None
        
        mock_query = MagicMock()
        mock_query.execute.return_value = mock_response
        mock_query.select.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        with pytest.raises(RuntimeError) as exc_info:
            repository.list_models()
        
        assert "Failed to list models: Database connection failed" in str(exc_info.value)
    
    def test_batch_get_success(self, repository, mock_db_connection):
        """Test successfully retrieving multiple models."""
        mock_response_1 = MagicMock()
        mock_response_1.error = None
        mock_response_1.data = [{
            "artifact_uri": "s3://bucket/model1.pkl",
            "model_name": "model1",
            "associated_run_id": "run_1",
            "lifecycle_stage": "registered"
        }]
        
        mock_response_2 = MagicMock()
        mock_response_2.error = None
        mock_response_2.data = [{
            "artifact_uri": "s3://bucket/model2.pkl",
            "model_name": "model2",
            "associated_run_id": "run_2",
            "lifecycle_stage": "staging"
        }]
        
        mock_query = MagicMock()
        mock_query.execute.side_effect = [mock_response_1, mock_response_2]
        mock_query.eq.return_value = mock_query
        mock_query.select.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        model_names = ["model1", "model2"]
        result = repository.batch_get(model_names)
        
        assert len(result) == 2
        assert result[0].model_name == "model1"
        assert result[1].model_name == "model2"
    
    def test_batch_get_empty_list(self, repository):
        """Test batch_get with empty list."""
        result = repository.batch_get([])
        
        assert result == []
    
    def test_batch_get_skips_invalid_entries(self, repository, mock_db_connection):
        """Test batch_get skips empty names and non-found models."""
        mock_response_1 = MagicMock()
        mock_response_1.error = None
        mock_response_1.data = [{
            "artifact_uri": "s3://bucket/model1.pkl",
            "model_name": "model1",
            "associated_run_id": "run_1",
            "lifecycle_stage": "registered"
        }]
        
        mock_response_2 = MagicMock()
        mock_response_2.error = None
        mock_response_2.data = []
        
        mock_query = MagicMock()
        mock_query.execute.side_effect = [mock_response_1, mock_response_2]
        mock_query.eq.return_value = mock_query
        mock_query.select.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        model_names = ["model1", "", "nonexistent"]
        result = repository.batch_get(model_names)
        
        assert len(result) == 1
        assert result[0].model_name == "model1"
    
    def test_batch_get_non_string_name(self, repository):
        """Test batch_get raises ValueError when model name is not a string."""
        with pytest.raises(ValueError) as exc_info:
            repository.batch_get([123])
        
        assert "Model names must be strings" in str(exc_info.value)
    
    def test_model_exists_true(self, repository, mock_db_connection):
        """Test model_exists returns True when model exists."""
        mock_response = MagicMock()
        mock_response.error = None
        mock_response.data = [{
            "artifact_uri": "s3://bucket/model.pkl",
            "model_name": "test_model",
            "associated_run_id": "run_123",
            "lifecycle_stage": "registered"
        }]
        
        mock_query = MagicMock()
        mock_query.execute.return_value = mock_response
        mock_query.eq.return_value = mock_query
        mock_query.select.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        result = repository.model_exists("test_model")
        
        assert result is True
    
    def test_model_exists_false(self, repository, mock_db_connection):
        """Test model_exists returns False when model does not exist."""
        mock_response = MagicMock()
        mock_response.error = None
        mock_response.data = []
        
        mock_query = MagicMock()
        mock_query.execute.return_value = mock_response
        mock_query.eq.return_value = mock_query
        mock_query.select.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        result = repository.model_exists("nonexistent")
        
        assert result is False

