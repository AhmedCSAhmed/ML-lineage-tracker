import pytest
from unittest.mock import Mock, MagicMock, patch
from storage.run_repository import RunRepository
from core.run import Run
from datetime import datetime, timezone


class TestRunRepository:
    """Test suite for RunRepository class."""
    
    @pytest.fixture
    def mock_db_connection(self):
        """Create a mock database connection."""
        return Mock()
    
    @pytest.fixture
    def repository(self, mock_db_connection):
        """Create a RunRepository instance with mocked database connection."""
        return RunRepository(mock_db_connection)
    
    @pytest.fixture
    def sample_run(self):
        """Create a sample run for testing."""
        return Run(
            dataset_id="dataset_1",
            run_name="test_run",
            actor="test_user",
            parameters={"learning_rate": 0.01, "epochs": 10},
            start_time=datetime.now(timezone.utc),
            code_reference="abc123",
            metrics={"accuracy": 0.95, "loss": 0.05}
        )
    
    def test_init(self, mock_db_connection):
        """Test RunRepository initialization."""
        repo = RunRepository(mock_db_connection)
        assert repo.db_connection == mock_db_connection
        assert repo.RUN_TABLE == "Run"
    
    @patch('storage.run_repository.datetime')
    def test_add_run_success(self, mock_datetime, repository, sample_run, mock_db_connection):
        """Test successfully adding a run to the repository."""
        fixed_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = fixed_time
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
        
        repository.add_run(sample_run)
        
        mock_db_connection.insert.assert_called_once()
        call_args = mock_db_connection.insert.call_args
        assert call_args[0][0] == "Run"
        record = call_args[0][1]
        assert record["run_name"] == "test_run"
        assert record["dataset_id"] == "dataset_1"
        assert record["start_time"] == fixed_time.replace(tzinfo=None)
    
    def test_get_run_success(self, repository, mock_db_connection):
        """Test successfully retrieving a run by name."""
        mock_response = MagicMock()
        mock_response.error = None
        mock_response.data = [{
            "dataset_id": "dataset_1",
            "run_name": "test_run",
            "actor": "test_user",
            "parameters": {"learning_rate": 0.01},
            "start_time": datetime.now(timezone.utc),
            "code_reference": "abc123",
            "metrics": {"accuracy": 0.95},
            "end_time": None
        }]
        
        mock_query = MagicMock()
        mock_query.execute.return_value = mock_response
        mock_query.eq.return_value = mock_query
        mock_query.select.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        result = repository.get_run("test_run")
        
        assert result is not None
        assert isinstance(result, Run)
        assert result.run_name == "test_run"
        
        mock_db_connection.table.assert_called_once_with("Run")
        mock_query.select.assert_called_once_with("*")
        mock_query.eq.assert_called_once_with("run_name", "test_run")
        mock_query.execute.assert_called_once()
    
    def test_get_run_not_found(self, repository, mock_db_connection):
        """Test retrieving a run that doesn't exist returns None."""
        mock_response = MagicMock()
        mock_response.error = None
        mock_response.data = []
        
        mock_query = MagicMock()
        mock_query.execute.return_value = mock_response
        mock_query.eq.return_value = mock_query
        mock_query.select.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        result = repository.get_run("nonexistent")
        
        assert result is None
    
    def test_get_run_with_error(self, repository, mock_db_connection):
        """Test that get_run raises RuntimeError when database returns an error."""
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
            repository.get_run("test_run")
        
        assert "Error retrieving run: Database connection failed" in str(exc_info.value)
    
    def test_get_run_missing_name(self, repository):
        """Test that get_run raises ValueError when run_name is missing."""
        with pytest.raises(ValueError) as exc_info:
            repository.get_run("")
        
        assert "Run name is required to retrieve a run" in str(exc_info.value)
    
    def test_batch_get_success(self, repository, mock_db_connection):
        """Test successfully retrieving multiple runs."""
        mock_response_1 = MagicMock()
        mock_response_1.error = None
        mock_response_1.data = [{
            "dataset_id": "dataset_1",
            "run_name": "run1",
            "actor": "user1",
            "parameters": {},
            "start_time": datetime.now(timezone.utc),
            "end_time": None
        }]
        
        mock_response_2 = MagicMock()
        mock_response_2.error = None
        mock_response_2.data = [{
            "dataset_id": "dataset_2",
            "run_name": "run2",
            "actor": "user2",
            "parameters": {},
            "start_time": datetime.now(timezone.utc),
            "end_time": None
        }]
        
        mock_query = MagicMock()
        mock_query.execute.side_effect = [mock_response_1, mock_response_2]
        mock_query.eq.return_value = mock_query
        mock_query.select.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        run_names = ["run1", "run2"]
        runs, count = repository.batch_get(run_names)
        
        assert count == 2
        assert len(runs) == 2
        assert runs[0].run_name == "run1"
        assert runs[1].run_name == "run2"
    
    def test_batch_get_empty_list(self, repository):
        """Test batch_get with empty list."""
        runs, count = repository.batch_get([])
        
        assert count == 0
        assert len(runs) == 0
    
    def test_batch_get_skips_errors(self, repository, mock_db_connection):
        """Test batch_get skips runs that cause RuntimeError."""
        mock_response_1 = MagicMock()
        mock_response_1.error = None
        mock_response_1.data = [{
            "dataset_id": "dataset_1",
            "run_name": "run1",
            "actor": "user1",
            "parameters": {},
            "start_time": datetime.now(timezone.utc),
            "end_time": None
        }]
        
        mock_error = MagicMock()
        mock_error.message = "Database error"
        
        mock_response_2 = MagicMock()
        mock_response_2.error = mock_error
        mock_response_2.data = None
        
        mock_query = MagicMock()
        mock_query.execute.side_effect = [mock_response_1, mock_response_2]
        mock_query.eq.return_value = mock_query
        mock_query.select.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        run_names = ["run1", "run2"]
        runs, count = repository.batch_get(run_names)
        
        assert count == 1
        assert len(runs) == 1
        assert runs[0].run_name == "run1"
    
    def test_update_run_success(self, repository, sample_run, mock_db_connection):
        """Test successfully updating a run."""
        mock_query = MagicMock()
        mock_query.update.return_value = mock_query
        mock_query.eq.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        repository.update_run(sample_run)
        
        mock_db_connection.table.assert_called_once_with("Run")
        mock_query.update.assert_called_once()
        mock_query.eq.assert_called_once_with("run_name", "test_run")
        mock_query.execute.assert_called_once()
    
    def test_update_run_missing_run_name(self, repository, mock_db_connection):
        """Test update_run raises ValueError when run_name is missing."""
        mock_run = MagicMock()
        mock_run.to_record.return_value = {"dataset_id": "dataset_1", "actor": "user"}
        
        with pytest.raises(ValueError) as exc_info:
            repository.update_run(mock_run)
        
        assert "Run name is required to update a run" in str(exc_info.value)
    
    @patch('storage.run_repository.datetime')
    def test_end_run_success(self, mock_datetime, repository, mock_db_connection):
        """Test successfully ending a run."""
        fixed_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = fixed_time
        
        mock_run = MagicMock()
        mock_run.end_time = None
        
        mock_query_update = MagicMock()
        mock_query_update.eq.return_value = mock_query_update
        mock_query_update.update.return_value = mock_query_update
        mock_db_connection.table.return_value = mock_query_update
        
        with patch.object(repository, 'get_run', return_value=mock_run):
            repository.end_run("test_run")
            
            mock_db_connection.table.assert_called_with("Run")
            mock_query_update.update.assert_called_once_with({"end_time": fixed_time.replace(tzinfo=None)})
            mock_query_update.eq.assert_called_once_with("run_name", "test_run")
            mock_query_update.execute.assert_called_once()
    
    def test_end_run_missing_name(self, repository):
        """Test end_run raises ValueError when run_name is missing."""
        with pytest.raises(ValueError) as exc_info:
            repository.end_run("")
        
        assert "Run name is required to end a run" in str(exc_info.value)
    
    def test_end_run_already_ended(self, repository, mock_db_connection):
        """Test end_run raises ValueError when run is already ended."""
        mock_run = MagicMock()
        mock_run.end_time = datetime.now(timezone.utc)
        
        with patch.object(repository, 'get_run', return_value=mock_run):
            with pytest.raises(ValueError) as exc_info:
                repository.end_run("test_run")
            
            assert "Run has already been ended" in str(exc_info.value)
    
    @patch('storage.run_repository.datetime')
    def test_batch_create_success(self, mock_datetime, repository, mock_db_connection):
        """Test successfully creating multiple runs."""
        fixed_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = fixed_time
        
        run1 = Run(
            dataset_id="dataset_1",
            run_name="run1",
            actor="user",
            parameters={},
            start_time=datetime.now(timezone.utc)
        )
        
        run2 = Run(
            dataset_id="dataset_2",
            run_name="run2",
            actor="user",
            parameters={},
            start_time=datetime.now(timezone.utc)
        )
        
        runs = [run1, run2]
        repository.batch_create(runs)
        
        assert mock_db_connection.insert.call_count == 2
    
    @patch('storage.run_repository.datetime')
    def test_batch_create_skips_invalid_entries(self, mock_datetime, repository, mock_db_connection):
        """Test batch_create skips runs without run_name."""
        fixed_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = fixed_time
        
        run1 = Run(
            dataset_id="dataset_1",
            run_name="run1",
            actor="user",
            parameters={},
            start_time=datetime.now(timezone.utc)
        )
        
        mock_run_without_name = MagicMock()
        mock_run_without_name.run_name = ""
        
        runs = [run1, mock_run_without_name]
        repository.batch_create(runs)
        
        assert mock_db_connection.insert.call_count == 1
    
    def test_batch_create_empty_list(self, repository, mock_db_connection):
        """Test batch_create with empty list."""
        repository.batch_create([])
        
        mock_db_connection.insert.assert_not_called()
    
    def test_delete_run_success(self, repository, mock_db_connection):
        """Test successfully deleting a run."""
        mock_response = MagicMock()
        mock_response.error = None
        mock_response.count = 1
        
        mock_query = MagicMock()
        mock_query.execute.return_value = mock_response
        mock_query.eq.return_value = mock_query
        mock_query.delete.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        result = repository.delete_run("test_run")
        
        assert result == 1
        mock_db_connection.table.assert_called_once_with("Run")
        mock_query.delete.assert_called_once()
        mock_query.eq.assert_called_once_with("run_name", "test_run")
        mock_query.execute.assert_called_once()
    
    def test_delete_run_not_found(self, repository, mock_db_connection):
        """Test deleting a run that doesn't exist returns 0."""
        mock_response = MagicMock()
        mock_response.error = None
        mock_response.count = 0
        
        mock_query = MagicMock()
        mock_query.execute.return_value = mock_response
        mock_query.eq.return_value = mock_query
        mock_query.delete.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        result = repository.delete_run("nonexistent")
        
        assert result == 0
    
    def test_delete_run_with_error(self, repository, mock_db_connection):
        """Test that delete_run raises RuntimeError when database returns an error."""
        mock_error = MagicMock()
        mock_error.message = "Database connection failed"
        
        mock_response = MagicMock()
        mock_response.error = mock_error
        mock_response.count = None
        
        mock_query = MagicMock()
        mock_query.execute.return_value = mock_response
        mock_query.eq.return_value = mock_query
        mock_query.delete.return_value = mock_query
        mock_db_connection.table.return_value = mock_query
        
        with pytest.raises(RuntimeError) as exc_info:
            repository.delete_run("test_run")
        
        assert "Error deleting run: Database connection failed" in str(exc_info.value)
    
    def test_delete_run_missing_name(self, repository):
        """Test delete_run raises ValueError when run_name is missing."""
        with pytest.raises(ValueError) as exc_info:
            repository.delete_run("")
        
        assert "Run name is required to delete a run" in str(exc_info.value)

