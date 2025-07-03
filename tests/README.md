# Shared Writer Project Test Suite

This directory contains comprehensive tests for the shared writer project components.

## Test Components

### Buffer Manager Tests (`test_buffer_manager.py`)
- Buffer acquisition and release (thread-safe)
- Buffer exhaustion and recycling
- Buffer tracking accuracy

### Checkpoint Manager Tests (`test_checkpoint_manager.py`)
- Atomic checkpoint save/load
- Checkpoint rotation
- Checksum validation
- Recovery from valid checkpoints
- Metadata validation

### Multi-Disk Writer Tests (`test_multi_disk_writer.py`)
- Round-robin file writing
- Load-based file selection
- File integrity validation
- Disk failure handling

### Process Supervisor Tests (`test_process_supervisor.py`)
- Process monitoring and heartbeats
- Process failure recovery
- Checkpoint integration
- Soft shutdown handling

### Logger & Dashboard Tests (`test_logger_dashboard.py`)
- Thread/process activity logging
- System metrics collection
- Real-time WebSocket updates
- Error logging and export
- Dashboard API endpoints

## Setup

1. Install test dependencies:
   ```bash
   pip install -r requirements-test.txt
   ```

2. Run all tests:
   ```bash
   pytest
   ```

3. Run specific test files:
   ```bash
   pytest tests/test_checkpoint_manager.py
   pytest tests/test_process_supervisor.py
   ```

4. Run with coverage:
   ```bash
   pytest --cov=file_writer
   ```

## Test Categories

- **Unit Tests**: Basic component functionality
  ```bash
  pytest -m unit
  ```

- **Integration Tests**: Component interaction
  ```bash
  pytest -m integration
  ```

- **Slow Tests**: Long-running tests
  ```bash
  pytest -m "not slow"  # Skip slow tests
  ```

## Coverage Reports

After running tests with coverage, reports are generated in:
- Terminal output (missing lines)
- HTML report in `htmlcov/` directory

## Common Test Fixtures

- `temp_dir`: Temporary directory for test files
- `mock_process_ids`: Mock process IDs for testing
- `mock_thread_ids`: Mock thread IDs for testing
- `mock_logger`: Mock logger instance
- `mock_thread`: Thread class for concurrent testing

## Best Practices

1. Use appropriate markers for test categorization
2. Keep tests isolated and independent
3. Clean up test resources in fixtures
4. Mock external dependencies
5. Test both success and failure cases
6. Include docstrings for test functions

## Continuous Integration

The test suite is integrated with CI/CD:
- Runs on every pull request
- Enforces minimum coverage threshold
- Reports test results and coverage

## Adding New Tests

1. Create test file in `tests/` directory
2. Use appropriate fixtures from `conftest.py`
3. Add markers for test categorization
4. Include both positive and negative test cases
5. Update this README if adding new components 