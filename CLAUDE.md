# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Distributed is a library for distributed computation in Python, part of the Dask ecosystem. It provides a distributed task scheduler with work stealing, data locality, and fault tolerance.

## Development Commands

### Setup
```bash
# Create conda environment (replace 3.12 with your Python version: 3.10, 3.11, 3.12, or 3.13)
mamba env create -f continuous_integration/environment-3.12.yaml -n dask-distributed
conda activate dask-distributed

# Install in development mode
pip install -e .
```

### Testing
```bash
# Run all tests
pytest distributed

# Run tests in a specific file
pytest distributed/tests/test_client.py

# Run a specific test
pytest distributed/tests/test_client.py::test_submit

# Run with slow tests included
pytest distributed --runslow

# Run tests with markers (exclude CI-avoided tests, include partition)
pytest distributed -m "not avoid_ci and ci1" --runslow

# Run tests with coverage
pytest distributed --cov=distributed --cov-report=html

# Run tests with leak detection
pytest distributed --leaks=fds,processes,threads
```

### Code Quality
```bash
# Run all pre-commit hooks
pre-commit run --all-files

# Run ruff linting
ruff check distributed

# Run ruff formatting
ruff format distributed

# Run mypy type checking
mypy distributed

# Run codespell
codespell
```

### Documentation
```bash
cd docs
make html  # Build HTML documentation
```

### Running Distributed Components
```bash
# Start a scheduler
dask-scheduler

# Start a worker (connect to scheduler at specified address)
dask-worker tcp://127.0.0.1:8786

# Start scheduler via SSH
dask-ssh --hostfile hosts.txt
```

## Architecture Overview

### Core Components

**Scheduler (`distributed/scheduler.py`)** - The central coordinator that:
- Maintains cluster state (workers, tasks, clients)
- Schedules tasks to workers based on data locality and resource availability
- Implements work stealing for load balancing
- Handles worker failures and task retry
- Provides extension/plugin system for custom behavior

**Client (`distributed/client.py`)** - User-facing API that:
- Submits tasks and task graphs to the scheduler
- Retrieves results via futures
- Provides high-level operations: `submit()`, `map()`, `compute()`, `gather()`
- Manages connection to scheduler

**Worker (`distributed/worker.py`)** - Task executor that:
- Executes tasks in thread/process pools
- Manages local data storage with memory limits
- Reports metrics to scheduler
- Implements worker-side state machine (`worker_state_machine.py`)
- Handles data transfer between workers

**Nanny (`distributed/nanny.py`)** - Worker supervisor that:
- Spawns and monitors worker processes
- Restarts crashed workers
- Enforces memory limits (pause/restart on exceed)

### Communication Architecture

All components communicate via **async RPC over pluggable transports**:

- **Transport Layer** (`distributed/comm/`): TCP (default), WebSocket, in-process
- **RPC Protocol** (`distributed/core.py`): Message-based with handler dispatch
  - Messages are dicts: `{'op': 'operation_name', 'arg1': value, ...}`
  - Handlers registered in `handlers` dict on Server classes
  - First handler arg is `comm: Comm`, remaining args from message dict
- **Serialization** (`distributed/protocol/`): cloudpickle, msgpack, with compression
- **Connection Pooling** (`distributed/batched.py`): Persistent connections, message batching

### State Machine Pattern

Tasks flow through states on both scheduler and worker:

**Scheduler Task States:**
- `released` → `waiting` → `queued` → `processing` → `memory` → `forgotten`
- Alternative paths: `error`, `erred`, `no-worker`

**Worker Task States:** (via `WorkerState` in `worker_state_machine.py`)
- `fetch` → `flight` → `executing` → `memory` → `released`
- Alternative paths: `error`, `missing`, `constrained`, `long-running`

State transitions are coordinated via RPC and logged in `transition_log` for debugging.

### Plugin System

**SchedulerPlugin** (`distributed/diagnostics/plugin.py`):
- Hooks: `start()`, `close()`, `update_graph()`, `transition()`, etc.
- Full access to scheduler state
- Used by distributed data structures (Queue, Variable, Lock, etc.)

**WorkerPlugin**:
- Similar hooks for worker-side customization
- Used for monitoring, preprocessing, resource management

### Key Subsystems

**Shuffle (`distributed/shuffle/`)** - Distributed data repartitioning:
- Coordinates complex multi-worker data transfer
- Used by dask.dataframe and dask.array for repartitioning/rechunking
- Implements scheduler and worker plugins for orchestration

**Active Memory Manager (`distributed/active_memory_manager.py`)** - Automatic memory management:
- Replicates important data
- Drops redundant data when memory pressure builds
- Rebalances data across workers

**Dashboard (`distributed/dashboard/`)** - Web UI (Bokeh-based):
- Real-time cluster monitoring
- Task stream visualization
- Resource graphs (CPU, memory, network)
- Available at `http://scheduler:8787` by default

**Spans (`distributed/spans.py`)** - Distributed tracing:
- OpenTelemetry integration
- Track computation spans across cluster

## Testing Patterns

### Fixtures and Utilities

Tests use fixtures from `distributed.utils_test`:
- `cleanup()` - Ensure clean state between tests
- `loop()` - Event loop fixture
- `cluster()` / `client()` - LocalCluster and Client fixtures
- `ws` fixture - Automatically marks test as `@pytest.mark.workerstate`

### Common Testing Patterns

```python
# Use gen_cluster for async tests with cluster
@gen_cluster(client=True)
async def test_something(c, s, a, b):
    # c = client, s = scheduler, a = first worker, b = second worker
    future = c.submit(inc, 1)
    result = await future
    assert result == 2

# Use cluster fixture for sync tests
def test_something_sync(client):
    future = client.submit(inc, 1)
    result = future.result()
    assert result == 2
```

### Test Markers

- `@pytest.mark.slow` - Long-running tests (skip without `--runslow`)
- `@pytest.mark.avoid_ci` - Flaky/broken tests excluded from CI
- `@pytest.mark.ci1` / not `ci1` - Test partitioning for parallel CI runs
- `@pytest.mark.gpu` - GPU-specific tests
- `@pytest.mark.leaking` - Tests with expected resource leaks

## Configuration System

Distributed uses Dask's configuration system with distributed-specific config in `~/.config/dask/distributed.yaml`:

```yaml
distributed:
  scheduler:
    allowed-failures: 3
    bandwidth: 100000000
    work-stealing: True
  worker:
    memory:
      target: 0.60  # Start spilling at 60% memory
      spill: 0.70   # Spill to disk at 70%
      pause: 0.80   # Pause at 80%
      terminate: 0.95  # Terminate at 95%
  comm:
    compression: auto
    timeouts:
      connect: 30s
      tcp: 30s
```

Override in code:
```python
import dask
dask.config.set({'distributed.scheduler.work-stealing': False})
```

Or via environment variables:
```bash
DASK_DISTRIBUTED__SCHEDULER__WORK_STEALING=False pytest distributed/tests/test_scheduler.py
```

## Important Implementation Notes

### Async/Await Throughout

- Built on tornado IOLoop (compatible with asyncio)
- All I/O is non-blocking
- RPC handlers should be `async def` functions
- Use `await self.rpc(worker_address).some_operation(args)` for RPC calls

### Serialization

- User functions serialized with cloudpickle
- Small messages use msgpack for efficiency
- Large data uses custom protocol with zero-copy where possible
- Compression configurable (default: auto based on size/type)

### Worker State Machine (`worker_state_machine.py`)

- The 142k-line file is the deterministic core of worker task execution
- Pure functions (no I/O) that take state and return new state
- Extensively tested with property-based testing
- When modifying, ensure transitions remain deterministic

### Memory Management

- Workers track memory via `psutil`
- Three-tier spilling: memory → disk → drop
- Memory limits configurable per worker: `--memory-limit 4GB`
- Active Memory Manager coordinates cluster-wide optimization

### Test Isolation

- Each test should clean up resources (workers, schedulers, clients)
- Use fixtures for automatic cleanup
- Pytest resource leak detection catches unclosed resources
- Tests run with strict warnings (most warnings are errors)

## Common Development Tasks

### Adding a New RPC Operation

1. Add handler to target class (Scheduler/Worker/Client):
```python
class Scheduler:
    async def my_new_operation(self, arg1, arg2):
        # Implementation
        return result
```

2. Call from remote:
```python
result = await self.rpc(scheduler_address).my_new_operation(val1, val2)
```

### Adding a Scheduler Plugin

```python
from distributed.diagnostics.plugin import SchedulerPlugin

class MyPlugin(SchedulerPlugin):
    async def start(self, scheduler):
        # Called when plugin registered
        pass

    async def transition(self, key, start, finish, *args, **kwargs):
        # Called on every task state transition
        pass

# Register
client.register_scheduler_plugin(MyPlugin())
```

### Adding a Worker Plugin

```python
from distributed.diagnostics.plugin import WorkerPlugin

class MyPlugin(WorkerPlugin):
    def setup(self, worker):
        # Called once when plugin registered
        pass

    def transition(self, key, start, finish, *args, **kwargs):
        # Called on worker task transitions
        pass

# Register
client.register_worker_plugin(MyPlugin())
```

## CI/CD Details

- Tests run on Ubuntu, Windows, and macOS
- Python versions: 3.10, 3.11, 3.12, 3.13
- Special environments: mindeps (minimum dependencies)
- Tests partitioned with `ci1` marker for parallel execution
- pytest-timeout configured (signal mode on Unix, thread mode on Windows)
- Coverage reports uploaded to codecov.io
- Longitudinal test reports track flaky tests over time
