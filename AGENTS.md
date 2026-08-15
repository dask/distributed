# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## Overview

Dask Distributed is the distributed scheduler for the Dask framework, enabling parallel computing across multiple machines. It implements multi-machine task scheduling, fault tolerance, work stealing, memory management, and network communication.

## Environment & Commands

The project uses **Pixi** for environment management.

```bash
# Run arbitrary Python commands
pixi run -- python -c 'print("Hello world!")'

# Run tests
pixi run test

# Run tests (CI mode with coverage, leak detection, slow tests)
pixi run test-ci

# Lint
pixi run lint

# Run a single test file
pixi run test distributed/tests/test_client.py

# Run a single test
pixi run test distributed/tests/test_client.py::test_client_submit

# Run tests matching a pattern
pixi run test distributed/tests/test_client.py -k "submit"

# Run tests in a specific environment
pixi run -e py312 test
```

Key pytest options:
- `--runslow` — include slow tests (omitted by default)
- `-m ci1` / `-m "not ci1"` — run first/second CI partition (tests split for parallelism)
- `--leaks=fds,processes,threads` — enable resource leak detection

## Architecture

### Core modules (all in `distributed/`)

| File | Purpose |
|------|---------|
| `scheduler.py` | Main scheduler — task graph, work stealing, fault tolerance |
| `client.py` | User-facing API — submit tasks, gather futures |
| `worker.py` | Worker process — executes tasks, manages memory |
| `worker_state_machine.py` | Worker state transitions (separate from I/O logic) |
| `core.py` | RPC infrastructure, connection handling |
| `utils_test.py` | Test fixtures and helpers used across all tests |

### Subdirectories

- `comm/` — Communication backends (TCP, UCX, compression)
- `deploy/` — Cluster types: `LocalCluster`, `SSHCluster`, `SpecCluster`, adaptive scaling
- `dashboard/` — Bokeh-based web UI for monitoring
- `diagnostics/` — Task streams, memory sampling, profiling
- `shuffle/` — Distributed shuffle for large data movement
- `protocol/` — Message serialization
- `cli/` — Entry points: `dask scheduler`, `dask worker`, `dask ssh`, `dask spec`

### Key classes

- `Client` — entry point for submitting work to a cluster
- `Scheduler` — coordinates all workers and task execution
- `Worker` — executes tasks; state tracked separately in `WorkerState`/`worker_state_machine.py`
- `LocalCluster` — single-machine cluster for testing/development
- `TaskState` — tracks task lifecycle on both scheduler and worker sides

## Testing

Tests live in `distributed/tests/` (67 files) and each submodule has its own `tests/` subdirectory. The global `conftest.py` at the repo root and `distributed/utils_test.py` provide shared fixtures.

Tests are partitioned by the `ci1` marker for parallel CI execution. Resource leak detection (fds, processes, threads) runs in CI via `distributed/pytest_resourceleaks.py`.

Timeout: 300 seconds per test (signal-based on Unix, thread-based on Windows).

## Key Patterns for Contributors

**IMPORTANT**: never call .compute() or .persist() in the middle of graph definition
(e.g. in all methods of Array, Series, DataFrame, Bag, Delayed). The only place when the
graph is materialized should be where the end user explicitly calls .compute() or
.persist(). When you are defining the graph, you must work with available metadata to
infer the outputs.

## Code Style

- Line length: **120 characters**
- Formatter: Black
- Linter: Ruff (rules: B, TID, I, UP, RUF)
- Type checking: MyPy — strict on newer modules (`scheduler`, `worker`, `active_memory_manager`, `config`, `shuffle`), loose on older ones

## Type Checking Notes

MyPy is configured per-module in `pyproject.toml`. Newer modules have strict settings; older modules like `client.py` use `allow_incomplete_defs = true`. Platform target is Linux.

## Contributing

You must never think or speak instead of the user in discussions, code reviews, or any
other interactions with other humans.

When the user asks you to open or update a PR, follow the rules in the `open-pr` skill
(`.agents/skills/open-pr/SKILL.md`).

## Releasing

A coding agent must NEVER create a new release.
