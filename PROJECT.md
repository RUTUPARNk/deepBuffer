# deepBuffer — portfolio notes

See [README.md](README.md) (core pipeline) and [README_ENHANCED.md](README_ENHANCED.md)
(advanced modules) for full docs. This is the short "what it is / what I did".

## What it is

An original, ambitious **high-throughput parallel data pipeline in pure Python**,
built to push matrices/tensors through generate → compress → write at scale while
sidestepping the GIL with `multiprocessing.shared_memory`. It started as a
multi-threaded matrix writer and grew into a multiprocess shared-memory system
with a live web dashboard.

The headline trick: producer / compressor / writer run as **separate processes**
sharing zero-copy NumPy buffers in shared memory, coordinated by a buffer manager
that does reference counting and buffer reuse. A FastAPI + WebSocket dashboard
streams per-process CPU/memory/queue stats live.

## What I built (by module)

Core pipeline:
- `shared_memory_pipeline.py` — producer/compressor/writer processes over shared memory.
- `buffer_manager.py` — shared-memory buffer pool, ref-counting, reuse, cleanup.
- `main.py` / `main_enhanced.py` — controllers/CLI for the two generations.
- `logger.py` — per-process activity + idle-time tracking.
- `dashboard_server.py` + `dashboard_client.html` + `run_dashboard.py` —
  real-time FastAPI/WebSocket monitoring UI.

Advanced layer:
- `gpu_compression.py` — optional CUDA/CuPy GPU compression with CPU fallback.
- `multi_disk_writer.py` — RAID 0/1/5-style striped/redundant writes across disks.
- `thread_migration.py` + `adaptive_load_balancer.py` — dynamic task
  redistribution off idle/overloaded workers.
- `fast_reader.py` — memory-mapped, index-first reader with LRU cache and slice reads.
- `multiprocess_writer.py` — the multiprocess (GIL-bypass) writer generation.

Reliability layer:
- `write_ahead_log.py` + `transactional_checkpoint.py` — WAL and checkpointing
  for crash recovery.
- `process_supervisor.py` — detects and restarts failed workers.
- `tensor_buffer_manager.py` — newest addition: a **batch API for atomic
  multi-tensor acquire/release** (the last code commit).

Plus a real `tests/` suite (buffer manager, load balancer, supervisor, checkpoint,
multi-disk writer, tensor buffer, logger/dashboard) and `benchmarking.py`.

## Timeline

Three pushes (Jun–Jul 2025): first commit landed the pipeline + most modules;
second added the FastAPI/WebSocket dashboard + logging + live process control;
third added the tensor buffer manager batch API.

## Cleanup done in this pass (2026)

- Removed committed `__pycache__/*.pyc` and added a `.gitignore`.
- Removed two redundant implementation-summary docs (`SUMMARY.md`,
  `PROJECT_SUMMARY.md`) that duplicated the READMEs; kept the two READMEs and
  cross-linked them.
- Flagged in the README that the throughput/benchmark tables are illustrative
  targets, not measured numbers.

## Honest caveats

Broad in scope and the docs (heavy on emoji, enterprise framing, benchmark
tables) read as aspirational in places — treat the numbers as goals. The core
shared-memory pipeline + dashboard is the solid, demonstrable center; the GPU /
RAID / WAL layers are real modules but vary in how battle-tested they are.
