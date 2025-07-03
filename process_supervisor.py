import threading
import time
from typing import Dict, List, Optional, Callable, Any
from logger import ProcessLogger
from transactional_checkpoint import CheckpointManager

class ProcessSupervisor:
    """
    Supervises and recovers processes in the shared memory pipeline.
    Monitors heartbeats, restarts failed processes, checkpoints data, and logs events for dashboard.
    Integrates atomic checkpointing for hot recovery.
    """
    process_ids: List[int]
    heartbeat_interval: float
    logger: ProcessLogger
    checkpoint_callback: Optional[Callable[[], None]]
    running: bool
    lock: threading.Lock
    supervisor_thread: threading.Thread
    checkpoint_manager: CheckpointManager
    checkpoint_interval: float
    tasks_per_checkpoint: int
    last_checkpoint_time: float
    completed_tasks_since_checkpoint: int
    # heartbeats: Dict[int, float]  # Removed to avoid shadowing
    # system_state: Dict[str, Any]  # Removed to avoid shadowing

    def __init__(self, process_ids: List[int], heartbeat_interval: float, logger: ProcessLogger, checkpoint_path: str, checkpoint_interval: float = 10.0, tasks_per_checkpoint: int = 10, checkpoint_callback: Optional[Callable[[], None]] = None) -> None:
        self.process_ids = process_ids
        self.heartbeat_interval = heartbeat_interval
        self.logger = logger
        self.checkpoint_callback = checkpoint_callback
        self.heartbeats: Dict[int, float] = {pid: time.time() for pid in process_ids}
        self.running = False
        self.lock = threading.Lock()
        self.supervisor_thread = threading.Thread(target=self._supervise_loop, daemon=True)
        # Checkpointing
        self.checkpoint_manager = CheckpointManager(checkpoint_path)
        self.checkpoint_interval = checkpoint_interval
        self.tasks_per_checkpoint = tasks_per_checkpoint
        self.last_checkpoint_time = time.time()
        self.completed_tasks_since_checkpoint = 0
        self.system_state: Dict[str, Any] = {
            "active_tasks": [],
            "buffer_usage": {},
            "process_health": {},
            "file_write_progress": {}
        }
        # On startup, try to load checkpoint
        restored = self.checkpoint_manager.load_checkpoint()
        if restored:
            self.system_state = restored
            self.logger.log_error('INFO', 'Restored system state from checkpoint')
        else:
            self.logger.log_error('INFO', 'No checkpoint found, starting fresh')

    def start(self) -> None:
        self.running = True
        self.supervisor_thread.start()

    def stop(self) -> None:
        self.running = False
        if self.supervisor_thread.is_alive():
            self.supervisor_thread.join(timeout=2)
        self.save_final_checkpoint()

    def update_heartbeat(self, process_id: int) -> None:
        with self.lock:
            self.heartbeats[process_id] = time.time()
            self.system_state["process_health"][process_id] = "alive"

    def task_completed(self, task_info: Any) -> None:
        """Call this when a task is completed to trigger task-based checkpointing."""
        with self.lock:
            self.completed_tasks_since_checkpoint += 1
            # Update system state as needed
            # self.system_state["active_tasks"].remove(task_info)  # Example
            if self.completed_tasks_since_checkpoint >= self.tasks_per_checkpoint:
                self.save_checkpoint()
                self.completed_tasks_since_checkpoint = 0

    def _supervise_loop(self) -> None:
        while self.running:
            now = time.time()
            with self.lock:
                for pid, last_beat in self.heartbeats.items():
                    if now - last_beat > self.heartbeat_interval * 2:
                        self._handle_failure(pid)
            # Periodic checkpoint
            if time.time() - self.last_checkpoint_time >= self.checkpoint_interval:
                self.save_checkpoint()
            # Periodic checkpoint callback
            if self.checkpoint_callback:
                try:
                    self.checkpoint_callback()
                    self.logger.log_error('INFO', 'Checkpoint callback completed')
                except Exception as e:
                    self.logger.log_error('ERROR', f'Checkpoint callback failed: {e}')
            time.sleep(self.heartbeat_interval)

    def save_checkpoint(self) -> None:
        """Save a checkpoint of the current system state."""
        with self.lock:
            self.checkpoint_manager.save_checkpoint(self.system_state)
            self.last_checkpoint_time = time.time()
            info = self.checkpoint_manager.get_last_checkpoint_info()
            self.logger.log_error('INFO', f'Checkpoint saved: {info}')

    def save_final_checkpoint(self) -> None:
        """Save a final checkpoint before shutdown."""
        self.save_checkpoint()
        self.logger.log_error('INFO', 'Final checkpoint saved before shutdown')

    def _handle_failure(self, process_id: int) -> None:
        # Attempt to restart process (placeholder: real restart logic needed)
        self.logger.log_error('ERROR', f'Process {process_id} unresponsive, attempting restart')
        # Here you would implement actual restart logic
        self.heartbeats[process_id] = time.time()  # Simulate restart
        self.system_state["process_health"][process_id] = "restarted"
        # After restart, resume from last checkpoint
        restored = self.checkpoint_manager.load_checkpoint()
        if restored:
            self.system_state = restored
            self.logger.log_error('INFO', 'Resumed from last checkpoint after process restart')
        self.logger.log_error('INFO', f'Process {process_id} restarted')

    def soft_shutdown(self) -> None:
        self.logger.log_error('INFO', 'Supervisor initiating soft shutdown')
        self.stop()
        # Additional cleanup logic here 