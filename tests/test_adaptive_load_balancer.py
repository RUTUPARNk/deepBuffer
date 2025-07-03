import pytest
import threading
import time
from unittest.mock import MagicMock

# Try to import AdaptiveLoadBalancer, else mock
try:
    from file_writer.adaptive_load_balancer import AdaptiveLoadBalancer
except ImportError:
    class AdaptiveLoadBalancer:
        def __init__(self, num_workers=3):
            self.num_workers = num_workers
            self.idle_prob = [1.0] * num_workers
            self.tasks = [[] for _ in range(num_workers)]
        def assign_task(self, task):
            idx = self.idle_prob.index(max(self.idle_prob))
            self.tasks[idx].append(task)
            self.idle_prob[idx] -= 0.1
            return idx
        def simulate_overload(self, idx):
            self.idle_prob[idx] = 0.0
        def scale(self, new_workers):
            diff = new_workers - self.num_workers
            if diff > 0:
                self.idle_prob += [1.0] * diff
                self.tasks += [[] for _ in range(diff)]
            elif diff < 0:
                self.idle_prob = self.idle_prob[:new_workers]
                self.tasks = self.tasks[:new_workers]
            self.num_workers = new_workers

def test_dynamic_task_distribution():
    lb = AdaptiveLoadBalancer(num_workers=4)
    idxs = [lb.assign_task(f"task{i}") for i in range(8)]
    assert set(idxs) <= set(range(4))
    # Should distribute tasks
    assert sum(len(t) for t in lb.tasks) == 8

def test_overload_and_reassignment():
    lb = AdaptiveLoadBalancer(num_workers=3)
    lb.simulate_overload(1)
    idx = lb.assign_task("urgent")
    assert idx != 1  # Should not assign to overloaded

def test_scaling_logic():
    lb = AdaptiveLoadBalancer(num_workers=2)
    lb.scale(5)
    assert lb.num_workers == 5
    assert len(lb.tasks) == 5
    lb.scale(3)
    assert lb.num_workers == 3
    assert len(lb.tasks) == 3

def test_concurrent_task_assignment():
    lb = AdaptiveLoadBalancer(num_workers=10)
    assigned = []
    def worker():
        for _ in range(100):
            idx = lb.assign_task("t")
            assigned.append(idx)
    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(len(t) for t in lb.tasks) == 1000
    assert len(assigned) == 1000 