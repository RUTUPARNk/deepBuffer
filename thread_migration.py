import threading
import time
import queue
from typing import Dict, List, Optional, Callable, Any, Tuple
from dataclasses import dataclass
from collections import defaultdict, deque
import statistics
import psutil


@dataclass
class ThreadStats:
    """Statistics for a single thread."""
    thread_id: int
    idle_probability: float
    current_load: float
    queue_size: int
    avg_task_duration: float
    last_task_time: float
    cpu_usage: float
    memory_usage: float
    is_overloaded: bool = False


@dataclass
class MigrationDecision:
    """Decision for task migration."""
    source_thread: int
    target_thread: int
    task_id: str
    reason: str
    expected_improvement: float


class ThreadMigrationScheduler:
    """Dynamic thread migration scheduler based on live idleness statistics."""
    
    def __init__(self, num_threads: int = 4, migration_threshold: float = 0.3):
        self.num_threads = num_threads
        self.migration_threshold = migration_threshold
        
        # Thread statistics tracking
        self.thread_stats: Dict[int, ThreadStats] = {}
        self.stats_history: Dict[int, deque] = defaultdict(lambda: deque(maxlen=50))
        
        # Migration tracking
        self.migration_history: List[MigrationDecision] = []
        self.migration_lock = threading.Lock()
        
        # Performance monitoring
        self.performance_metrics = {
            "total_migrations": 0,
            "successful_migrations": 0,
            "failed_migrations": 0,
            "avg_migration_improvement": 0.0
        }
        
        # Initialize thread statistics
        self._init_thread_stats()
        
        # Start monitoring thread
        self.monitoring_active = True
        self.monitor_thread = threading.Thread(target=self._monitor_threads, daemon=True)
        self.monitor_thread.start()
    
    def _init_thread_stats(self):
        """Initialize statistics for all threads."""
        for i in range(self.num_threads):
            self.thread_stats[i] = ThreadStats(
                thread_id=i,
                idle_probability=1.0,
                current_load=0.0,
                queue_size=0,
                avg_task_duration=0.0,
                last_task_time=time.time(),
                cpu_usage=0.0,
                memory_usage=0.0,
                is_overloaded=False
            )
    
    def update_thread_stats(self, thread_id: int, stats: Dict):
        """Update statistics for a specific thread."""
        if thread_id not in self.thread_stats:
            return
        
        # Update current stats
        current_stats = self.thread_stats[thread_id]
        current_stats.idle_probability = stats.get("idle_probability", current_stats.idle_probability)
        current_stats.current_load = stats.get("current_load", current_stats.current_load)
        current_stats.queue_size = stats.get("queue_size", current_stats.queue_size)
        current_stats.avg_task_duration = stats.get("avg_task_duration", current_stats.avg_task_duration)
        current_stats.cpu_usage = stats.get("cpu_usage", current_stats.cpu_usage)
        current_stats.memory_usage = stats.get("memory_usage", current_stats.memory_usage)
        
        # Update history
        self.stats_history[thread_id].append({
            "timestamp": time.time(),
            "idle_probability": current_stats.idle_probability,
            "current_load": current_stats.current_load,
            "queue_size": current_stats.queue_size,
            "cpu_usage": current_stats.cpu_usage
        })
        
        # Check for overload conditions
        self._check_overload_conditions(thread_id)
    
    def _check_overload_conditions(self, thread_id: int):
        """Check if a thread is overloaded based on various metrics."""
        stats = self.thread_stats[thread_id]
        
        # Multiple overload indicators
        overload_indicators = [
            stats.queue_size > 5,  # Large queue
            stats.cpu_usage > 90.0,  # High CPU usage
            stats.current_load > 0.8,  # High load
            stats.idle_probability < 0.1  # Low idle time
        ]
        
        # Thread is overloaded if majority of indicators are true
        stats.is_overloaded = sum(overload_indicators) >= 2
    
    def should_migrate_tasks(self) -> bool:
        """Determine if task migration is beneficial."""
        # Check if there are significant load imbalances
        loads = [stats.current_load for stats in self.thread_stats.values()]
        if not loads:
            return False
        
        load_variance = statistics.variance(loads)
        avg_load = statistics.mean(loads)
        
        # Migrate if there's high variance in load distribution
        return load_variance > 0.1 and avg_load > 0.3
    
    def find_migration_candidates(self) -> List[MigrationDecision]:
        """Find tasks that should be migrated between threads."""
        if not self.should_migrate_tasks():
            return []
        
        # Find overloaded and underutilized threads
        overloaded_threads = [tid for tid, stats in self.thread_stats.items() 
                            if stats.is_overloaded]
        underutilized_threads = [tid for tid, stats in self.thread_stats.items() 
                               if stats.idle_probability > 0.5 and stats.queue_size < 2]
        
        migration_candidates = []
        
        for source_tid in overloaded_threads:
            for target_tid in underutilized_threads:
                if source_tid == target_tid:
                    continue
                
                # Calculate expected improvement
                improvement = self._calculate_migration_improvement(source_tid, target_tid)
                
                if improvement > self.migration_threshold:
                    migration_candidates.append(MigrationDecision(
                        source_thread=source_tid,
                        target_thread=target_tid,
                        task_id=f"task_{source_tid}_{int(time.time())}",
                        reason="load_balancing",
                        expected_improvement=improvement
                    ))
        
        return migration_candidates
    
    def _calculate_migration_improvement(self, source_thread: int, target_thread: int) -> float:
        """Calculate the expected improvement from migrating a task."""
        source_stats = self.thread_stats[source_thread]
        target_stats = self.thread_stats[target_thread]
        
        # Calculate load reduction for source thread
        source_load_reduction = source_stats.current_load * 0.2  # Assume 20% load reduction
        
        # Calculate load increase for target thread
        target_load_increase = target_stats.current_load * 0.3  # Assume 30% load increase
        
        # Net improvement
        improvement = source_load_reduction - target_load_increase
        
        # Consider idle probability
        if target_stats.idle_probability > 0.7:
            improvement *= 1.5  # Bonus for very idle threads
        
        return max(0, improvement)
    
    def execute_migration(self, migration: MigrationDecision) -> bool:
        """Execute a task migration."""
        try:
            with self.migration_lock:
                # Record migration attempt
                self.migration_history.append(migration)
                self.performance_metrics["total_migrations"] += 1
                
                # Simulate migration (in real implementation, this would move actual tasks)
                print(f"Migrating task {migration.task_id} from thread {migration.source_thread} "
                      f"to thread {migration.target_thread} (expected improvement: {migration.expected_improvement:.3f})")
                
                # Update thread statistics to reflect migration
                self._update_stats_after_migration(migration)
                
                self.performance_metrics["successful_migrations"] += 1
                return True
                
        except Exception as e:
            print(f"Migration failed: {e}")
            self.performance_metrics["failed_migrations"] += 1
            return False
    
    def _update_stats_after_migration(self, migration: MigrationDecision):
        """Update thread statistics after a migration."""
        source_stats = self.thread_stats[migration.source_thread]
        target_stats = self.thread_stats[migration.target_thread]
        
        # Reduce load on source thread
        source_stats.current_load *= 0.8
        source_stats.queue_size = max(0, source_stats.queue_size - 1)
        
        # Increase load on target thread
        target_stats.current_load = min(1.0, target_stats.current_load * 1.2)
        target_stats.queue_size += 1
        
        # Recalculate overload conditions
        self._check_overload_conditions(migration.source_thread)
        self._check_overload_conditions(migration.target_thread)
    
    def get_optimal_thread_for_task(self, task_complexity: float = 1.0) -> int:
        """Find the optimal thread for a new task based on current statistics."""
        best_thread = 0
        best_score = float('-inf')
        
        for thread_id, stats in self.thread_stats.items():
            # Calculate thread score based on multiple factors
            score = self._calculate_thread_score(thread_id, task_complexity)
            
            if score > best_score:
                best_score = score
                best_thread = thread_id
        
        return best_thread
    
    def _calculate_thread_score(self, thread_id: int, task_complexity: float) -> float:
        """Calculate a score for how suitable a thread is for a task."""
        stats = self.thread_stats[thread_id]
        
        # Base score from idle probability
        score = stats.idle_probability * 10
        
        # Penalty for high queue size
        score -= stats.queue_size * 2
        
        # Penalty for high CPU usage
        score -= (stats.cpu_usage / 100) * 5
        
        # Bonus for low memory usage
        score += (1 - stats.memory_usage / 100) * 2
        
        # Penalty for overloaded threads
        if stats.is_overloaded:
            score -= 10
        
        # Consider task complexity
        if task_complexity > 1.0:
            # Prefer less loaded threads for complex tasks
            score -= stats.current_load * 5
        
        return score
    
    def _monitor_threads(self):
        """Background thread for monitoring and automatic migration."""
        while self.monitoring_active:
            try:
                # Check for migration opportunities
                migration_candidates = self.find_migration_candidates()
                
                for migration in migration_candidates:
                    if migration.expected_improvement > self.migration_threshold:
                        self.execute_migration(migration)
                
                # Update performance metrics
                self._update_performance_metrics()
                
                # Sleep before next check
                time.sleep(2)  # Check every 2 seconds
                
            except Exception as e:
                print(f"Error in thread monitoring: {e}")
                time.sleep(5)
    
    def _update_performance_metrics(self):
        """Update performance metrics based on migration history."""
        if not self.migration_history:
            return
        
        recent_migrations = self.migration_history[-10:]  # Last 10 migrations
        improvements = [m.expected_improvement for m in recent_migrations]
        
        if improvements:
            self.performance_metrics["avg_migration_improvement"] = statistics.mean(improvements)
    
    def get_migration_stats(self) -> Dict:
        """Get comprehensive migration statistics."""
        return {
            "thread_stats": {tid: {
                "idle_probability": stats.idle_probability,
                "current_load": stats.current_load,
                "queue_size": stats.queue_size,
                "cpu_usage": stats.cpu_usage,
                "memory_usage": stats.memory_usage,
                "is_overloaded": stats.is_overloaded
            } for tid, stats in self.thread_stats.items()},
            "performance_metrics": self.performance_metrics.copy(),
            "recent_migrations": len(self.migration_history[-10:]),
            "load_balance_score": self._calculate_load_balance_score()
        }
    
    def _calculate_load_balance_score(self) -> float:
        """Calculate how well balanced the load is across threads."""
        loads = [stats.current_load for stats in self.thread_stats.values()]
        if not loads:
            return 1.0
        
        # Perfect balance would have zero variance
        variance = statistics.variance(loads)
        # Convert to a score between 0 and 1 (1 = perfectly balanced)
        balance_score = max(0, 1 - variance)
        
        return balance_score
    
    def get_recommendations(self) -> List[str]:
        """Get recommendations for improving thread utilization."""
        recommendations = []
        
        # Check for overloaded threads
        overloaded_count = sum(1 for stats in self.thread_stats.values() if stats.is_overloaded)
        if overloaded_count > 0:
            recommendations.append(f"Consider adding {overloaded_count} more threads to handle overload")
        
        # Check for underutilized threads
        underutilized_count = sum(1 for stats in self.thread_stats.values() 
                                if stats.idle_probability > 0.7)
        if underutilized_count > 0:
            recommendations.append(f"{underutilized_count} threads are underutilized - consider reducing thread count")
        
        # Check migration effectiveness
        if self.performance_metrics["total_migrations"] > 0:
            success_rate = (self.performance_metrics["successful_migrations"] / 
                          self.performance_metrics["total_migrations"])
            if success_rate < 0.8:
                recommendations.append("Migration success rate is low - review migration strategy")
        
        return recommendations
    
    def shutdown(self):
        """Shutdown the migration scheduler."""
        self.monitoring_active = False
        if self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5)


class AdaptiveMigrationScheduler(ThreadMigrationScheduler):
    """Enhanced migration scheduler that adapts its strategy based on performance."""
    
    def __init__(self, num_threads: int = 4, migration_threshold: float = 0.3):
        super().__init__(num_threads, migration_threshold)
        
        # Adaptive parameters
        self.adaptive_threshold = migration_threshold
        self.performance_window = deque(maxlen=100)
        self.strategy_history = []
        
        # Learning parameters
        self.success_weight = 0.1
        self.threshold_adjustment_rate = 0.05
    
    def execute_migration(self, migration: MigrationDecision) -> bool:
        """Execute migration with adaptive learning."""
        success = super().execute_migration(migration)
        
        # Record performance for learning
        self.performance_window.append({
            "migration": migration,
            "success": success,
            "timestamp": time.time(),
            "threshold": self.adaptive_threshold
        })
        
        # Adapt strategy based on recent performance
        self._adapt_strategy()
        
        return success
    
    def _adapt_strategy(self):
        """Adapt migration strategy based on recent performance."""
        if len(self.performance_window) < 10:
            return
        
        recent_performances = self.performance_window[-10:]
        success_rate = sum(1 for p in recent_performances if p["success"]) / len(recent_performances)
        
        # Adjust threshold based on success rate
        if success_rate > 0.8:
            # High success rate - can be more aggressive
            self.adaptive_threshold *= (1 - self.threshold_adjustment_rate)
        elif success_rate < 0.5:
            # Low success rate - be more conservative
            self.adaptive_threshold *= (1 + self.threshold_adjustment_rate)
        
        # Keep threshold within reasonable bounds
        self.adaptive_threshold = max(0.1, min(0.8, self.adaptive_threshold))
        
        # Record strategy change
        self.strategy_history.append({
            "timestamp": time.time(),
            "new_threshold": self.adaptive_threshold,
            "success_rate": success_rate
        })
    
    def get_adaptive_stats(self) -> Dict:
        """Get adaptive learning statistics."""
        return {
            "adaptive_threshold": self.adaptive_threshold,
            "performance_window_size": len(self.performance_window),
            "strategy_changes": len(self.strategy_history),
            "recent_success_rate": self._calculate_recent_success_rate()
        }
    
    def _calculate_recent_success_rate(self) -> float:
        """Calculate success rate of recent migrations."""
        if not self.performance_window:
            return 0.0
        
        recent = self.performance_window[-20:]  # Last 20 migrations
        return sum(1 for p in recent if p["success"]) / len(recent) 