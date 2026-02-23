"""Metrics collection and export for DAG simulation."""

import csv
import json
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional
from pathlib import Path

try:
    from .models import ScheduledTask
except ImportError:
    from models import ScheduledTask


@dataclass
class TaskMetrics:
    """Metrics for a single task."""

    task_id: str
    node_id: str
    scheduled_start: float
    scheduled_end: float
    scheduled_duration: float
    actual_start: float
    actual_end: float
    actual_duration: float
    uplink_time: float
    compute_time: float
    downlink_time: float
    start_delta: float  # actual_start - scheduled_start
    end_delta: float  # actual_end - scheduled_end
    duration_delta: float  # actual_duration - scheduled_duration


class MetricsCollector:
    """Collects and exports timing metrics for DAG simulation."""

    def __init__(self):
        self.task_metrics: Dict[str, TaskMetrics] = {}
        self._completion_order: List[str] = []

    def record_task_completion(self, task: ScheduledTask):
        """Record metrics when a task completes.

        Args:
            task: The completed scheduled task
        """
        actual_duration = task.actual_duration or 0.0
        actual_start = task.actual_start or 0.0
        actual_end = task.actual_end or 0.0

        metrics = TaskMetrics(
            task_id=task.task_id,
            node_id=task.node_id,
            scheduled_start=task.scheduled_start,
            scheduled_end=task.scheduled_end,
            scheduled_duration=task.scheduled_duration,
            actual_start=actual_start,
            actual_end=actual_end,
            actual_duration=actual_duration,
            uplink_time=task.uplink_time,
            compute_time=task.compute_time,
            downlink_time=task.downlink_time,
            start_delta=actual_start - task.scheduled_start,
            end_delta=actual_end - task.scheduled_end,
            duration_delta=actual_duration - task.scheduled_duration,
        )

        self.task_metrics[task.task_id] = metrics
        self._completion_order.append(task.task_id)

    def get_summary(self, heft_makespan: float, simulated_makespan: float) -> dict:
        """Generate a summary of the simulation results.

        Args:
            heft_makespan: The scheduler-predicted makespan
            simulated_makespan: The actual simulated makespan

        Returns:
            Summary dictionary
        """
        if not self.task_metrics:
            return {}

        all_metrics = list(self.task_metrics.values())

        total_uplink = sum(m.uplink_time for m in all_metrics)
        total_compute = sum(m.compute_time for m in all_metrics)
        total_downlink = sum(m.downlink_time for m in all_metrics)

        # Calculate average deltas
        avg_start_delta = sum(m.start_delta for m in all_metrics) / len(all_metrics)
        avg_end_delta = sum(m.end_delta for m in all_metrics) / len(all_metrics)
        avg_duration_delta = sum(m.duration_delta for m in all_metrics) / len(all_metrics)

        # Calculate makespan difference
        makespan_diff = simulated_makespan - heft_makespan
        makespan_diff_pct = (makespan_diff / heft_makespan) * 100 if heft_makespan > 0 else 0

        return {
            "num_tasks": len(all_metrics),
            "heft_makespan": heft_makespan,
            "simulated_makespan": simulated_makespan,
            "makespan_difference": makespan_diff,
            "makespan_difference_pct": makespan_diff_pct,
            "timing_breakdown": {
                "total_uplink_time": total_uplink,
                "total_compute_time": total_compute,
                "total_downlink_time": total_downlink,
            },
            "average_deltas": {
                "start_delta": avg_start_delta,
                "end_delta": avg_end_delta,
                "duration_delta": avg_duration_delta,
            },
        }

    def export_json(self, filepath: str, summary: Optional[dict] = None):
        """Export metrics to JSON file.

        Args:
            filepath: Output file path
            summary: Optional summary to include
        """
        output = {
            "summary": summary or {},
            "tasks": [asdict(m) for m in self.task_metrics.values()],
        }

        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w") as f:
            json.dump(output, f, indent=2)

    def export_csv(self, filepath: str):
        """Export per-task metrics to CSV file.

        Args:
            filepath: Output file path
        """
        if not self.task_metrics:
            return

        Path(filepath).parent.mkdir(parents=True, exist_ok=True)

        fieldnames = [
            "task_id",
            "node_id",
            "scheduled_start",
            "scheduled_end",
            "scheduled_duration",
            "actual_start",
            "actual_end",
            "actual_duration",
            "uplink_time",
            "compute_time",
            "downlink_time",
            "start_delta",
            "end_delta",
            "duration_delta",
        ]

        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for metrics in self.task_metrics.values():
                writer.writerow(asdict(metrics))

    def print_summary(self, heft_makespan: float, simulated_makespan: float, scheduler_name: str = "HEFT"):
        """Print a human-readable summary to console.

        Args:
            heft_makespan: The scheduler-predicted makespan
            simulated_makespan: The actual simulated makespan
            scheduler_name: Name of the scheduler (e.g., "HEFT", "MCT")
        """
        summary = self.get_summary(heft_makespan, simulated_makespan)

        print("\n" + "=" * 60)
        print("DAG SIMULATION RESULTS")
        print("=" * 60)

        print(f"\nTasks executed: {summary['num_tasks']}")

        print("\nMAKESPAN COMPARISON:")
        print(f"  {scheduler_name} predicted:  {summary['heft_makespan']:,.2f}")
        print(f"  Simulated:       {summary['simulated_makespan']:,.2f}")
        print(f"  Difference:      {summary['makespan_difference']:+,.2f} ({summary['makespan_difference_pct']:+.2f}%)")

        breakdown = summary["timing_breakdown"]
        print("\nTIMING BREAKDOWN (cumulative across all tasks):")
        print(f"  Total uplink time:   {breakdown['total_uplink_time']:,.2f}")
        print(f"  Total compute time:  {breakdown['total_compute_time']:,.2f}")
        print(f"  Total downlink time: {breakdown['total_downlink_time']:,.2f}")

        deltas = summary["average_deltas"]
        print("\nAVERAGE SCHEDULE DELTAS:")
        print(f"  Start delta:    {deltas['start_delta']:+,.2f}")
        print(f"  End delta:      {deltas['end_delta']:+,.2f}")
        print(f"  Duration delta: {deltas['duration_delta']:+,.2f}")

        print("\n" + "=" * 60)
