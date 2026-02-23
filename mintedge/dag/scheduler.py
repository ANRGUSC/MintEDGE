"""SimPy-based task execution scheduler for DAG simulation."""

from typing import Callable, Dict, List, Optional

import simpy

try:
    from .models import TaskDefinition, ScheduledTask
    from .topology import DAGTopology
    from .metrics import MetricsCollector
except ImportError:
    from models import TaskDefinition, ScheduledTask
    from topology import DAGTopology
    from metrics import MetricsCollector


class DAGScheduler:
    """SimPy-based scheduler for executing DAG tasks.

    Replays the schedule, simulating task execution with
    uplink, compute, and downlink phases.
    """

    def __init__(
        self,
        env: simpy.Environment,
        topology: DAGTopology,
        task_definitions: Dict[str, TaskDefinition],
        scheduled_tasks: Dict[str, ScheduledTask],
        metrics: MetricsCollector,
        base_compute_speed: float = 1.0,
    ):
        """Initialize the scheduler.

        Args:
            env: SimPy environment
            topology: Network topology
            task_definitions: Task definitions from DAG
            scheduled_tasks: Scheduled tasks
            metrics: Metrics collector
            base_compute_speed: Base compute speed for all nodes
        """
        self.env = env
        self.topology = topology
        self.task_definitions = task_definitions
        self.scheduled_tasks = scheduled_tasks
        self.metrics = metrics
        self.base_compute_speed = base_compute_speed

        # Completion events for each task
        self.completion_events: Dict[str, simpy.Event] = {}

        # Map from scheduled task ID to base task ID
        # e.g., "T0_c6" -> "T0"
        self._task_id_map: Dict[str, str] = {}

    def _get_base_task_id(self, scheduled_task_id: str) -> str:
        """Extract base task ID from scheduled task ID.

        e.g., "T0_c6" -> "T0"
        """
        if scheduled_task_id in self._task_id_map:
            return self._task_id_map[scheduled_task_id]

        # Try to parse as "Tx_cy" format
        parts = scheduled_task_id.rsplit("_", 1)
        if len(parts) == 2 and parts[1].startswith("c"):
            base_id = parts[0]
        else:
            # Assume it's already a base task ID
            base_id = scheduled_task_id

        self._task_id_map[scheduled_task_id] = base_id
        return base_id

    def _get_predecessors(self, scheduled_task_id: str) -> List[str]:
        """Get predecessor scheduled task IDs for a given task.

        Maps from base task predecessors to scheduled task predecessors.
        For multi-cell tasks like T0_c6, predecessors are tasks from
        the same cell.
        """
        base_task_id = self._get_base_task_id(scheduled_task_id)
        task_def = self.task_definitions.get(base_task_id)

        if not task_def or not task_def.predecessors:
            return []

        # Extract cell suffix if present
        parts = scheduled_task_id.rsplit("_", 1)
        cell_suffix = parts[1] if len(parts) == 2 and parts[1].startswith("c") else None

        predecessors = []
        for pred_base_id in task_def.predecessors:
            if cell_suffix:
                # Look for predecessor with same cell suffix
                pred_scheduled_id = f"{pred_base_id}_{cell_suffix}"
                if pred_scheduled_id in self.scheduled_tasks:
                    predecessors.append(pred_scheduled_id)
            else:
                # No cell suffix - use base task ID
                if pred_base_id in self.scheduled_tasks:
                    predecessors.append(pred_base_id)

        return predecessors

    def _get_data_from_predecessor(
        self,
        pred_task_id: str,
        current_task_id: str,
    ) -> float:
        """Get data size from a predecessor task.

        Args:
            pred_task_id: Predecessor scheduled task ID
            current_task_id: Current scheduled task ID

        Returns:
            Data size in bytes
        """
        pred_base_id = self._get_base_task_id(pred_task_id)
        current_base_id = self._get_base_task_id(current_task_id)

        task_def = self.task_definitions.get(pred_base_id)
        if not task_def:
            return 0.0

        return task_def.get_data_to_successor(current_base_id)

    def _execute_task(self, scheduled_task: ScheduledTask):
        """Execute a single task with dependencies.

        This is a SimPy process that:
        1. Waits for all predecessors to complete
        2. Transfers data from predecessors (uplink phase)
        3. Executes computation
        4. Signals completion
        """
        task_id = scheduled_task.task_id
        base_task_id = self._get_base_task_id(task_id)
        task_def = self.task_definitions.get(base_task_id)

        if not task_def:
            raise ValueError(f"No task definition found for {base_task_id}")

        # Record actual start time (before waiting for predecessors)
        wait_start = self.env.now

        # 1. Wait for all predecessors
        predecessors = self._get_predecessors(task_id)
        if predecessors:
            pred_events = [
                self.completion_events[pred_id]
                for pred_id in predecessors
                if pred_id in self.completion_events
            ]
            if pred_events:
                yield simpy.AllOf(self.env, pred_events)
                                                                                                     
        scheduled_task.actual_start = self.env.now                                                                                                                                     
        # 2. Uplink: transfer data FROM predecessors TO this node   
        uplink_time = 0.0
        if predecessors:
            transfer_times = []
            for pred_id in predecessors:
                pred_task = self.scheduled_tasks.get(pred_id)
                if pred_task:
                    data_size = self._get_data_from_predecessor(pred_id, task_id)
                    if data_size > 0:
                        transfer_time = self.topology.get_transfer_time(
                            pred_task.node_id,
                            scheduled_task.node_id,
                            data_size,
                        )
                        transfer_times.append(transfer_time)

            # Parallel transfers - take the maximum time
            if transfer_times:
                uplink_time = max(transfer_times)

        scheduled_task.uplink_time = uplink_time
        if uplink_time > 0:
            yield self.env.timeout(uplink_time)
        # Record actual start time (after data arrival and node availability)
        scheduled_task.actual_start = self.env.now

        # 3. Compute: process task on assigned node
        compute_time = self.topology.get_compute_time(
            scheduled_task.node_id,
            task_def.compute_cost,
            self.base_compute_speed,
        )
        scheduled_task.compute_time = compute_time
        yield self.env.timeout(compute_time)

        # Record actual end time
        scheduled_task.actual_end = self.env.now

        self.completion_events[task_id].succeed()

        # Record metrics
        self.metrics.record_task_completion(scheduled_task)

    def run(self) -> float:
        """Run the simulation.

        Returns:
            The makespan (total execution time)
        """
        # Create completion events for all tasks
        for task_id in self.scheduled_tasks:
            self.completion_events[task_id] = self.env.event()

        # Start all task processes
        processes = []
        for task_id, scheduled_task in self.scheduled_tasks.items():
            proc = self.env.process(self._execute_task(scheduled_task))
            processes.append(proc)

        # Run until all tasks complete
        self.env.run()

        # Calculate makespan
        makespan = max(
            task.actual_end
            for task in self.scheduled_tasks.values()
            if task.actual_end is not None
        )

        return makespan
