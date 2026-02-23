"""Data models for DAG-based task scheduling simulation."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class TaskDefinition:
    """Definition of a task in the DAG."""

    task_id: str
    compute_cost: float  # Computational cost (arbitrary units)
    successors: List[str] = field(default_factory=list)
    predecessors: List[str] = field(default_factory=list)
    edge_weights: Dict[str, float] = field(default_factory=dict)  # data size to each successor

    def get_data_to_successor(self, successor_id: str) -> float:
        """Get the data size to be transferred to a successor task."""
        return self.edge_weights.get(successor_id, 0.0)


@dataclass
class ScheduledTask:
    """A task with its schedule assignment."""

    task_id: str
    node_id: str  # Assigned compute node
    scheduled_start: float
    scheduled_end: float
    scheduled_duration: float

    # Actual execution times (filled during simulation)
    actual_start: Optional[float] = None
    actual_end: Optional[float] = None
    uplink_time: float = 0.0
    compute_time: float = 0.0
    downlink_time: float = 0.0

    @property
    def actual_duration(self) -> Optional[float]:
        """Calculate actual duration from start and end times."""
        if self.actual_start is not None and self.actual_end is not None:
            return self.actual_end - self.actual_start
        return None


@dataclass
class ComputeNode:
    """A compute node in the network topology."""

    node_id: str
    speed_multiplier: float  # Higher = faster computation
    node_type: str  # DU, aggregation, cloud

    def get_capacity(self, base_speed: float) -> float:
        """Get the effective compute capacity.

        Args:
            base_speed: Base compute speed (operations per time unit)

        Returns:
            Effective capacity = base_speed * speed_multiplier
        """
        return base_speed * self.speed_multiplier

    def get_compute_time(self, compute_cost: float, base_speed: float) -> float:
        """Calculate time to execute a task with given compute cost.

        Args:
            compute_cost: Computational cost of the task
            base_speed: Base compute speed

        Returns:
            Time to complete the computation
        """
        capacity = self.get_capacity(base_speed)
        if capacity <= 0:
            raise ValueError(f"Node {self.node_id} has invalid capacity: {capacity}")
        return compute_cost / capacity


@dataclass
class NetworkLink:
    """A network link between compute nodes."""

    source: str
    target: str
    bandwidth: float  # Mbps (higher = faster transfer)
    link_type: str  # fronthaul, inter-DU, core, self-loop

    def get_transfer_time(self, data_size_bytes: float) -> float:
        """Calculate time to transfer data over this link.

        Following MintEDGE pattern: transfer_time = data_size / bandwidth

        Args:
            data_size_bytes: Size of data in bytes

        Returns:
            Transfer time in the same time unit as the simulation
        """
        if self.bandwidth <= 0:
            raise ValueError(f"Link {self.source}->{self.target} has invalid bandwidth: {self.bandwidth}")

        # Convert Mbps to bytes per time unit
        # bandwidth is in Mbps, data_size is in bytes
        # 1 Mbps = 1,000,000 bits/sec = 125,000 bytes/sec
        bytes_per_second = self.bandwidth * 1e6 / 8
        return data_size_bytes / bytes_per_second
