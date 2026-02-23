"""DAG-based task scheduling simulation module for MintEDGE.

This module can be used standalone without requiring the main mintedge package.
"""

# Use try/except to handle both package import and direct import scenarios
try:
    from .models import TaskDefinition, ScheduledTask, ComputeNode, NetworkLink
    from .loaders import load_dag, load_topology, load_schedule
    from .topology import DAGTopology
    from .scheduler import DAGScheduler
    from .metrics import MetricsCollector
    from .simulation import DAGSimulation
except ImportError:
    from models import TaskDefinition, ScheduledTask, ComputeNode, NetworkLink
    from loaders import load_dag, load_topology, load_schedule
    from topology import DAGTopology
    from scheduler import DAGScheduler
    from metrics import MetricsCollector
    from simulation import DAGSimulation

__all__ = [
    "TaskDefinition",
    "ScheduledTask",
    "ComputeNode",
    "NetworkLink",
    "load_dag",
    "load_topology",
    "load_schedule",
    "DAGTopology",
    "DAGScheduler",
    "MetricsCollector",
    "DAGSimulation",
]
