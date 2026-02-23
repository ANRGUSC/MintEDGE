"""JSON parsers for DAG, topology, and schedule files."""

import json
from typing import Dict, List, Tuple

try:
    from .models import TaskDefinition, ScheduledTask, ComputeNode, NetworkLink
except ImportError:
    from models import TaskDefinition, ScheduledTask, ComputeNode, NetworkLink


def load_dag(filepath: str) -> Tuple[Dict[str, TaskDefinition], Dict[str, dict]]:
    """Load task definitions from dag.json.

    Args:
        filepath: Path to dag.json

    Returns:
        Tuple of (task_definitions dict, metadata dict)
    """
    with open(filepath, "r") as f:
        data = json.load(f)

    tasks: Dict[str, TaskDefinition] = {}

    # Get node weights (compute costs)
    node_weights = data.get("node_weights", {})

    # Get DAG structure (edges: task -> list of successors)
    dag_structure = data.get("dag_structure", {})
    edges = dag_structure.get("edges", {})

    # Get edge weights (data sizes between tasks)
    edge_weights_raw = data.get("edge_weights", {})

    # Parse edge weights - keys are like "('T0', 'T1')"
    edge_weights: Dict[str, Dict[str, float]] = {}
    for key, value in edge_weights_raw.items():
        # Parse the tuple string
        key_clean = key.strip("()").replace("'", "").replace(" ", "")
        parts = key_clean.split(",")
        if len(parts) == 2:
            src, dst = parts
            if src not in edge_weights:
                edge_weights[src] = {}
            edge_weights[src][dst] = float(value)

    # Build predecessor map
    predecessors: Dict[str, List[str]] = {}
    for task_id in node_weights:
        predecessors[task_id] = []

    for src, successors in edges.items():
        for dst in successors:
            if dst not in predecessors:
                predecessors[dst] = []
            predecessors[dst].append(src)

    # Create TaskDefinition objects
    for task_id, compute_cost in node_weights.items():
        tasks[task_id] = TaskDefinition(
            task_id=task_id,
            compute_cost=float(compute_cost),
            successors=edges.get(task_id, []),
            predecessors=predecessors.get(task_id, []),
            edge_weights=edge_weights.get(task_id, {}),
        )

    metadata = {
        "configuration": data.get("configuration", {}),
        "metadata": data.get("metadata", {}),
    }

    return tasks, metadata


def load_topology(filepath: str) -> Tuple[Dict[str, ComputeNode], List[NetworkLink]]:
    """Load network topology from network_topology.json.

    Args:
        filepath: Path to network_topology.json

    Returns:
        Tuple of (compute_nodes dict, network_links list)
    """
    with open(filepath, "r") as f:
        data = json.load(f)

    nodes: Dict[str, ComputeNode] = {}
    links: List[NetworkLink] = []

    # Load compute nodes
    for node_data in data.get("nodes", []):
        node_id = node_data["id"]
        nodes[node_id] = ComputeNode(
            node_id=node_id,
            speed_multiplier=float(node_data.get("weight", 1)),
            node_type=node_data.get("type", "unknown"),
        )

    # Load network links
    for edge_data in data.get("edges", []):
        link = NetworkLink(
            source=edge_data["source"],
            target=edge_data["target"],
            bandwidth=float(edge_data.get("weight", 1)),
            link_type=edge_data.get("type", "unknown"),
        )
        links.append(link)

    return nodes, links


def load_schedule(filepath: str) -> Tuple[Dict[str, ScheduledTask], Dict[str, str], dict]:
    """Load schedule from a schedule JSON file.

    Args:
        filepath: Path to schedule file

    Returns:
        Tuple of (scheduled_tasks dict, task_to_node mapping, metadata)
    """
    with open(filepath, "r") as f:
        data = json.load(f)

    scheduled_tasks: Dict[str, ScheduledTask] = {}
    task_to_node: Dict[str, str] = data.get("task_to_node", {})

    for entry in data.get("schedule", []):
        task_id = entry["task"]
        scheduled_tasks[task_id] = ScheduledTask(
            task_id=task_id,
            node_id=entry["node"],
            scheduled_start=float(entry["start"]),
            scheduled_end=float(entry["end"]),
            scheduled_duration=float(entry["duration"]),
        )

    metadata = data.get("metadata", {})

    return scheduled_tasks, task_to_node, metadata
