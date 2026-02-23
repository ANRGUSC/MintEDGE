"""Network topology management with path finding."""

from typing import Dict, List, Optional, Tuple

import networkx as nx

try:
    from .models import ComputeNode, NetworkLink
except ImportError:
    from models import ComputeNode, NetworkLink


class DAGTopology:
    """Network topology graph with Dijkstra path finding.

    Follows MintEDGE infrastructure patterns for delay calculation.
    """

    def __init__(
        self,
        nodes: Dict[str, ComputeNode],
        links: List[NetworkLink],
    ):
        """Initialize the topology.

        Args:
            nodes: Dictionary of compute nodes
            links: List of network links
        """
        self.nodes = nodes
        self.links = links
        self.graph = nx.Graph()
        self._link_map: Dict[Tuple[str, str], NetworkLink] = {}

        self._build_graph()

    def _build_graph(self):
        """Build the NetworkX graph from nodes and links."""
        # Add nodes
        for node_id, node in self.nodes.items():
            self.graph.add_node(node_id, compute_node=node)

        # Add edges with bandwidth as weight
        # For path finding, we want to minimize delay, which is inversely proportional to bandwidth
        # So we use 1/bandwidth as the edge weight for Dijkstra
        for link in self.links:
            # Skip self-loops for path finding (they're used for local compute)
            if link.source == link.target:
                # Store self-loops separately
                self._link_map[(link.source, link.target)] = link
                continue

            # Use inverse bandwidth as weight (lower weight = preferred path)
            # This ensures higher bandwidth paths are preferred
            weight = 1.0 / link.bandwidth if link.bandwidth > 0 else float("inf")

            # NetworkX Graph is undirected, so we add the edge once
            # Store both directions in link map
            self.graph.add_edge(
                link.source,
                link.target,
                weight=weight,
                link=link,
            )
            self._link_map[(link.source, link.target)] = link
            self._link_map[(link.target, link.source)] = link

    def get_node(self, node_id: str) -> Optional[ComputeNode]:
        """Get a compute node by ID."""
        return self.nodes.get(node_id)

    def get_link(self, source: str, target: str) -> Optional[NetworkLink]:
        """Get the link between two nodes."""
        return self._link_map.get((source, target))

    def get_path(self, source: str, target: str) -> List[str]:
        """Find the shortest path between two nodes using Dijkstra.

        Args:
            source: Source node ID
            target: Target node ID

        Returns:
            List of node IDs forming the path (including source and target)
        """
        if source == target:
            return [source]

        try:
            return nx.dijkstra_path(self.graph, source, target, weight="weight")
        except nx.NetworkXNoPath:
            raise ValueError(f"No path exists between {source} and {target}")
        except nx.NodeNotFound as e:
            raise ValueError(f"Node not found: {e}")

    def get_path_links(self, source: str, target: str) -> List[NetworkLink]:
        """Get all links along the shortest path.

        Args:
            source: Source node ID
            target: Target node ID

        Returns:
            List of NetworkLink objects along the path
        """
        if source == target:
            # Return self-loop if exists
            self_loop = self._link_map.get((source, target))
            return [self_loop] if self_loop else []

        path = self.get_path(source, target)
        links = []
        for i in range(len(path) - 1):
            link = self._link_map.get((path[i], path[i + 1]))
            if link:
                links.append(link)
        return links

    def get_transfer_time(
        self,
        source: str,
        target: str,
        data_size_bytes: float,
    ) -> float:
        """Calculate the total transfer time along the path.

        Following MintEDGE pattern: sum of link delays along path.

        Args:
            source: Source node ID
            target: Target node ID
            data_size_bytes: Size of data to transfer in bytes

        Returns:
            Total transfer time
        """
        if source == target:
            # Same node - no transfer time (or use self-loop bandwidth)
            self_loop = self._link_map.get((source, target))
            if self_loop:
                return self_loop.get_transfer_time(data_size_bytes)
            return 0.0

        links = self.get_path_links(source, target)
        if not links:
            raise ValueError(f"No path links found between {source} and {target}")

        # Sum delays along the path
        total_time = sum(link.get_transfer_time(data_size_bytes) for link in links)
        return total_time

    def get_compute_time(
        self,
        node_id: str,
        compute_cost: float,
        base_speed: float,
    ) -> float:
        """Calculate compute time on a node.

        Args:
            node_id: Node ID
            compute_cost: Computational cost of the task
            base_speed: Base compute speed

        Returns:
            Compute time
        """
        node = self.nodes.get(node_id)
        if not node:
            raise ValueError(f"Unknown node: {node_id}")
        return node.get_compute_time(compute_cost, base_speed)
