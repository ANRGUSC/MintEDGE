"""Main entry point for DAG simulation."""

from typing import Optional, Tuple

import simpy

try:
    from .loaders import load_dag, load_topology, load_schedule
    from .topology import DAGTopology
    from .scheduler import DAGScheduler
    from .metrics import MetricsCollector
except ImportError:
    from loaders import load_dag, load_topology, load_schedule
    from topology import DAGTopology
    from scheduler import DAGScheduler
    from metrics import MetricsCollector


class DAGSimulation:
    """Orchestrates the DAG-based task scheduling simulation.

    This class loads the input files, sets up the simulation environment,
    runs the simulation, and exports the results.
    """

    def __init__(
        self,
        dag_path: str,
        topology_path: str,
        schedule_path: str,
        base_compute_speed: float = 1.0,
    ):
        """Initialize the simulation.

        Args:
            dag_path: Path to dag.json
            topology_path: Path to network_topology.json
            schedule_path: Path to schedule file
            base_compute_speed: Base compute speed for all nodes
        """
        self.dag_path = dag_path
        self.topology_path = topology_path
        self.schedule_path = schedule_path
        self.base_compute_speed = base_compute_speed

        # Loaded data
        self.task_definitions = None
        self.dag_metadata = None
        self.nodes = None
        self.links = None
        self.scheduled_tasks = None
        self.task_to_node = None
        self.schedule_metadata = None

        # Simulation components
        self.topology = None
        self.metrics = None
        self.scheduler = None

        # Results
        self.heft_makespan = None
        self.simulated_makespan = None

    def load(self):
        """Load all input files."""
        # Load DAG
        self.task_definitions, self.dag_metadata = load_dag(self.dag_path)
        print(f"Loaded {len(self.task_definitions)} task definitions from {self.dag_path}")

        # Load topology
        self.nodes, self.links = load_topology(self.topology_path)
        print(f"Loaded {len(self.nodes)} nodes and {len(self.links)} links from {self.topology_path}")

        # Load schedule
        self.scheduled_tasks, self.task_to_node, self.schedule_metadata = load_schedule(
            self.schedule_path
        )
        self.heft_makespan = self.schedule_metadata.get("makespan", 0.0)
        self.scheduler_name = self.schedule_metadata.get("scheduler", "Schedule")
        print(f"Loaded {len(self.scheduled_tasks)} scheduled tasks from {self.schedule_path}")
        print(f"{self.scheduler_name} makespan: {self.heft_makespan:,.2f}")

    def setup(self):
        """Set up the simulation components."""
        # Build topology
        self.topology = DAGTopology(self.nodes, self.links)

        # Initialize metrics collector
        self.metrics = MetricsCollector()

    def run(self) -> float:
        """Run the simulation.

        Returns:
            The simulated makespan
        """
        # Create SimPy environment
        env = simpy.Environment()

        # Create scheduler
        self.scheduler = DAGScheduler(
            env=env,
            topology=self.topology,
            task_definitions=self.task_definitions,
            scheduled_tasks=self.scheduled_tasks,
            metrics=self.metrics,
            base_compute_speed=self.base_compute_speed,
        )

        print("\nRunning simulation...")
        self.simulated_makespan = self.scheduler.run()
        print(f"Simulation complete. Makespan: {self.simulated_makespan:,.2f}")

        return self.simulated_makespan

    def print_summary(self):
        """Print simulation summary to console."""
        if self.metrics and self.heft_makespan is not None and self.simulated_makespan is not None:
            self.metrics.print_summary(self.heft_makespan, self.simulated_makespan, self.scheduler_name)

    def export_results(self, output_dir: str):
        """Export results to JSON and CSV files.

        Args:
            output_dir: Output directory path
        """
        if not self.metrics:
            print("No metrics to export")
            return

        summary = self.metrics.get_summary(self.heft_makespan, self.simulated_makespan)

        # Add input file info to summary
        summary["input_files"] = {
            "dag": self.dag_path,
            "topology": self.topology_path,
            "schedule": self.schedule_path,
        }
        summary["configuration"] = {
            "base_compute_speed": self.base_compute_speed,
        }

        # Export JSON
        json_path = f"{output_dir}/simulation_results.json"
        self.metrics.export_json(json_path, summary)
        print(f"Exported JSON results to {json_path}")

        # Export CSV
        csv_path = f"{output_dir}/task_metrics.csv"
        self.metrics.export_csv(csv_path)
        print(f"Exported CSV metrics to {csv_path}")

    def run_full(self, output_dir: Optional[str] = None) -> Tuple[float, float]:
        """Run the full simulation pipeline.

        Args:
            output_dir: Optional output directory for results

        Returns:
            Tuple of (heft_makespan, simulated_makespan)
        """
        self.load()
        self.setup()
        self.run()
        self.print_summary()

        if output_dir:
            self.export_results(output_dir)

        return self.heft_makespan, self.simulated_makespan
