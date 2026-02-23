#!/usr/bin/env python3
"""CLI entry point for DAG-based task scheduling simulation."""

import argparse
import sys
from pathlib import Path

# Add the dag module directory to the path so we can import it directly
# without triggering the main mintedge package (which has libsumo dependency)
_dag_path = Path(__file__).parent / "mintedge" / "dag"
sys.path.insert(0, str(_dag_path))


def main():
    parser = argparse.ArgumentParser(
        description="Run DAG-based task scheduling simulation for MintEDGE",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--dag",
        type=str,
        default="dag.json",
        help="Path to DAG definition file",
    )

    parser.add_argument(
        "--topology",
        type=str,
        default="network_topology.json",
        help="Path to network topology file",
    )

    parser.add_argument(
        "--schedule",
        type=str,
        default="schedule_heft_7cells.json",
        help="Path to schedule file (e.g., schedule_heft_7cells.json, schedule_mct_7cells.json)",
    )

    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output directory for results (JSON and CSV). If not specified, no files are written.",
    )

    parser.add_argument(
        "--base-compute-speed",
        type=float,
        default=1.0,
        help="Base compute speed for all nodes (higher = faster computation)",
    )

    args = parser.parse_args()

    # Validate input files exist
    for path_arg, name in [
        (args.dag, "DAG"),
        (args.topology, "topology"),
        (args.schedule, "schedule"),
    ]:
        if not Path(path_arg).exists():
            print(f"Error: {name} file not found: {path_arg}", file=sys.stderr)
            sys.exit(1)

    # Import directly from the dag module path (added to sys.path above)
    from simulation import DAGSimulation

    # Run simulation
    sim = DAGSimulation(
        dag_path=args.dag,
        topology_path=args.topology,
        schedule_path=args.schedule,
        base_compute_speed=args.base_compute_speed,
    )

    try:
        heft_makespan, simulated_makespan = sim.run_full(output_dir=args.output)

        # Verify dependency constraints
        print("\nVERIFICATION:")
        violations = verify_dependencies(sim)
        if violations:
            print(f"  WARNING: {len(violations)} dependency violations found!")
            for v in violations[:5]:  # Show first 5
                print(f"    - {v}")
            if len(violations) > 5:
                print(f"    ... and {len(violations) - 5} more")
        else:
            print("  All task dependencies respected.")

        # Verify timing breakdown
        verify_timing(sim)

    except Exception as e:
        print(f"Error running simulation: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


def verify_dependencies(sim) -> list:
    """Verify that all task dependencies are respected.

    Returns:
        List of violation messages (empty if all good)
    """
    violations = []

    for task_id, scheduled_task in sim.scheduled_tasks.items():
        # Get predecessors
        predecessors = sim.scheduler._get_predecessors(task_id)

        for pred_id in predecessors:
            pred_task = sim.scheduled_tasks.get(pred_id)
            if pred_task and pred_task.actual_end is not None:
                if scheduled_task.actual_start < pred_task.actual_end:
                    violations.append(
                        f"Task {task_id} started at {scheduled_task.actual_start:.2f} "
                        f"before predecessor {pred_id} ended at {pred_task.actual_end:.2f}"
                    )

    return violations


def verify_timing(sim):
    """Verify that timing breakdown sums correctly for each task."""
    total_issues = 0

    for task_id, metrics in sim.metrics.task_metrics.items():
        expected_duration = metrics.uplink_time + metrics.compute_time + metrics.downlink_time
        actual_duration = metrics.actual_duration

        # Allow small floating point differences
        if abs(expected_duration - actual_duration) > 0.001:
            if total_issues < 5:  # Only show first 5
                print(
                    f"  Timing mismatch for {task_id}: "
                    f"components sum to {expected_duration:.2f}, "
                    f"actual duration is {actual_duration:.2f}"
                )
            total_issues += 1

    if total_issues == 0:
        print("  Timing breakdown sums correctly for all tasks.")
    elif total_issues > 5:
        print(f"  ... and {total_issues - 5} more timing mismatches")


if __name__ == "__main__":
    main()
