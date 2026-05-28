"""DAG construction and topological level sort.

DAG wiring is inferred purely from names: a step's parameter names are matched
against other steps' output names. No explicit wiring is needed.

``build_dag`` returns an adjacency list (producer → consumers).
``topological_levels`` uses Kahn's algorithm to group steps into levels where
all steps in a level can run concurrently (no intra-level dependencies).
"""

from __future__ import annotations

from collections import defaultdict, deque

from conduit_etl.core.errors import CycleError
from conduit_etl.core.models import Step


def build_dag(steps: list[Step]) -> dict[str, list[str]]:
    """Return adjacency list: step_name → list of step_names that depend on it."""
    producers: dict[str, str] = {s.output_name: s.name for s in steps}
    graph: dict[str, list[str]] = {s.name: [] for s in steps}
    for step in steps:
        for input_name in step.input_names:
            if input_name in producers:
                producer = producers[input_name]
                graph[producer].append(step.name)
    return graph


def topological_levels(graph: dict[str, list[str]]) -> list[list[str]]:
    """Kahn's algorithm — returns steps grouped by execution level.

    Steps in the same level have no dependency on each other and are safe to
    run concurrently. Raises :class:`CycleError` if the graph has a cycle.
    """
    in_degree: dict[str, int] = defaultdict(int)
    for node in graph:
        in_degree.setdefault(node, 0)
    for node, dependents in graph.items():
        for dep in dependents:
            in_degree[dep] += 1

    queue: deque[str] = deque(n for n, d in in_degree.items() if d == 0)
    levels: list[list[str]] = []
    visited = 0

    while queue:
        level = list(queue)
        queue.clear()
        levels.append(level)
        next_nodes: list[str] = []
        for node in level:
            visited += 1
            for dep in graph.get(node, []):
                in_degree[dep] -= 1
                if in_degree[dep] == 0:
                    next_nodes.append(dep)
        queue.extend(next_nodes)

    if visited != len(graph):
        raise CycleError("step graph contains a cycle — check step input/output names")

    return levels


def execution_order(steps: list[Step]) -> list[list[Step]]:
    """Convenience wrapper — returns steps (not just names) grouped by level."""
    by_name = {s.name: s for s in steps}
    graph = build_dag(steps)
    levels = topological_levels(graph)
    return [[by_name[name] for name in level] for level in levels]
