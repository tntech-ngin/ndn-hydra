"""
 Automate the topology creation to use Hydra
"""
from typing import List, Dict
from collections import deque

# nodes = ["Node1","Node2","Node3","Node4","Node5"]
def create_graph(nodes: List[str]) -> Dict[str, List[str]]:
    # Define neighbors
    graph: Dict[str, List[str]] = {node: [] for node in nodes}

    # Create edges between all the nodes
    for i, node in enumerate(nodes):
        for j in range(i + 1, len(nodes)):
            neighbor = nodes[j]
            graph[node].append(neighbor)
            graph[neighbor].append(node)


    return graph

def bfs_shortest_path(graph: Dict[str, List[str]], start: str, goal: str) -> List[str]:
    queue = deque([[start]])
    visited = {start}

    while queue:
        path = queue.popleft()
        node = path[-1]

        if node == goal:
            return path

        for neighbor in graph.get(node, []):
            if neighbor not in visited:
                visited.add(neighbor)
                new_path = list(path)
                new_path.append(neighbor)
                queue.append(new_path)

    return []

def remove_cycles(topology: Dict[str, Dict[str, List[str]]]) -> Dict[str, Dict[str, List[str]]]:
    acyclic_topology: Dict[str, Dict[str, List[str]]] = {}

    for source in topology:
        acyclic_topology[source] = {}
        for destination, path in topology[source].items():
            if source not in path[1:]:  # Exclude paths that return to the source
                acyclic_topology[source][destination] = path

    return acyclic_topology

def create_topology(graph: Dict[str, List[str]]) -> Dict[str, List[str]]:
    # Map all the nodes to define sources and destinations
    # A ---B---C---E---F---> D
    # Source ----ROUTE-----> Destination

    # Define the shortest path for each pair of Source and Destination
    shortest_paths: Dict[str, Dict[str, List[str]]] = {}

    for source in graph:
        shortest_paths[source] = {}
        for destination in graph:
            if source != destination:
                path = bfs_shortest_path(graph, source, destination)
                if path:
                    shortest_paths[source][destination] = path

    # Drop all the cycles
    acyclic_topology = remove_cycles(shortest_paths)

    return acyclic_topology

