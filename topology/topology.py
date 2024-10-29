"""
 Automate the topology creation to use Hydra
"""
from typing import Dict, List, Tuple
import yaml

# Usage example:
"""
# Initialize slice, sites, and other parameters
slice = fabric_slice()
sites = ["site1", "site2", ...]
cores = 2
ram = 4
disk = 10
image = "ubuntu-20.04"

# Run the automation
network = main(slice, sites, cores, ram, disk, image)
"""

class TopologyAutomation:
    def __init__(self, iface_table, nodes_list, connections, client_nodes_list, client_connections):
        self.iface_table: Dict[str, List[object]] = iface_table # Store interfaces for each node
        self.nodes_list: List[str] = nodes_list
        self.connections: List[Dict[str, List[str]]] = connections
        self.client_nodes_list: List[str] = client_nodes_list
        self.client_connections: List[Dict[str, List[str]]] = client_connections
        self.nodes: Dict[str, object] = {node: {} for node in nodes_list}
        self.client_nodes: Dict[str, object] = {node: {} for node in client_nodes_list}

    def add_new_node(self, node_name: str, used_slice, site, cores, ram, disk, image) -> None:
        """Add a node to the network with specified configurations."""
        print(f"\n > Processing node {node_name}")
        node = used_slice.add_node(name=node_name, site=site)
        node.set_capacities(cores=cores, ram=ram, disk=disk)
        node.set_image(image)
        self.nodes[node_name] = node
        self.iface_table[node_name] = []

    def add_interface(self, node_name: str, nic_name: str) -> None:
        """Add a network interface to a node."""
        node = self.nodes[node_name]
        iface = node.add_component(model='NIC_Basic', name=nic_name).get_interfaces()[0]
        self.iface_table[node_name].append(iface)

    def create_network_connection(
            self,
            selected_slice,
            network_name: str,
            node1: str,
            node2: str,
            interface1_idx: int,
            interface2_idx: int
    ) -> None:
        """Create a network connection between two nodes using specified interfaces."""
        created_connections = []
        if (node1, node2) not in created_connections and (node2, node1) not in created_connections:
            selected_slice.add_l2network(
                name=network_name,
                interfaces=[
                    self.iface_table[node1][interface1_idx],
                    self.iface_table[node2][interface2_idx]
                ]
            )
            created_connections.append((node1, node2))
            print(f"\n > Created connection: {network_name} between {node1} and {node2}")

    def setup_network(self, selected_slice, base_network_name: str, nodes: List[str]) -> None:
        """Create a full mesh network between the specified nodes."""

        # Flatten the connections into a list of tuples first
        flat_connections = []
        for conn_dict in self.connections:
            for source, targets in conn_dict.items():
                flat_connections.extend((source, target) for target in targets)

        interface_indices = {node: 0 for node in nodes}

        for i, (source_node, target_node) in enumerate(flat_connections):

            network_name = f"{base_network_name}{i}"

            # Create connection using current interface indices
            self.create_network_connection(
                selected_slice,
                network_name,
                source_node,
                target_node,
                interface_indices[source_node],
                interface_indices[target_node]
            )

            # Increment interface indices
            interface_indices[source_node] += 1
            interface_indices[target_node] += 1

    def setup_client_connections(
            self,
            selected_slice,
            base_network_name: str,
            client_connections: List[List[str, str]]
    ) -> None:
        """Set up connections between client nodes and their designated targets."""
        for idx, connection in enumerate(client_connections):
            client = connection[0]
            target = connection[1]
            network_name = f"{base_network_name}{idx}"
            # Clients typically use their first (and only) interface
            # Target nodes need to use their next available interface
            target_interface_idx = len(self.iface_table[target]) - 1
            self.create_network_connection(selected_slice, network_name, client, target, 0, target_interface_idx)


def main(selected_slice, sites, cores, ram, disk, image):
    with open('../ndn_hydra/repo/config.yaml', 'r') as config_file:
        config = yaml.safe_load(config_file)

    topology = config['default_config']['topology']

    nodes_list: List[str] = topology['nodes']
    connections: List[Tuple[str, str]] = topology['connections']

    client_nodes_list: List[str] = topology['client_nodes']
    client_connections: List[Tuple[str, str]] = topology['client_connections']

    # Initialize automation
    net_auto = TopologyAutomation({}, nodes_list, connections, client_nodes_list, client_connections)

    # Define nodes
    all_nodes = client_nodes_list + nodes_list

    # Create all nodes
    for i, node_name in enumerate(all_nodes):
        net_auto.add_new_node(node_name, selected_slice, sites[i % len(sites)], cores, ram, disk, image)

    # Add interfaces for nodes (need multiple interfaces for full mesh)
    for node in nodes_list:
        # Add N-1 interfaces for connections (where N is the number of nodes)
        for i in range(len(nodes_list) - 1):
            nic_name = f"{node}_nic_{i}"
            net_auto.add_interface(node, nic_name)

    # Add single interface for client nodes
    for node in client_nodes_list:
        net_auto.add_interface(node, f"{node}_nic_0")

    # Setup mesh network
    net_auto.setup_network(selected_slice, "net_", nodes_list)

    # Setup client connections
    net_auto.setup_client_connections(selected_slice, "net_client_", client_connections)

    return net_auto