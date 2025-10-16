# --- FABRIC ↔ DTN-Clemson Proof of Concept Setup ---

from fabrictestbed_extensions.fablib.fablib import FablibManager
from ipaddress import IPv4Network, IPv4Address

fablib = FablibManager()
fablib.show_config()

# --- Configuration ----------------------------------------------------
fabric_slice_name = "fabric_dtn_clemson_poc"
fabric_site = "CLEM"
facility_port_name = "DTN-Clemson"  # official FABRIC name for Clemson DTN port
vlan_id = "4010"                    # choose one VLAN from 4010–4019

# Choose any /24 subnet for testing
subnet = IPv4Network("192.168.40.0/24")
fabric_ip = IPv4Address("192.168.40.1")  # FABRIC side
dtn_ip = IPv4Address("192.168.40.2")     # DTN side (configured manually on DTN)

# --- Create slice -----------------------------------------------------
fabric_slice = fablib.new_slice(name=fabric_slice_name)

# Create an L2 network (no gateway, pure layer 2)
net1 = fabric_slice.add_l2network(name="net1", subnet=subnet)

# --- Add FABRIC node --------------------------------------------------
fabric_node = fabric_slice.add_node(name="Node1", site=fabric_site)
fabric_node_iface = fabric_node.add_component(model="NIC_Basic", name="nic1").get_interfaces()[0]
fabric_node_iface.set_mode("config")
net1.add_interface(fabric_node_iface)
fabric_node_iface.set_ip_addr(fabric_ip)

# --- Add DTN-Clemson facility port -----------------------------------
fabric_facility_port = fabric_slice.add_facility_port(
    name=facility_port_name,
    site=fabric_site,
    vlan=str(vlan_id)
)
fabric_facility_port_iface = fabric_facility_port.get_interfaces()[0]
fabric_facility_port_iface.set_mode("manual")
net1.add_interface(fabric_facility_port_iface)

# --- Submit the slice -------------------------------------------------
fabric_slice.submit()

# --- Post-provision: Assign IP & Test -------------------------------
try:
    fabric_node = fabric_slice.get_node(name="Node1")
    fabric_node_iface = fabric_node.get_interface(network_name="net1")

    # Ensure NIC is up and IP configured
    fabric_node.execute(f"sudo ip addr add {fabric_ip}/24 dev {fabric_node_iface.get_os_interface()}")
    fabric_node.execute(f"sudo ip link set {fabric_node_iface.get_os_interface()} up")

    print(f"Pinging DTN ({dtn_ip}) from FABRIC node...")
    stdout, stderr = fabric_node.execute(f"ping -c 5 {dtn_ip}")
    print(stdout)

except Exception as e:
    print(f"Exception: {e}")
