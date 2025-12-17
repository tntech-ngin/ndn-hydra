from fabrictestbed_extensions.fablib.fablib import FablibManager
from ipaddress import IPv4Network
from datetime import datetime, timezone, timedelta
import time

fablib = FablibManager()
fablib.show_config()

# Configuration
slice_name = f"clemson_dtn_poc_{int(time.time())}"

image = 'default_ubuntu_20'
site = "CLEM"
facility_port_name = "DTN-Clemson"   
facility_port_vlan = "4019"          

subnet = IPv4Network("192.168.40.0/24")
fabric_ip = "192.168.40.1"
dtn_ip = "192.168.40.2"  # to be set manually on DTN

# Create slice
print(f"Creating slice: {slice_name}")
slice = fablib.new_slice(name=slice_name)

# Add a FABRIC node
node = slice.add_node(name="Node1", site=site, image=image)
node_iface = node.add_component(model="NIC_Basic", name="nic1").get_interfaces()[0]

# Add Facility port 
facility = slice.add_facility_port(
    name=facility_port_name,
    site=site,
    vlan=facility_port_vlan
)
facility_iface = facility.get_interfaces()[0]

# Create L2 network and add interfaces
net = slice.add_l2network(name="net1", subnet=subnet)
net.add_interface(node_iface)
net.add_interface(facility_iface)

# Submit slice --------------------
print("Submitting slice…")
slice.submit()
print("Slice submitted. Waiting for provisioning.")
slice = fablib.get_slice(name=slice_name)

# Enable FABRIC node NIC 
try:
    node = slice.get_node("Node1")
    iface = node.get_interface(network_name="net1")
    os_iface = iface.get_os_interface()

    print(f"Configuring Node1 interface: {os_iface}")

    node.execute(f"sudo ip link set {os_iface} up")
    node.execute(f"sudo ip addr add {fabric_ip}/24 dev {os_iface}")

    print(f"FABRIC node configured with IP {fabric_ip}.")

except Exception as e:
    print(f"Post-provision configuration error: {e}")

# Add ssh keys
try:
    with open("../../fabric_config/slice_key_susmit.pub", "r") as f:
        key = f.read().strip()
        for n in slice.get_nodes():
            n.add_public_key(key)
except:
    pass

# Renew slice
try:
    end_date = (datetime.now(timezone.utc) + timedelta(days=14))
    slice.renew(end_date.strftime("%Y-%m-%d %H:%M:%S %z"))
except Exception as e:
    print(f"Renew error: {e}")

print("\nSlice setup complete.")
