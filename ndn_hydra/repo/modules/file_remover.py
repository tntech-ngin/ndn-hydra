# Responsible for removing data from the data storage 
# and NDN-DPDK fileserver from all associated nodes
def remove_file(config, data_storage, file):
    for node_name in file['stores']:
        if config['node_name'] == node_name:
            for seq in range(0, file['packets']):
                segment_comp = f"/seg={seq}"
                data_storage.remove_packet(file['file_name'] + segment_comp)
            file_path = f"{config['fileserver_path']}/{file['file_name']}"
            if os.path.exists(file_path):
                os.remove(file_path)