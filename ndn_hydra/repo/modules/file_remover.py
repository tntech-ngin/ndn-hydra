import os
import asyncio as aio
from ndn_hydra.repo.modules.data_storage import DataStorage

# Responsible for removing data from the data storage
def remove_file(data_storage, file, config):
    data_storage = DataStorage(config['data_storage_path'])
    keys = [file['file_name'] + f'/seg={seq}' for seq in range(file['packets'])]
    aio.get_event_loop().run_in_executor(None, data_storage.remove_packets, keys)
    file_path = f"{config['fileserver_path']}/{file['file_name']}"
    if os.path.exists(file_path):
        os.remove(file_path)
