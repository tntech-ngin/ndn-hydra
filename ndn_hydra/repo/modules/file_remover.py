import asyncio as aio


# Responsible for removing data from the data storage
def remove_file(data_storage, file):
    keys = [file['file_name'] + f'/seg={seq}' for seq in range(file['packets'])]
    aio.get_event_loop().run_in_executor(None, data_storage.remove_packets, keys)
