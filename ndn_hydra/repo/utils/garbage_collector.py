import time
import logging
import asyncio as aio
from ndn.svs import SVSync
from ndn.storage import Storage
from ndn.encoding import Name, Component
from ndn_hydra.repo.modules.global_view import GlobalView
from ndn_hydra.repo.group_messages.remove import RemoveMessageTlv
from ndn_hydra.repo.group_messages.message import Message, MessageTypes
from ndn_hydra.repo.modules.file_remover import remove_file


def collect_db_garbage(global_view: GlobalView, data_storage: Storage, svs: SVSync, config: dict, logger: logging.Logger) -> None:
    """
    Removes files that have not been accessed in the last month from a node's databases.
    """
    logger.info("\nGARBAGE COLLECTOR: Collecting DB garbage...")
    
    # Remove files that have expired (as based on the expiration_time configuration) 
    for file in global_view.get_files():
        current_time = time.time()
        expire_time = int(file['expiration_time'])
        # If expire_time is 0, file is set to not expire
        if current_time >= expire_time and expire_time != 0:
            # Delete from global view
            global_view.delete_file(file['file_name'])
            logger.info(f"GARBAGE COLLECTOR: Removed {file['file_name']} from global view.")
            # Remove from data_storage from this node if present
            if config['node_name'] in file['stores']:
                remove_file(data_storage, file)
                logger.info(f"GARBAGE COLLECTOR: Removed {file['file_name']} from data storage.")

    logger.info("\nGARBAGE COLLECTOR: Finished collecting DB garbage.")

