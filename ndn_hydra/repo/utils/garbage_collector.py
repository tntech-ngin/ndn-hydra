import time
import logging
import os
from ndn.svs import SVSync
from ndn.storage import Storage
from ndn_hydra.repo.modules.global_view import GlobalView
from ndn_hydra.repo.group_messages.remove import RemoveMessageTlv
from ndn_hydra.repo.group_messages.message import Message, MessageTypes


def collect_db_garbage(global_view: GlobalView, data_storage: Storage, svs: SVSync, config: dict, logger: logging.Logger) -> None:
    """
    Removes files that have not been accessed in the last month from a node's databases.
    """
    logger.info("GARBAGE COLLECTOR: Collecting DB garbage...")    
    
    current_time = time.time()

    # Find files that have expired (as based on the expiration_time configuration) or have been explicitly deleted
    all_files = global_view.get_files()
    files_to_remove = []
    for file in all_files:
        expire_time = int(file['expiration_time'])
        # If expire_time is 0, file is set to not expire
        if (current_time >= expire_time and expire_time != 0) or file['is_deleted']:
            files_to_remove.append(file['file_name'])

    # Remove files from storage
    for file_name in files_to_remove:
        # Delete from global view if not already deleted
        global_view.delete_file(file_name)
        logger.info(f"GARBAGE COLLECTOR: Removed {file_name} from global view.")

        # Remove from data storage and NDN-DPDK fileserver
        data_storage.remove_packet(file_name) # TODO: need to remove all packets of the file
        if os.path.exists(f"{config['fileserver_path']}/{file_name}"):
            os.remove(f"{config['fileserver_path']}/{file_name}")
        logger.info(f"GARBAGE COLLECTOR: Removed {file_name} from data storage.")


    logger.info("GARBAGE COLLECTOR: Finished collecting DB garbage.")

