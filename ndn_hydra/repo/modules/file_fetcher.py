# -------------------------------------------------------------
# NDN Hydra File Fetcher
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/tntech-ngin/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

import asyncio as aio
import logging
import random
import time
import os
from ndn.app import NDNApp
from ndn.encoding import parse_data
from ndn.storage import Storage
from ndn_hydra.repo.modules import *
from ndn_hydra.repo.group_messages import *
from ndn_hydra.repo.utils.concurrent_fetcher import concurrent_fetcher


class FileFetcher:
    """
    A class to abstract client-to-node and node-to-node fetching.
    """

    def __init__(self, app: NDNApp, global_view: GlobalView, data_storage: Storage, config: dict) -> None:
        self.app = app
        self.global_view = global_view
        self.data_storage = data_storage
        self.config = config
        self.repo_prefix = config['repo_prefix']
        # Custom: save files for dpdk fileserver
        self.fileserver_path = config['fileserver_path']
        # custom: end
        self.logger = logging.getLogger()
        self.store_func = None  # This function must be initialized to store properly store
        self.fetching = []

    def fetch_file_from_client(self, file_name: str, packets: int, packet_size: int, fetch_path: str):
        if file_name in self.fetching:
            self.logger.info("\nFileFetcher [Client]: Already fetching")
            return
        if not self.store_func:
            self.logger.info("\nFileFetcher: No storage function defined")
            return
        self.fetching.append(file_name)
        aio.ensure_future(self._fetch_file_helper(file_name, packets, packet_size, fetch_path))

    def fetch_file_from_node(self, file_name: str, packets: int, packet_size: int):
        if file_name in self.fetching:
            self.logger.info("\nFileFetcher [Node]: Already fetching")
            return
        if not self.store_func:
            self.logger.info("\nFileFetcher: No storage function defined")
            return
        self.fetching.append(file_name)
        # Randomly select a node to fetch file from
        file_info = self.global_view.get_file(file_name)
        on_list = file_info["stores"]
        if not on_list:
            self.logger.info("\nFileFetcher: File not in stores")
            return
        active_nodes = set([node['node_name'] for node in self.global_view.get_nodes()])
        on_list = [x for x in on_list if x in active_nodes]
        selected_node = random.choice(on_list)
        if not selected_node:
            return
        # Fetch file from selected node
        fetch_path = self.repo_prefix + file_name
        forwarding_hint = [(1, Name.to_str(self.repo_prefix) + selected_node + Name.to_str(file_name))]
        aio.ensure_future(self._fetch_file_helper(file_name, packets, packet_size, fetch_path, forwarding_hint=forwarding_hint, internal=True))

    async def _fetch_file_helper(self, file_name: str, packets: int, packet_size: int, fetch_path: str, forwarding_hint=None, internal=False):
        self.logger.info(f"\n[ACT][FETCH]*  "
                         f"\n\tFile name={file_name};"
                         f"\n\tPackets={packets};"
                         f"\n\tfetch_path={fetch_path}")
        start = time.time()

        # Custom: save files for dpdk fileserver
        file_path = f"{self.fileserver_path}/{file_name}"
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        f = open(file_path, 'wb')
        # Custom: end

        async for (_, _, _, data_bytes, key) in concurrent_fetcher(self.app, fetch_path, file_name, 0,
                                                                         packets - 1, aio.Semaphore(15), forwarding_hint=forwarding_hint):
            if not internal:
                _, _, data_bytes, _ = parse_data(data_bytes)
            self.data_storage.put_packet(key, data_bytes)  #TODO: check digest

            # Custom: save files for dpdk fileserver
            _, _, data_bytes, _ = parse_data(data_bytes)
            f.write(data_bytes)
            # Custom: end

        # Custom: save files for dpdk fileserver
        f.close()
        # Custom: end

        end = time.time()
        duration = end - start
        self.logger.info(f"\n[ACT][FETCHED]* "
                         f"\n\tpackets={packets};"
                         f"\n\tduration={duration}")
        self.store_func(file_name)
        try:
            self.fetching.remove(file_name)
        except:
            pass
