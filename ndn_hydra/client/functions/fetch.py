# -------------------------------------------------------------
# NDN Hydra Fetch Client
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/tntech-ngin/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

from asyncio import Semaphore
import logging
import time
from ndn.app import NDNApp
from ndn.encoding import FormalName, Component, Name, ContentType
import os
from ndn_hydra.repo.utils.concurrent_fetcher import concurrent_fetcher


class HydraFetchClient(object):
    def __init__(self, app: NDNApp, client_prefix: FormalName, repo_prefix: FormalName) -> None:
        """
        This client fetches data packets from the remote repo.
        :param app: NDNApp.
        :param client_prefix: NonStrictName. Routable name to client.
        :param repo_prefix: NonStrictName. Routable name to remote repo.
        """
        self.app = app
        self.client_prefix = client_prefix
        self.repo_prefix = repo_prefix

    async def fetch_file(self, file_name: FormalName, local_filename: str = None, overwrite: bool = False) -> None:
        """
        Fetch a file from remote repo, and write to the current working directory.
        :param name_at_repo: NonStrictName. The name with which this file is stored in the repo.
        :param local_filename: str. The filename of the retrieved file on the local file system.
        :param overwrite: If true, existing files are replaced.
        """
        name_at_repo = self.repo_prefix + file_name + [Component.from_segment(0)]

        # If no local filename is provided, store file with last name component
        # of repo filename
        if local_filename is None:
            local_filename = Name.to_str(file_name)
            local_filename = os.path.basename(local_filename)

        # If the file already exists locally and overwrite=False, retrieving the file makes no
        # sense.
        if os.path.isfile(local_filename) and not overwrite:
            raise FileExistsError("{} already exists".format(local_filename))

        # Ping with only one data packet at the start
        b_array = bytearray()
        start_index = 0
        end_index = None
        data_name, meta_info, content, data_bytes = await self.app.express_interest(
            name_at_repo, need_raw_packet=True, can_be_prefix=False, must_be_fresh=False, lifetime=4000)

        forwarding_hint = []
        name_at_repo = name_at_repo[:-1]

        if meta_info.content_type == ContentType.NACK:
            print("Distributed Repo does not have that file.")
            return

        if meta_info.content_type == ContentType.LINK:
            # name_at_repo = Name.from_str(bytes(content).decode())
            forwarding_hint = [(1, bytes(content).decode())]
        # else:
        #     # name_at_repo = name_at_repo[:-1]
        #     # start_index = start_index + 1
        #     # print(name_at_repo)
        #     b_array.extend(content)

        end_index = Component.to_number(meta_info.final_block_id)

        # print(Name.to_str(data_name))

        # Fetch the rest of the file.
        if start_index <= end_index:
            async for (_, _, content, _, _) in concurrent_fetcher(self.app, name_at_repo, Name.from_str(local_filename), start_index, end_index, Semaphore(10), forwarding_hint=forwarding_hint):
                b_array.extend(content)

        # After b_array is filled, sort out what to do with the data.
        if len(b_array) > 0:
            print(f'Fetching completed, writing to file {local_filename}')

            # Create folder hierarchy
            local_folder = os.path.dirname(local_filename)
            if local_folder:
                os.makedirs(local_folder, exist_ok=True)

            # Write retrieved data to file
            if os.path.isfile(local_filename) and overwrite:
                os.remove(local_filename)
            with open(local_filename, 'wb') as f:
                f.write(b_array)

            return name_at_repo
        else:
            print("Client Fetch Command Failed.")