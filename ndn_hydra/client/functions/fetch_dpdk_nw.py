# -------------------------------------------------------------
# NDN Hydra Fetch Client (From NDN_DPDK Fileserver)
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/tntech-ngin/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

from ndn.app import NDNApp
from ndn.encoding import FormalName, Component, Name
from ndn_hydra.client.functions.query import HydraQueryClient
import os
import random
import subprocess


class HydraFetchClientDPDKNW(object):
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

    async def get_holding_repo(self, file_name: str) -> str:
        # Get all available nodes
        query_client = HydraQueryClient(self.app, self.client_prefix, self.repo_prefix)
        query = [Component.from_str("nodes")]
        node_list = await query_client.send_query(query)

        file_basename = file_name.split("/")[-1]
        source_repo = None

        # Check each node for the file
        for node in node_list:
            try:
                # List files in the target directory
                list_cmd = f"ndndpdk-godemo ls --name {node} 2>/dev/null"
                result = subprocess.run(list_cmd, shell=True, capture_output=True, text=True, timeout=5)
                file_list = result.stdout.strip().splitlines()

                if file_basename in file_list:
                    source_repo = node
                    break
            except Exception as e:
                print(f"Error querying node {node}: {e}")
                continue

        if not source_repo:
            raise FileNotFoundError(f"Could not find {file_basename} on any node.")

        return source_repo

    async def fetch_file_dpdk(self, file_name: FormalName, local_filename: str = None, overwrite: bool = False) -> None:
        """
        Fetch a file from remote repo, and write to the current working directory.
        :param name_at_repo: NonStrictName. The name with which this file is stored in the repo.
        :param local_filename: str. The filename of the retrieved file on the local file system.
        :param overwrite: If true, existing files are replaced.
        """
        name_at_repo = self.repo_prefix + file_name + [Component.from_segment(0)]

        # If the file already exists locally and overwrite=False, retrieving the file makes no
        # sense.
        if os.path.isfile(local_filename) and not overwrite:
            raise FileExistsError("{} already exists".format(local_filename))

        # Get repo which holds the file
        source_repo = await self.get_holding_repo(Name.to_str(file_name))
        file_name = Name.to_str(file_name)

        file_name = Name.to_str(file_name)
        command = f'''docker run -t \
            --mount type=volume,source=run-ndn,target=/run/ndn \
            sankalpatimilsina/ndnc:nov-11 \
            ./sandie-ndn/NDNc/build/ndncft-client --pipeline-type fixed --lifetime 2000 --name-prefix {source_repo} --copy {file_name}'''
        subprocess.run(command, shell=True)

        return name_at_repo