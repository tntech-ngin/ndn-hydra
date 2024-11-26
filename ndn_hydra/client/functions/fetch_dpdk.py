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


class HydraFetchClientDPDK(object):
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
        query_client = HydraQueryClient(self.app, self.client_prefix, self.repo_prefix)
        query = [Component.from_str("filestores")] + file_name
        query_result = await query_client.send_query(query)
        source_repo = random.choice(query_result)

        file_name = Name.to_str(file_name)
        command = f'''docker run -t \
            --mount type=volume,source=run-ndn,target=/run/ndn \
            sankalpatimilsina/ndnc:nov-11 \
            ./sandie-ndn/NDNc/build/ndncft-client --pipeline-type fixed --lifetime 2000 --name-prefix {source_repo} --copy {file_name}'''
        subprocess.run(command, shell=True)

        return name_at_repo