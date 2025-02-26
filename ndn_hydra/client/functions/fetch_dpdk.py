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
import json
import os
import random
import subprocess


class HydraFetchClientDPDK(object):
    def __init__(self, app: NDNApp, client_prefix: FormalName, repo_prefix: FormalName, gqlserver: str='http://127.0.0.1:3032/') -> None:
        """
        This client fetches data packets from the remote repo.
        :param app: NDNApp.
        :param client_prefix: NonStrictName. Routable name to client.
        :param repo_prefix: NonStrictName. Routable name to remote repo.
        :param gqlserver: str. The URL of the GraphQL server.
        """
        self.app = app
        self.client_prefix = client_prefix
        self.repo_prefix = repo_prefix
        self.gqlserver = gqlserver

    def start_tg(self):
        tg_c = (
            "jq -n '{ "
            "face: { scheme: \"memif\", socketName: \"/run/ndn/tg.sock\", id: 1, role: \"client\", dataroom: 9000 }, "
            "fetcher: { nThreads: 4, nTasks: 7 } }' | "
            f"ndndpdk-ctrl --gqlserver {self.gqlserver} start-trafficgen"
        )
        out = subprocess.run(tg_c, shell=True, capture_output=True, text=True).stdout
        data = json.loads(out)
        return data["id"], data["fetcher"]["id"]

    def stop_tg(self, tg_id: str):
        stop_c = f"ndndpdk-ctrl --gqlserver {self.gqlserver} stop-trafficgen --id {tg_id}"
        subprocess.run(stop_c, shell=True)

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
        file_name = source_repo + Name.to_str(file_name)

        # Get fetch args
        fetch_args_c = f"ndndpdk-godemo --gqlserver {self.gqlserver.replace('3032', '3030')} fetch --name {file_name} --tg-fetcher"
        fetch_args_out = subprocess.run(fetch_args_c, shell=True, capture_output=True, text=True).stdout.strip()
        
        # Start fetch
        tg_id, fetcher_id = self.start_tg()
        fetch_c = (
            f"ndndpdk-ctrl --gqlserver {self.gqlserver} start-fetch --fetcher {fetcher_id} "
            f"--filename {local_filename} {fetch_args_out}"
        )
        task_out = subprocess.run(fetch_c, shell=True, capture_output=True, text=True).stdout
        task_id = json.loads(task_out)["id"]
        
        # Watch fetch (auto-stop)
        log_filename = f"{local_filename}.log"
        watch_c = (
            f"ndndpdk-ctrl --gqlserver {self.gqlserver} watch-fetch --id {task_id} --auto-stop &> {log_filename}"
        )
        subprocess.run(watch_c, shell=True)

        # Stop traffic generator
        self.stop_tg(tg_id)

        return name_at_repo