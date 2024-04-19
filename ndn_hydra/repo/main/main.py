# -------------------------------------------------------------
# NDN Hydra Main
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

from argparse import ArgumentParser
import asyncio as aio
import random
import logging
import yaml
from typing import Dict
from threading import Thread
import pkg_resources
from ndn.app import NDNApp
from ndn.encoding import Name
from ndn.utils import gen_nonce
from ndn.storage import SqliteStorage
import sys, os
from ndn.svs import SVSyncLogger
from ndn_hydra.repo import *
from ndn_hydra.repo.modules.file_fetcher import FileFetcher
from utils.update_config import update_config_file


def process_cmd_opts():
    def interpret_version() -> None:
        set = True if "-v" in sys.argv else False
        if set and (len(sys.argv)-1 < 2):
            try: print("ndn-hydra " + pkg_resources.require("ndn-hydra")[0].version)
            except pkg_resources.DistributionNotFound: print("ndn-hydra source,undetermined")
            sys.exit(0)
    def interpret_help() -> None:
        set = True if "-h" in sys.argv else False
        if set:
            if (len(sys.argv)-1 < 2):
                print("usage: ndn-hydra-repo [-h] [-v] -rp REPO_PREFIX -n NODE_NAME")
                print("    ndn-hydra-repo: hosting a node for hydra, the NDN distributed repo.")
                print("    ('python3 ./examples/repo.py' instead of 'ndn-hydra-repo' if from source.)")
                print("")
                print("* informational args:")
                print("  -h, --help                       |   shows this help message and exits.")
                print("  -v, --version                    |   shows the current version and exits.")
                print("")
                print("* required args:")
                print("  -rp, --repoprefix REPO_PREFIX    |   repo (group) prefix. Example: \"/hydra\"")
                print("  -n, --nodename NODE_NAME         |   node name. Example: \"node01\"")
                print("")
                print("Thank you for using hydra.")
            sys.exit(0)
    def process_name(input_string: str):
        if input_string[-1] == "/":
            input_string = input_string[:-1]
        if input_string[0] != "/":
            input_string = "/" + input_string
        return input_string
    def parse_cmd_opts():
        # Command Line Parser
        parser = ArgumentParser(prog="ndn-hydra-repo",add_help=False,allow_abbrev=False)
        # Adding all Command Line Arguments
        parser.add_argument("-h","--help",action="store_true",dest="help",default=False,required=False)
        parser.add_argument("-v","--version",action="store_true",dest="version",default=False,required=False)
        parser.add_argument("-rp","--repoprefix",action="store",dest="repo_prefix",required=True)
        parser.add_argument("-n","--nodename",action="store",dest="node_name",required=True)
        # Interpret Informational Arguments
        interpret_version()
        interpret_help()
        # Getting all Arguments
        vars = parser.parse_args()

        # Process args
        args = {}
        args["repo_prefix"] = process_name(vars.repo_prefix)
        args["node_name"] = process_name(vars.node_name)
        workpath = "{home}/.ndn/repo{repo_prefix}/{node_name}".format(
            home=os.path.expanduser("~"),
            repo_prefix=args["repo_prefix"],
            node_name=args["node_name"])
        args["logging_path"] = "{workpath}/session.log".format(workpath=workpath)
        args["data_storage_path"] = "{workpath}/data.db".format(workpath=workpath)
        args["global_view_path"] = "{workpath}/global_view.db".format(workpath=workpath)
        args["svs_storage_path"] = "{workpath}/svs.db".format(workpath=workpath)
        return args

    args = parse_cmd_opts()
    return args

async def listen(repo_prefix: Name, pb: PubSub, insert_handle: InsertCommandHandle, delete_handle: DeleteCommandHandle):
    # pubsub
    pb.set_publisher_prefix(repo_prefix)
    await pb.wait_for_ready()
    # protocol handle
    await insert_handle.listen(repo_prefix)
    await delete_handle.listen(repo_prefix)

class HydraNodeThread(Thread):
    def __init__(self, config: Dict):
        Thread.__init__(self)
        self.config = config

    def run(self) -> None:
        if len(os.path.dirname(self.config['logging_path'])) > 0 and not os.path.exists(os.path.dirname(self.config['logging_path'])):
            try:
                os.makedirs(os.path.dirname(self.config['logging_path']))
            except PermissionError:
                raise PermissionError("Could not create directory: {}".format(self.config['logging_path'])) from None
            except FileExistsError:
                pass

        # logging
        SVSyncLogger.config(False, None, logging.INFO)
        logging.basicConfig(level=logging.INFO,
                            format='%(created)f  %(levelname)-8s  %(message)s',
                            filename=self.config['logging_path'],
                            filemode='w')
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        logging.getLogger().addHandler(console)

        # NDN
        app = NDNApp()

        # Post-start
        async def start_main_loop():
            # databases
            data_storage = SqliteStorage(self.config['data_storage_path'])
            global_view = GlobalView(self.config['global_view_path'])
            svs_storage = SqliteStorage(self.config['svs_storage_path'])
            pb = PubSub(app)

            # file fetcher module
            file_fetcher = FileFetcher(app, global_view, data_storage, self.config)

            # main_loop (svs)
            main_loop = MainLoop(app, self.config, global_view, data_storage, svs_storage, file_fetcher)

            # handles (reads, commands & queries)
            read_handle = ReadHandle(app, data_storage, global_view, self.config)
            insert_handle = InsertCommandHandle(app, data_storage, pb, self.config, main_loop, global_view)
            delete_handle = DeleteCommandHandle(app, data_storage, pb, self.config, main_loop, global_view)
            query_handle = QueryHandle(app, global_view, self.config)

            await listen(Name.normalize(self.config['repo_prefix']), pb, insert_handle, delete_handle)
            await main_loop.start()

        # start listening
        try:
            app.run_forever(after_start=start_main_loop())
        except (FileNotFoundError, ConnectionRefusedError):
            print('Error: could not connect to NFD.')
            sys.exit()

def main() -> int:
  # Load dynamic values into config file
  
  # isConfigUpdated = update_config_file()
  
  # if (isConfigUpdated):
  with open("./ndn_hydra/repo/main/config.yaml", "r") as yamlfile:
    config = yaml.load(yamlfile, Loader=yaml.FullLoader)
    HydraNodeThread(config).start()
    # return 0

  return 0


if __name__ == "__main__":
    sys.exit(main())
