# -------------------------------------------------------------
# NDN Hydra Main
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/tntech-ngin/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

import logging
from argparse import ArgumentParser
from typing import Dict
from threading import Thread
import pkg_resources
from ndn.app import NDNApp
from ndn.encoding import Name
from ndn.storage import SqliteStorage
import sys, os
from ndn.svs import SVSyncLogger
from ndn_hydra.repo import *
from ndn_hydra.repo.modules.file_fetcher import FileFetcher
from ndn_hydra.repo.modules.data_storage import DataStorage
from ndn_hydra.repo.modules.read_config import read_config_file


def process_cmd_opts():
    def interpret_version(args) -> None:
        if args.version and (len(sys.argv) - 1 < 2):
            try:
                print("ndn-hydra " + pkg_resources.require("ndn-hydra")[0].version)
            except pkg_resources.DistributionNotFound:
                print("ndn-hydra source, undetermined")
            sys.exit(0)

    def interpret_help(args) -> None:
        if args.help:
            if len(sys.argv) - 1 < 2:
                print("* Basic initialization:")
                print("    ndn-hydra-repo [-h] [-v] -rp REPO_PREFIX -n NODE_NAME")
                print("")
                print("    ndn-hydra-repo: hosting a node for hydra, the NDN distributed repo.")
                print("* Examples:")
                print("    ('python3 ./examples/repo.py' instead of 'ndn-hydra-repo' if from source.)")
                print("")
                print("* Informative arguments:")
                print("    -h, --help                        |   shows this help message and exits.")
                print("    -v, --version                     |   shows the current version and exits.")
                print("")
                print("* Optional arguments:")
                print("    -rp, --repoprefix REPO_PREFIX     |   repo (group) prefix. Example: \"/hydra\"")
                print("    -n,  --nodename NODE_NAME         |   node name. Example: \"node01\"")
                print("    -d,  --debugger                   |   enable debugging mode")
                print("    -cr, --critical                   |   enable critical logging level")
                print("")
                print("* Default configuration:")
                print("    The default configuration is in the config.yaml file in the repo folder: ")
                print("    ndn_hydra/repo/config.yaml")
                print("    You can update these parameters there and test again: ")
                print("")
                print("  ** Parameters **")
                print("    Repo prefix  | Prefix for all nodes in the repo. Example: \"/hydra\"")
                print("    Node name    | Name of the node. Example: \"node01\"")
                print("")
                print("  ** Paths **")
                print("    Base         | Base path for all files. Example: \"/home/user/.ndn\"")
                print("    Data storage | Path for data storage. Example: \"/home/user/.ndn/hydra/node01/data.db\"")
                print(
                    "    Global view  | Path for global view. Example: \"/home/user/.ndn/hydra/node01/global_view.db\"")
                print("    Svs storage  | Path for svs storage. Example: \"/home/user/.ndn/hydra/node01/svs.db\"")
                print("    Logging      | Path for logging. Example: \"/home/user/.ndn/hydra/node01/session.log\"")
                print("")
                print("Thank you for using hydra.")
            sys.exit(0)

    def process_name(input_string: str):
        if input_string[-1] == "/":
            input_string = input_string[:-1]
        if input_string[0] != "/":
            input_string = "/" + input_string
        return input_string

    def parse_cli_args():
        # Command Line Parser
        parser = ArgumentParser(prog="ndn-hydra-repo", add_help=False, allow_abbrev=False)

        # Adding all Command Line Arguments
        parser.add_argument("-h", "--help", action="store_true", dest="help", default=False, required=False)
        parser.add_argument("-v", "--version", action="store_true", dest="version", default=False, required=False)
        parser.add_argument("-rp", "--repoprefix", action="store", dest="repo_prefix", default=False, required=False)
        parser.add_argument("-n", "--nodename", action="store", dest="node_name", default=False, required=False)
        parser.add_argument("-d", "--debugger", action="store_true", dest="debugger", default=False, required=False)
        parser.add_argument("-cr", "--critical", action="store_true", dest="critical", default=False, required=False)

        # Getting all Arguments
        cli_args = parser.parse_args()

        # Interpret Informational Arguments
        interpret_version(cli_args)
        interpret_help(cli_args)

        return cli_args

    def create_config():
        cli_args = parse_cli_args()
        # Get values from YAML file
        default_config_file = read_config_file()

        default_repo_prefix = default_config_file['default_config']['repo_prefix']
        default_node_name = default_config_file['default_config']['node_name']

        config_data = {
            "repo_prefix": default_repo_prefix,
            "node_name": default_node_name,
            "loop_period": default_config_file['default_config']['timers']['loop_period'],
            "heartbeat_rate": default_config_file['default_config']['timers']['heartbeat_rate'],
            "tracker_rate": default_config_file['default_config']['timers']['tracker_rate'],
            "beats_to_fail": default_config_file['default_config']['timers']['beats_to_fail'],
            "beats_to_renew": default_config_file['default_config']['timers']['beats_to_renew'],
            "replication_degree": default_config_file['default_config']['timers']['replication_degree'],
            "file_expiration": default_config_file['default_config']['timers']['file_expiration'],
            "rtt": default_config_file['default_config']['favor']['rtt'],
            "num_users": default_config_file['default_config']['favor']['num_users'],
            "bandwidth": default_config_file['default_config']['favor']['bandwidth'],
            "network_cost": default_config_file['default_config']['favor']['network_cost'],
            "storage_cost": default_config_file['default_config']['favor']['storage_cost'],
            "remaining_storage": default_config_file['default_config']['favor']['remaining_storage'],
            "rw_speed": default_config_file['default_config']['favor']['rw_speed'],
            "logger_level": default_config_file['default_config']['logger_level'],
        }

        if cli_args.repo_prefix is not False:
            config_data["repo_prefix"] = process_name(cli_args.repo_prefix)
        if cli_args.node_name is not False:
            config_data["node_name"] = process_name(cli_args.node_name)
        if cli_args.debugger is not False:
            config_data["logger_level"] = "DEBUG"
        if cli_args.critical is not False:
            config_data["logger_level"] = "CRITICAL"

        workpath = "{home}/.ndn/repo{repo_prefix}/{node_name}".format(
            home=os.path.expanduser("~"),
            repo_prefix=config_data["repo_prefix"],
            node_name=config_data["node_name"])
        config_data["logging_path"] = "{workpath}/session.log".format(workpath=workpath)
        config_data["data_storage_path"] = "{workpath}/data.db".format(workpath=workpath)
        config_data["global_view_path"] = "{workpath}/global_view.db".format(workpath=workpath)
        config_data["svs_storage_path"] = "{workpath}/svs.db".format(workpath=workpath)
        return config_data

    configuration = create_config()
    return configuration


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
        if 'logging_path' not in self.config or self.config['logging_path'] is None:
            raise ValueError("The 'logging_path' was not set in the configuration.")

        logging_dir = os.path.dirname(self.config['logging_path'])

        if logging_dir and not os.path.exists(logging_dir):
            try:
                os.makedirs(logging_dir)
            except PermissionError:
                raise PermissionError(f"Could not create directory: {logging_dir}")
            except FileExistsError:
                pass

        # logging

        log_level = getattr(logging, self.config['logger_level'].upper(), logging.INFO)

        logging.basicConfig(level=log_level,
                            format='%(levelname)-8s  %(message)s',
                            filename=self.config['logging_path'],
                            filemode='w')
        console = logging.StreamHandler()
        console.setLevel(log_level)
        logging.getLogger().addHandler(console)

        SVSyncLogger.config(False, None, logging.CRITICAL)

        logging.getLogger('ndn').setLevel(logging.WARNING)

        # NDN
        app = NDNApp()

        # Post-start
        async def start_main_loop():
            # databases
            data_storage = DataStorage(self.config['data_storage_path'])
            global_view = GlobalView(self.config['global_view_path'])
            svs_storage = SqliteStorage(self.config['svs_storage_path'])
            pb = PubSub(app)

            # file fetcher module
            file_fetcher = FileFetcher(app, global_view, data_storage, self.config)

            # main_loop (svs)
            main_loop = MainLoop(app, self.config, global_view, data_storage, svs_storage, file_fetcher)

            # handles (reads, commands & queries)
            read_handle = ReadHandle(app, data_storage, global_view, main_loop, self.config)
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
    config_args = process_cmd_opts()

    try:
        HydraNodeThread(config_args).start()
        return 0

    except Exception as e:
        logging.warning(f"\nAn error occurred running the main thread: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
