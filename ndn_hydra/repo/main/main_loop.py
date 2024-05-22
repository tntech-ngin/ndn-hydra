# -------------------------------------------------------------
# NDN Hydra MainLoop
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

import asyncio as aio
import secrets
import random
from typing import Dict
from ndn.app import NDNApp
from ndn.encoding import Name, Component
from ndn.svs import SVSync
from ndn.storage import Storage, SqliteStorage
from ndn_hydra.repo.modules import *
from ndn_hydra.repo.group_messages import *
from ndn_hydra.repo.modules.file_fetcher import FileFetcher
from ndn_hydra.repo.utils.garbage_collector import collect_db_garbage
from ndn_hydra.repo.modules.favor_calculator import FavorCalculator
from ndn_hydra.repo.modules.read_remaining_space import get_remaining_space


class MainLoop:
    def __init__(self, app: NDNApp, config: Dict, global_view: GlobalView, data_storage: Storage, svs_storage: Storage, file_fetcher: FileFetcher):
        print(f'\n******* Initial config: {config}\n')
        self.app = app
        self.config = config
        self.global_view = global_view
        self.data_storage = data_storage
        self.svs_storage = svs_storage
        self.file_fetcher = file_fetcher
        self.file_fetcher.store_func = self.store
        self.svs = None
        self.logger = logging.getLogger()
        self.node_name = self.config['node_name']
        self.tracker = HeartbeatTracker(self.node_name, global_view, config['loop_period'], config['heartbeat_rate'], config['tracker_rate'], config['beats_to_fail'], config['beats_to_renew'])
        self.last_garbage_collect_t = time.time()  # time in seconds
        self.favor = global_view.get_node(self.config['node_name'])['favor']

    async def start(self):
        self.svs = SVSync(self.app, Name.normalize(self.config['repo_prefix'] + "/group"), Name.normalize(self.node_name), self.svs_missing_callback, storage=self.svs_storage)
        await aio.sleep(5)
        while True:
            await aio.sleep(self.config['loop_period'] / 1000.0)
            self.periodic()

    def periodic(self):
        self.tracker.detect()
        if self.tracker.beat():
            self.send_heartbeat()
            self.tracker.reset(self.node_name)
        self.backup_list_check()
        self.claim()
        self.check_garbage()

    def svs_missing_callback(self, missing_list):
        aio.ensure_future(self.on_missing_svs_messages(missing_list))

    async def on_missing_svs_messages(self, missing_list):
        # if missing list is greater than 100 messages, bootstrap
        for i in missing_list:
            if i.nid == self.config["node_name"]:
                self.tracker.restart(self.config["node_name"])
                # bootstrap
                continue
            while i.lowSeqno <= i.highSeqno:
                message_bytes = await self.svs.fetchData(Name.from_str(i.nid), i.lowSeqno)
                if message_bytes == None:
                    continue
                message = Message.specify(i.nid, i.lowSeqno, message_bytes)
                self.tracker.reset(i.nid)
                aio.ensure_future(message.apply(self.global_view, self.fetch_file, self.svs, self.config))
                i.lowSeqno = i.lowSeqno + 1
                
    def send_heartbeat(self):
        heartbeat_message = HeartbeatMessageTlv()
        heartbeat_message.node_name = Name.to_bytes(self.config['node_name'])
        heartbeat_message.favor_parameters = FavorParameters()
        heartbeat_message.favor_parameters.rtt = self.config['rtt']
        heartbeat_message.favor_parameters.num_users = self.config['num_users']
        heartbeat_message.favor_parameters.bandwidth = self.config['bandwidth']
        heartbeat_message.favor_parameters.network_cost = self.config['network_cost']
        heartbeat_message.favor_parameters.storage_cost = self.config['storage_cost']
        heartbeat_message.favor_parameters.remaining_storage = get_remaining_space()

        heartbeat_message.favor_weights = {
            'remaining_storage': 0.14,
            'bandwidth': 0,
            'rw_speed': 0
        }

        favor_before = FavorCalculator.calculate_favor(self, {
            'remaining_storage': get_remaining_space(),
            'bandwidth': self.config['bandwidth'],
            'rw_speed': self.config['rw_speed']
        }, {
            'remaining_storage': 0.14,
            'bandwidth': 0,
            'rw_speed': 0
        })
        print(f'\n Calculated favor at node {self.node_name}: {favor_before}\n')


        message = Message()
        message.type = MessageTypes.HEARTBEAT
        message.value = heartbeat_message.encode()
        
        try:
            next_state_vector = self.svs.getCore().getSeqno() + 1
        except TypeError:
            next_state_vector = 0

        favor = self.global_view.get_node(self.config['node_name'])['favor']
        self.global_view.update_node(self.config['node_name'], favor, next_state_vector)
        self.svs.publishData(message.encode())

    def backup_list_check(self):
        underreplicated_files = self.global_view.get_underreplicated_files()
        for underreplicated_file in underreplicated_files:
            deficit = underreplicated_file['desired_copies'] - len(underreplicated_file['stores'])
            for backuped_by in underreplicated_file['backups']:
                if (backuped_by['node_name'] == self.config['node_name']) and (backuped_by['rank'] < deficit):
                    self.fetch_file(underreplicated_file['file_name'], underreplicated_file['packets'], underreplicated_file['packet_size'], underreplicated_file['fetch_path'])

    def claim_heartbeat_message(self):
        # TODO: possibility based on # active sessions and period
        if random.random() < 0.618:
            return
        backupable_files = self.global_view.get_backupable_files()
        for backupable_file in backupable_files:
            if random.random() < 0.618:
                continue
            # print(json.dumps(backupable_insertion['stores']))
            # print(json.dumps(backupable_insertion['backups']))
            already_in = False
            for stored_by in backupable_file['stores']:
                if stored_by == self.config['node_name']:
                    already_in = True
                    break
            for backuped_by in backupable_file['backups']:
                if backuped_by['node_name'] == self.config['node_name']:
                    already_in = True
                    break
            if already_in:
                continue
            if len(backupable_file['backups']) == 0 and len(backupable_file['stores']) == 0:
                continue
            authorizer = None
            if len(backupable_file['backups']) == 0:
                authorizer = {
                    'node_name': backupable_file['stores'][-1],
                    'rank': -1,
                    'nonce': backupable_file['file_name']
                }
            else:
                authorizer = backupable_file['backups'][-1]
            # generate claim (request) msg and send
            # claim tlv
            claim_message = ClaimMessageTlv()
            claim_message.node_name = self.config['node_name'].encode()
            claim_message.favor = self.global_view.get_node(claim_message.node_name)['favor']
            claim_message.file_name = Name.from_str(backupable_file['file_name'])
            claim_message.type = ClaimTypes.REQUEST
            claim_message.claimer_node_name = self.config['node_name'].encode()
            claim_message.claimer_nonce = secrets.token_hex(4).encode()
            claim_message.authorizer_node_name = authorizer['node_name'].encode()
            claim_message.authorizer_nonce = authorizer['nonce'].encode()

            # claim msg
            message = Message()
            message.type = MessageTypes.CLAIM
            message.value = claim_message.encode()

            self.svs.publishData(message.encode())
            self.logger.info(f"[MSG][CLAIM.R]* nam={self.config['node_name']};fil={backupable_file['file_name']}")

    def store(self, file_name: str):
        store_message = StoreMessageTlv()
        store_message.node_name = self.config['node_name'].encode()
        store_message.favor = str(self.global_view.get_node(store_message.node_name)['favor']).encode()
        store_message.file_name = Name.from_str(file_name)

        message = Message()
        message.type = MessageTypes.STORE
        message.value = store_message.encode()

        self.global_view.store_file(file_name, self.config['node_name'])
        self.svs.publishData(message.encode())
        self.logger.info(f"[MSG][STORE]*   nam={self.config['node_name']};fil={file_name}")

    def fetch_file(self, file_name: str, packets: int, packet_size: int, fetch_path: str):
        self.file_fetcher.fetch_file_from_client(file_name, packets, packet_size, fetch_path)

    def check_garbage(self):
        """
        Checks for database garbage daily.
        """
        current_time = time.time()
        hours_since_last_collection = (current_time - self.last_garbage_collect_t) / (60*60)
        if hours_since_last_collection >= 24:
            collect_db_garbage(self.global_view, self.svs, self.config, self.logger)
            self.last_garbage_collect_t = time.time()
