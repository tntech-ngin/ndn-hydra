# -------------------------------------------------------------
# NDN Hydra Add Group Message
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/tntech-ngin/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

from typing import Callable
from ndn.encoding import *
import time
from ndn.storage import Storage
from ndn_hydra.repo.modules.global_view import GlobalView
from ndn_hydra.repo.group_messages.specific_message import SpecificMessage
from ndn_hydra.repo.protocol.base_models import File


class AddMessageTypes:
    NODE_NAME = 84
    FAVOR = 86

    FILE = 91
    DESIRED_COPIES = 92
    FETCH_PATH = 93
    IS_STORED_BY_ORIGIN = 94
    EXPIRATION_DATE = 95

    BACKUP = 100
    BACKUP_NODE_NAME = 101
    BACKUP_NONCE = 102


class FetchPathTlv(TlvModel):
    prefix = NameField()


class BackupTlv(TlvModel):
    node_name = BytesField(AddMessageTypes.BACKUP_NODE_NAME)
    nonce = BytesField(AddMessageTypes.BACKUP_NONCE)


class AddMessageTlv(TlvModel):
    node_name = BytesField(AddMessageTypes.NODE_NAME)
    favor = BytesField(AddMessageTypes.FAVOR)
    file = ModelField(AddMessageTypes.FILE, File)
    desired_copies = UintField(AddMessageTypes.DESIRED_COPIES)
    fetch_path = ModelField(AddMessageTypes.FETCH_PATH, FetchPathTlv)
    is_stored_by_origin = UintField(AddMessageTypes.IS_STORED_BY_ORIGIN)
    expiration_time = UintField(AddMessageTypes.EXPIRATION_DATE)
    backup_list = RepeatedField(ModelField(AddMessageTypes.BACKUP, BackupTlv))


class AddMessage(SpecificMessage):
    def __init__(self, nid:str, seqno:int, raw_bytes:bytes):
        super(AddMessage, self).__init__(nid, seqno)
        self.message = AddMessageTlv.parse(raw_bytes)

    async def apply(self, global_view: GlobalView, data_storage: Storage, fetch_file: Callable, svs, config):
        node_name = self.message.node_name.tobytes().decode()
        favor = float(self.message.favor.tobytes().decode())
        file = self.message.file
        file_name = Name.to_str(file.file_name)
        packets = file.packets
        packet_size = file.packet_size
        size = file.size
        desired_copies = self.message.desired_copies
        fetch_path = self.message.fetch_path.prefix
        is_stored_by_origin = False if (self.message.is_stored_by_origin == 0) else True
        expiration_time = self.message.expiration_time
        backups = self.message.backup_list
        backup_list = []
        bak = ""
        for backup in backups:
            backup_list.append((backup.node_name.tobytes().decode(), backup.nonce.tobytes().decode()))
            bak = bak + backup.node_name.tobytes().decode() + ","

        self.logger.info(f"\n[MSG][ADD]      nam={node_name};fil={file_name};cop={desired_copies};pck={packets};pck_size={packet_size};siz={size};bak={bak};exp={expiration_time}")

        global_view.add_file(
            file_name,
            size,
            node_name,
            Name.to_str(fetch_path),
            packet_size,
            packets=packets,
            desired_copies=desired_copies,
            expiration_time=expiration_time,
        )

        global_view.set_backups(file_name, backup_list)

        # get pending stores
        copies_needed = desired_copies
        pending_stores = global_view.get_pending_stores(file_name)
        for pending_store in pending_stores:
            global_view.store_file(file_name, pending_store)
            copies_needed -= 1

        # if I need to store this file
        # if is_stored_by_origin:
        #     copies_needed -= 1
        need_to_store = False
        for i in range(copies_needed):
            backup = backup_list[i]
            if backup[0] == config['node_name']:
                need_to_store = True
                break
        if need_to_store:
            fetch_file(file_name, packets, packet_size, Name.to_str(fetch_path))

        # update session
        global_view.update_node(node_name, favor, self.seqno)
