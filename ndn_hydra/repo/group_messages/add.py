# -------------------------------------------------------------
# NDN Hydra Add Group Message
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

from typing import Callable
from ndn.encoding import *
import time
from ndn_hydra.repo.modules.global_view import GlobalView
from ndn_hydra.repo.group_messages.specific_message import SpecificMessage
from ndn_hydra.repo.protocol.base_models import File

class AddMessageTypes:
    SESSION_ID = 83
    NODE_NAME = 84
    EXPIRE_AT = 85
    FAVOR = 86

    INSERTION_ID = 90
    FILE = 91
    DESIRED_COPIES = 92
    PACKETS = 93
    DIGEST = 94
    SIZE = 95
    SEQUENCE_NUMBER = 96
    FETCH_PATH = 97
    IS_STORED_BY_ORIGIN = 98

    BACKUP = 100
    BACKUP_SESSION_ID = 101
    BACKUP_NONCE = 102

class FetchPathTlv(TlvModel):
    prefix = NameField()

class BackupTlv(TlvModel):
    session_id = BytesField(AddMessageTypes.BACKUP_SESSION_ID)
    nonce = BytesField(AddMessageTypes.BACKUP_NONCE)

class AddMessageTlv(TlvModel):
    session_id = BytesField(AddMessageTypes.SESSION_ID)
    node_name = BytesField(AddMessageTypes.NODE_NAME)
    expire_at = UintField(AddMessageTypes.EXPIRE_AT)
    favor = BytesField(AddMessageTypes.FAVOR)
    insertion_id = BytesField(AddMessageTypes.INSERTION_ID)
    file = ModelField(AddMessageTypes.FILE, File)

    desired_copies = UintField(AddMessageTypes.DESIRED_COPIES)
    sequence_number = UintField(AddMessageTypes.SEQUENCE_NUMBER)
    fetch_path = ModelField(AddMessageTypes.FETCH_PATH, FetchPathTlv)
    is_stored_by_origin = UintField(AddMessageTypes.IS_STORED_BY_ORIGIN)
    backup_list = RepeatedField(ModelField(AddMessageTypes.BACKUP, BackupTlv))

class AddMessage(SpecificMessage):
    def __init__(self, nid:str, seqno:int, raw_bytes:bytes):
        super(AddMessage, self).__init__(nid, seqno)
        self.message = AddMessageTlv.parse(raw_bytes)

    async def apply(self, global_view: GlobalView, fetch_file: Callable, svs, config):
        session_id = self.message.session_id.tobytes().decode()
        node_name = self.message.node_name.tobytes().decode()
        expire_at = self.message.expire_at
        favor = float(self.message.favor.tobytes().decode())
        insertion_id = self.message.insertion_id.tobytes().decode()
        file = self.message.file
        file_name = file.file_name
        packets = file.packets
        digests = file.digests
        size = file.size
        desired_copies = self.message.desired_copies
        sequence_number = self.message.sequence_number
        fetch_path = self.message.fetch_path.prefix
        is_stored_by_origin = False if (self.message.is_stored_by_origin == 0) else True
        backups = self.message.backup_list
        backup_list = []
        bak = ""
        for backup in backups:
            backup_list.append((backup.session_id.tobytes().decode(), backup.nonce.tobytes().decode()))
            bak = bak + backup.session_id.tobytes().decode() + ","
        val = "[MSG][ADD]     sid={sid};iid={iid};file={fil};cop={cop};pck={pck};siz={siz};seq={seq};slf={slf};bak={bak}".format(
            sid=session_id,
            iid=insertion_id,
            fil=Name.to_str(file_name),
            cop=desired_copies,
            pck=packets,
            siz=size,
            seq=sequence_number,
            slf=1 if is_stored_by_origin else 0,
            bak=bak
        )
        self.logger.info(val)
        global_view.add_insertion(
            insertion_id,
            Name.to_str(file_name),
            sequence_number,
            size,
            session_id,
            Name.to_str(fetch_path),
            self.seqno,
            b''.join(digests),
            packets=packets,
            desired_copies=desired_copies
        )

        global_view.set_backups(insertion_id, backup_list)

        # get pending stores
        copies_needed = desired_copies
        pending_stores = global_view.get_pending_stores(insertion_id)
        for pending_store in pending_stores:
            # data_storage.add_metainfos(insertion_id, Name.to_str(file_name), packets, digests, Name.to_str(fetch_path))
            global_view.store_file(insertion_id, pending_store)
            copies_needed -= 1

        # if I need to store this file
        # if is_stored_by_origin:
        #     copies_needed -= 1
        need_to_store = False
        for i in range(copies_needed):
            backup = backup_list[i]
            if backup[0] == config['session_id']:
                need_to_store = True
                break
        if need_to_store == True:
            fetch_file(insertion_id, Name.to_str(file_name), packets, digests, Name.to_str(fetch_path))

            # from .message import MessageTlv, MessageTypes
            # # generate store msg and send
            # # store tlv
            # expire_at = int(time.time()+(config['period']*2))
            # favor = 1.85
            # store_message = StoreMessageTlv()
            # store_message.session_id = config['session_id'].encode()
            # store_message.node_name = config['node_name'].encode()
            # store_message.expire_at = expire_at
            # store_message.favor = str(favor).encode()
            # store_message.insertion_id = insertion_id.encode()
            # # store msg
            # store_message = MessageTlv()
            # store_message.type = MessageTypes.STORE
            # store_message.value = store_message.encode()
            # # apply globalview and send msg thru SVS
            # # next_state_vector = svs.getCore().getStateVector().get(config['session_id']) + 1

            # # global_view.store_file(insertion_id, config['session_id'])
            # svs.publishData(store_message.encode())
            # val = "[MSG][STORE]*  sid={sid};iid={iid}".format(
            #     sid=config['session_id'],
            #     iid=insertion_id
            # )
            # self.logger.info(val)
        # update session
        global_view.update_session(session_id, node_name, expire_at, favor, self.seqno)
        return
