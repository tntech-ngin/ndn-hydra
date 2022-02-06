# -------------------------------------------------------------
# NDN Hydra Store Group Message
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
from ndn_hydra.repo.group_messages.specific_message import SpecificMessage
from ndn_hydra.repo.global_view.global_view import GlobalView

class StoreMessageTypes:
    SESSION_ID = 83
    NODE_NAME = 84
    EXPIRE_AT = 85
    FAVOR = 86

    INSERTION_ID = 90

class StoreMessageTlv(TlvModel):
    session_id = BytesField(StoreMessageTypes.SESSION_ID)
    node_name = BytesField(StoreMessageTypes.NODE_NAME)
    expire_at = UintField(StoreMessageTypes.EXPIRE_AT)
    favor = BytesField(StoreMessageTypes.FAVOR)
    insertion_id = BytesField(StoreMessageTypes.INSERTION_ID)

class StoreMessage(SpecificMessage):
    def __init__(self, nid:str, seqno:int, raw_bytes:bytes):
        super(StoreMessage, self).__init__(nid, seqno)
        self.message = StoreMessageTlv.parse(raw_bytes)

    async def apply(self, global_view: GlobalView, fetch_file: Callable, svs, config):
        session_id = self.message.session_id.tobytes().decode()
        node_name = self.message.node_name.tobytes().decode()
        expire_at = self.message.expire_at
        favor = float(self.message.favor.tobytes().decode())
        insertion_id = self.message.insertion_id.tobytes().decode()
        val = "[MSG][STORE]   sid={sid};iid={iid}".format(
            sid=session_id,
            iid=insertion_id
        )
        self.logger.info(val)
        # if insertion
        insertion = global_view.get_insertion(insertion_id)
        if (insertion == None) or (insertion['is_deleted'] == True):
            # add store to pending_stores
            self.logger.warning('add to pending store')
            global_view.add_pending_store(insertion_id, session_id)
        else:
            global_view.store_file(insertion_id, session_id)
        # update session
        global_view.update_session(session_id, node_name, expire_at, favor, self.seqno)
        return