# -------------------------------------------------------------
# NDN Hydra Remove Group Message
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/tntech-ngin/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

import asyncio as aio
from typing import Callable
from ndn.encoding import *
from ndn_hydra.repo.modules.global_view import GlobalView
from ndn_hydra.repo.group_messages.specific_message import SpecificMessage
from ndn_hydra.repo.modules.file_remover import remove_file


class RemoveMessageTypes:
    NODE_NAME = 84
    FAVOR = 86


class RemoveMessageTlv(TlvModel):
    node_name = BytesField(RemoveMessageTypes.NODE_NAME)
    favor = BytesField(RemoveMessageTypes.FAVOR)
    file_name = NameField()


class RemoveMessage(SpecificMessage):
    def __init__(self, nid: str, seqno: int, raw_bytes: bytes):
        super(RemoveMessage, self).__init__(nid, seqno)
        self.message = RemoveMessageTlv.parse(raw_bytes)

    async def apply(self, global_view, data_storage, fetch_file, svs, config):
        node_name = self.message.node_name.tobytes().decode()
        file_name = Name.to_str(self.message.file_name)

        self.logger.info(f"\n[MSG][REMOVE]   "
                         f"\n\tFile name={file_name}")
        file = global_view.get_file(file_name)
        if not file:
            self.logger.warning('nothing to remove')
        else:
            # Delete from global view
            global_view.delete_file(file_name)
            # Remove from data_storage from this node if present
            if config['node_name'] in file['stores']:
                remove_file(data_storage, file)

        global_view.update_node(node_name, float(self.message.favor.tobytes().decode()), self.seqno)
