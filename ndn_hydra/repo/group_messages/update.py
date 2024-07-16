# -------------------------------------------------------------
# NDN Hydra Update Group Message
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/tntech-ngin/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

from typing import Callable
from ndn.encoding import *
from ndn_hydra.repo.modules.global_view import GlobalView
from ndn_hydra.repo.group_messages.specific_message import SpecificMessage


class UpdateMessageTypes:
    NODE_NAME = 84
    FAVOR = 86
    EXPIRATION_DATE = 95


class UpdateMessageTlv(TlvModel):
    node_name = BytesField(UpdateMessageTypes.NODE_NAME)
    favor = BytesField(UpdateMessageTypes.FAVOR)
    file_name = NameField()
    expiration_time = UintField(UpdateMessageTypes.EXPIRATION_DATE)


class UpdateMessage(SpecificMessage):
    def __init__(self, nid: str, seqno: int, raw_bytes: bytes):
        super(UpdateMessage, self).__init__(nid, seqno)
        self.message = UpdateMessageTlv.parse(raw_bytes)

    async def apply(self, global_view, data_storage, fetch_file, svs, config):
        node_name = self.message.node_name.tobytes().decode()
        file_name = Name.to_str(self.message.file_name)
        expiration_time = self.message.expiration_time

        self.logger.info(f"\n[MSG][UPDATE]   "
                         f"\n\tFile name={file_name}")
        file = global_view.get_file(file_name)
        if not file:
            self.logger.warning('\n*** Nothing to update')
        else:
            global_view.update_file(file_name, expiration_time)

        global_view.update_node(node_name, float(self.message.favor.tobytes().decode()), self.seqno)
