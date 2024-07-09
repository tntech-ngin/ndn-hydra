# -------------------------------------------------------------
# NDN Hydra General Group Message
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/tntech-ngin/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

from __future__ import annotations

from ndn.encoding import *
from typing import Optional
from ndn_hydra.repo.protocol.tlv import HydraTlvTypes
from ndn_hydra.repo.group_messages.specific_message import SpecificMessage
from ndn_hydra.repo.group_messages.add import AddMessage
from ndn_hydra.repo.group_messages.remove import RemoveMessage
from ndn_hydra.repo.group_messages.update import UpdateMessage
from ndn_hydra.repo.group_messages.store import StoreMessage
from ndn_hydra.repo.group_messages.claim import ClaimMessage
from ndn_hydra.repo.group_messages.heartbeat import HeartbeatMessage


class MessageTypes:
    ADD = 1
    REMOVE = 2
    UPDATE = 3
    STORE = 4
    CLAIM = 5
    HEARTBEAT = 6


class Message(TlvModel):
    type = UintField(HydraTlvTypes.MESSAGE_TYPE)
    value = BytesField(HydraTlvTypes.MESSAGE)

    @staticmethod
    def specify(nid: str, seqno: int, message_bytes: bytes) -> Optional[SpecificMessage]:
        message = Message.parse(message_bytes)
        message_type, message_bytes = message.type, bytes(message.value)
        if message_type == MessageTypes.ADD:
            return AddMessage(nid, seqno, message_bytes)
        elif message_type == MessageTypes.REMOVE:
            return RemoveMessage(nid, seqno, message_bytes)
        elif message_type == MessageTypes.UPDATE:
            return UpdateMessage(nid, seqno, message_bytes)
        elif message_type == MessageTypes.STORE:
            return StoreMessage(nid, seqno, message_bytes)
        elif message_type == MessageTypes.CLAIM:
            return ClaimMessage(nid, seqno, message_bytes)
        elif message_type == MessageTypes.HEARTBEAT:
            return HeartbeatMessage(nid, seqno, message_bytes)
        else:
            return None