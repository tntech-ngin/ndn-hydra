# -------------------------------------------------------------
# NDN Hydra Heartbeat Group Message
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
import struct
import json
from ndn_hydra.repo.modules.global_view import GlobalView
from ndn_hydra.repo.modules.favor_calculator import FavorCalculator, FavorParameters, FavorWeights
from ndn_hydra.repo.group_messages.specific_message import SpecificMessage


class HeartbeatMessageTypes:
    NODE_NAME = 84
    FAVOR_PARAMETERS = 85
    FAVOR_WEIGHTS = 86


class HeartbeatMessageTlv(TlvModel):
    node_name = BytesField(HeartbeatMessageTypes.NODE_NAME)
    favor_parameters = ModelField(HeartbeatMessageTypes.FAVOR_PARAMETERS, FavorParameters)
    favor_weights = RepeatedField(BytesField(HeartbeatMessageTypes.FAVOR_WEIGHTS, fixed_len=4))

    @favor_weights.creator
    def create_favor_weight(self, value):
        return struct.pack('f', value)

    @favor_weights.decoder
    def decode_favor_weight(self, value):
        return struct.unpack('f', value)[0]


class HeartbeatMessage(SpecificMessage):
    def __init__(self, nid: str, seqno: int, raw_bytes: bytes):
        super(HeartbeatMessage, self).__init__(nid, seqno)
        self.message = HeartbeatMessageTlv.parse(raw_bytes)

    async def apply(self, global_view: GlobalView):
        node_name = self.message.node_name
        favor = FavorCalculator().calculate_favor(self, self.message.favor_parameters, self.message.favor_weights)

        print(f'\nFavor of node {str(node_name)} is {str(favor)} \n')

        self.logger.debug(f"[MSG][HB]   nam={str(node_name)};fav={favor}")
        global_view.update_node(str(node_name), favor, self.seqno)
        self.logger.debug(f"{len(global_view.get_nodes())} nodes")
