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
from ndn_hydra.repo.modules.global_view import GlobalView
from ndn_hydra.repo.modules.favor_calculator import FavorCalculator, FavorParameters, FavorWeights
from ndn_hydra.repo.group_messages.specific_message import SpecificMessage


class HeartbeatMessageTypes:
    NODE_NAME = 84
    FAVOR_PARAMETERS = 85
    FAVOR_WEIGHTS = 86


class HeartbeatMessageTlv(TlvModel):
    node_name = BytesField(HeartbeatMessageTypes.NODE_NAME)
    favor_parameters = TlvModelField(HeartbeatMessageTypes.FAVOR_PARAMETERS, FavorParameters)
    favor_weights = TlvModelField(HeartbeatMessageTypes.FAVOR_WEIGHTS, FavorWeights)


class HeartbeatMessage(SpecificMessage):
    def __init__(self, nid: str, seqno: int, raw_bytes: bytes):
        super(HeartbeatMessage, self).__init__(nid, seqno)
        self.message = HeartbeatMessageTlv.parse(raw_bytes)
        self.message.favor_weights = self.decode_favor_weights(self.message.favor_weights)
        self.message.favor_parameters = self.decode_favor_parameters(self.message.favor_parameters)

    @staticmethod
    def decode_favor_weights(favor_weights):
        return {
            'remaining_storage': float(favor_weights.remaining_storage.decode('utf-8')),
            'bandwidth': float(favor_weights.bandwidth.decode('utf-8')),
            'rw_speed': float(favor_weights.rw_speed.decode('utf-8'))
        }

    @staticmethod
    def decode_favor_parameters(favor_parameters):
        return {
            'rtt': float(favor_parameters.rtt.decode('utf-8')),
            'num_users': float(favor_parameters.num_users.decode('utf-8')),
            'bandwidth': float(favor_parameters.bandwidth.decode('utf-8')),
            'network_cost': float(favor_parameters.network_cost.decode('utf-8')),
            'storage_cost': float(favor_parameters.storage_cost.decode('utf-8')),
            'remaining_storage': float(favor_parameters.remaining_storage.decode('utf-8')),
            'rw_speed': float(favor_parameters.rw_speed.decode('utf-8'))
        }

    async def apply(self, global_view: GlobalView):
        node_name = self.message.node_name.decode('utf-8')
        favor_parameters = self.message.favor_parameters
        favor_weights = self.message.favor_weights

        favor_calculator = FavorCalculator()
        favor = favor_calculator.calculate_favor(favor_parameters, favor_weights)

        print(f'\nFavor of node {node_name} is {favor} \n')

        self.logger.debug(f"[MSG][HB]   nam={node_name};fav={favor}")
        global_view.update_node(node_name, favor, self.seqno)
        self.logger.debug(f"{len(global_view.get_nodes())} nodes")
