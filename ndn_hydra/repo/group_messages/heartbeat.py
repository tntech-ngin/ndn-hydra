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
    favor_parameters = ModelField(HeartbeatMessageTypes.FAVOR_PARAMETERS, FavorParameters)
    favor_weights = ModelField(HeartbeatMessageTypes.FAVOR_WEIGHTS, FavorWeights)


class HeartbeatMessage(SpecificMessage):
    def __init__(self, nid: str, seqno: int, raw_bytes: bytes):
        super(HeartbeatMessage, self).__init__(nid, seqno)
        self.message = HeartbeatMessageTlv.parse(raw_bytes)

        self.message.favor_weights = self.decode_favor_weights(self.message.favor_weights)
        self.message.favor_parameters = self.decode_favor_parameters(self.message.favor_parameters)

    @staticmethod
    def decode_favor_weights(favor_weights):
        print(f'\nDecoding favor weights: {favor_weights}\n')

        return {
            'remaining_storage': float(favor_weights.remaining_storage.to_str()),
            'bandwidth': float(favor_weights.bandwidth.to_str()),
            'rw_speed': float(favor_weights.rw_speed.to_str())
        }

    @staticmethod
    def decode_favor_parameters(favor_parameters):
        print(f'\nDecoding favor parameters: {favor_parameters}\n')

        return {
            'rtt': float(favor_parameters.rtt.to_str()),
            'num_users': float(favor_parameters.num_users.to_str()),
            'bandwidth': float(favor_parameters.bandwidth.to_str()),
            'network_cost': float(favor_parameters.network_cost.to_str()),
            'storage_cost': float(favor_parameters.storage_cost.to_str()),
            'remaining_storage': float(favor_parameters.remaining_storage.to_str()),
            'rw_speed': float(favor_parameters.rw_speed.to_str())
        }

    async def apply(self, global_view: GlobalView):
        node_name = self.message.node_name.to_str()
        favor_parameters = self.message.favor_parameters
        favor_weights = self.message.favor_weights

        print(f'[HeartbeatMessage] Calculating favor on heartbeat for node: {node_name}\n')

        favor_calculator = FavorCalculator()
        favor = favor_calculator.calculate_favor(favor_parameters, favor_weights)

        print(f'\n[HeartbeatMessage] Favor of node {node_name} is {favor} \n')

        self.logger.debug(f"[MSG][HB]   nam={node_name};fav={favor}")
        global_view.update_node(node_name, favor, self.seqno)
        self.logger.debug(f"{len(global_view.get_nodes())} nodes")
