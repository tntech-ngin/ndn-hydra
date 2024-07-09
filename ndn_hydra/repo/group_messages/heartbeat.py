# -------------------------------------------------------------
# NDN Hydra Heartbeat Group Message
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/tntech-ngin/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

from typing import Callable
from ndn.encoding import *
import json
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
        decoded_weights = {
            'remaining_storage': float(bytes(favor_weights.remaining_storage)),
            'bandwidth': float(bytes(favor_weights.bandwidth)),
            'rw_speed': float(bytes(favor_weights.rw_speed))
        }
        return decoded_weights

    @staticmethod
    def decode_favor_parameters(favor_parameters):
        decoded_params = {
            'rtt': float(bytes(favor_parameters.rtt)),
            'num_users': float(bytes(favor_parameters.num_users)),
            'bandwidth': float(bytes(favor_parameters.bandwidth)),
            'network_cost': float(bytes(favor_parameters.network_cost)),
            'storage_cost': float(bytes(favor_parameters.storage_cost)),
            'remaining_storage': float(bytes(favor_parameters.remaining_storage)),
            'rw_speed': float(bytes(favor_parameters.rw_speed))
        }
        return decoded_params

    async def apply(self, global_view, data_storage, fetch_file, svs, config):
        node_name = self.message.node_name.tobytes().decode()
        favor_parameters = self.message.favor_parameters
        favor_weights = self.message.favor_weights

        favor_calculator = FavorCalculator()
        favor = favor_calculator.calculate_favor(favor_parameters, favor_weights)

        self.logger.debug(f"\n[MSG][HB]   "
                          f"\n\tNode Name={node_name};"
                          f"\n\tfavor={favor}")
        global_view.update_node(node_name, favor, self.seqno)
