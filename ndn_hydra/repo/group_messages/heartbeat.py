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


class FloatArrayField(BytesField):
    @staticmethod
    def encode(values):
        encoded_bytes = b''
        for value in values:
            encoded_bytes += struct.pack('f', value)
        return encoded_bytes

    @staticmethod
    def decode(bytes_value):
        decoded_values = []
        for i in range(0, len(bytes_value), 4):
            value_bytes = bytes_value[i:i+4]
            decoded_values.append(struct.unpack('f', value_bytes)[0])
        return decoded_values

    def __len__(self):
        value = self.get_value()
        if value is None:
            return 0
        else:
            return len(value) * 4


class HeartbeatMessageTypes:
    NODE_NAME = 84
    FAVOR_PARAMETERS = 85
    FAVOR_WEIGHTS = 86


class HeartbeatMessageTlv(TlvModel):
    node_name = BytesField(HeartbeatMessageTypes.NODE_NAME)
    favor_parameters = ModelField(HeartbeatMessageTypes.FAVOR_PARAMETERS, FavorParameters)
    favor_weights = RepeatedField(FloatArrayField(HeartbeatMessageTypes.FAVOR_WEIGHTS))

    def __len__(self):
        length = 0
        if self.node_name is not None:
            length += len(self.node_name)
        if self.favor_parameters is not None:
            length += len(self.favor_parameters)
        if self.favor_weights is not None:
            length += len(self.favor_weights)
        return length


class HeartbeatMessage(SpecificMessage):
    def __init__(self, nid: str, seqno: int, raw_bytes: bytes):
        super(HeartbeatMessage, self).__init__(nid, seqno)
        self.message = HeartbeatMessageTlv.parse(raw_bytes)
        self.message.favor_weights = self.decode_favor_weights(self.message.favor_weights)
        self.message.favor_weights = self.decode_favor_weights(self.message.favor_weights)

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

    @staticmethod
    def decode_favor_weights(favor_weights):
        return {
            'remaining_storage': float(favor_weights.remaining_storage.decode('utf-8')),
            'bandwidth': float(favor_weights.bandwidth.decode('utf-8')),
            'rw_speed': float(favor_weights.rw_speed.decode('utf-8'))
        }

    async def apply(self, global_view: GlobalView):
        node_name = self.message.node_name.decode('utf-8')
        favor_parameters = self.message.favor_parameters
        favor_weights = self.message.favor_weights

        favor = FavorCalculator().calculate_favor(favor_parameters, favor_weights)

        print(f'\nFavor of node {str(node_name)} is {str(favor)} \n')

        self.logger.debug(f"[MSG][HB]   nam={str(node_name)};fav={favor}")
        global_view.update_node(str(node_name), favor, self.seqno)
        self.logger.debug(f"{len(global_view.get_nodes())} nodes")
