#  @Project: NDN Hydra
#  @Date:    2024-03-04
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/tntech-ngin/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------
import numpy as np
from ndn.encoding import *
import struct
import shutil


# TODO:  Update favor objectives weights (NOT PARAMETERS)
class FavorWeightsTypes:
    REMAINING_STORAGE = 501,
    BANDWIDTH = 502,
    RW_SPEED = 503


class FavorParameterTypes:
    RTT = 501
    NUM_USERS = 502
    BANDWIDTH = 503
    NETWORK_COST = 504
    STORAGE_COST = 505
    REMAINING_STORAGE = 506
    RW_SPEED = 507


class FavorWeights(TlvModel):
    remaining_storage = BytesField(FavorWeightsTypes.REMAINING_STORAGE)
    bandwidth = BytesField(FavorWeightsTypes.BANDWIDTH)
    rw_speed = BytesField(FavorWeightsTypes.RW_SPEED)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.remaining_storage is not None:
            self.remaining_storage = struct.pack('f', self.remaining_storage)
        if self.bandwidth is not None:
            self.bandwidth = struct.pack('f', self.bandwidth)
        if self.rw_speed is not None:
            self.rw_speed = struct.pack('f', self.rw_speed)

    def __len__(self):
        length = 0
        if self.remaining_storage is not None:
            length += 4
        if self.bandwidth is not None:
            length += 4
        if self.rw_speed is not None:
            length += 4
        return length


class FavorParameters(TlvModel):
    rtt = UintField(FavorParameterTypes.RTT)
    num_users = UintField(FavorParameterTypes.NUM_USERS)
    bandwidth = UintField(FavorParameterTypes.BANDWIDTH)
    network_cost = UintField(FavorParameterTypes.NETWORK_COST)
    storage_cost = UintField(FavorParameterTypes.STORAGE_COST)
    remaining_storage = UintField(FavorParameterTypes.REMAINING_STORAGE)
    rw_speed = UintField(FavorParameterTypes.RW_SPEED)

    def __len__(self):
        length = 0
        if self.rtt is not None:
            length += 8
        if self.num_users is not None:
            length += 8
        if self.bandwidth is not None:
            length += 8
        if self.network_cost is not None:
            length += 8
        if self.storage_cost is not None:
            length += 8
        if self.remaining_storage is not None:
            length += 8
        return length


class FavorCalculator:
    """
    A class for abstracting favor calculations between two nodes.
    """
    @staticmethod
    def calculate_favor(self, favor_parameters: FavorParameters, favor_weights: FavorWeights) -> float:
        print(f'\nReceived parameters for calculation: {favor_parameters}\n')
        print(f'Received weights for calculation: {favor_weights}\n')
        favor = (favor_weights['remaining_storage'] * favor_parameters['remaining_storage']
                 + favor_weights['bandwidth'] * favor_parameters['bandwidth']
                 + favor_weights['rw_speed'] * favor_parameters['rw_speed'])
        return int(favor)
