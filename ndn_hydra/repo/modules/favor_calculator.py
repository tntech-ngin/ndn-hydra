#  @Project: NDN Hydra
#  @Date:    2024-03-04
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/tntech-ngin/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------
import numpy as np
from ndn.encoding import *
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


# TODO: Check with Dr. Shannigrahi what I should do becuase TLV don't have a flot type
class FavorWeights(TlvModel):
    remaining_storage = UintField(FavorWeightsTypes.REMAINING_STORAGE)
    bandwidth = UintField(FavorWeightsTypes.BANDWIDTH)
    rw_speed = UintField(FavorWeightsTypes.RW_SPEED)

    def __len__(self):
        length = 0
        if self.remaining_storage is not None:
            length += len(self.remaining_storage)
        if self.bandwidth is not None:
            length += len(self.bandwidth)
        if self.rw_speed is not None:
            length += len(self.rw_speed)
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
            length += len(self.rtt)
        if self.num_users is not None:
            length += len(self.num_users)
        if self.bandwidth is not None:
            length += len(self.bandwidth)
        if self.network_cost is not None:
            length += len(self.network_cost)
        if self.storage_cost is not None:
            length += len(self.storage_cost)
        if self.remaining_storage is not None:
            length += len(self.remaining_storage)
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
