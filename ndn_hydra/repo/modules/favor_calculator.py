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


class FavorParameters(TlvModel):
    rtt = UintField(FavorParameterTypes.RTT)
    num_users = UintField(FavorParameterTypes.NUM_USERS)
    bandwidth = UintField(FavorParameterTypes.BANDWIDTH)
    network_cost = UintField(FavorParameterTypes.NETWORK_COST)
    storage_cost = UintField(FavorParameterTypes.STORAGE_COST)
    remaining_storage = UintField(FavorParameterTypes.REMAINING_STORAGE)
    rw_speed = UintField(FavorParameterTypes.RW_SPEED)


class FavorCalculator:
    """
    A class for abstracting favor calculations between two nodes.
    """
    @staticmethod
    def calculate_favor(self, favor_parameters: FavorParameters, favor_weights: FavorWeights) -> float:
        print(f'Received parameters for calculation: {favor_parameters}\n')
        print(f'Received weights for calculation: {favor_weights}\n')
        favor = (favor_weights.remaining_storage * favor_parameters.remaining_storage
                 + favor_weights.bandwidth * favor_parameters.bandwidth
                 + favor_weights.rw_speed * favor_parameters.rw_speed)
        return int(favor)
