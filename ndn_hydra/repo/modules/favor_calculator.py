#  @Project: NDN Hydra
#  @Date:    2024-03-04
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/tntech-ngin/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------
import numpy as np
from ndn.encoding import *
from typing import Union, Dict
import shutil


class FavorParameterTypes:
    RTT = 501
    NUM_USERS = 502
    BANDWIDTH = 503
    NETWORK_COST = 504
    STORAGE_COST = 505
    REMAINING_STORAGE = 506
    RW_SPEED = 507


class FavorWeightsTypes:
    REMAINING_STORAGE = 508
    BANDWIDTH = 509
    RW_SPEED = 510


class FavorWeights(TlvModel):
    remaining_storage = BytesField(FavorWeightsTypes.REMAINING_STORAGE)
    bandwidth = BytesField(FavorWeightsTypes.BANDWIDTH)
    rw_speed = BytesField(FavorWeightsTypes.RW_SPEED)


class FavorParameters(TlvModel):
    rtt = BytesField(FavorParameterTypes.RTT)
    num_users = BytesField(FavorParameterTypes.NUM_USERS)
    bandwidth = BytesField(FavorParameterTypes.BANDWIDTH)
    network_cost = BytesField(FavorParameterTypes.NETWORK_COST)
    storage_cost = BytesField(FavorParameterTypes.STORAGE_COST)
    remaining_storage = BytesField(FavorParameterTypes.REMAINING_STORAGE)
    rw_speed = BytesField(FavorParameterTypes.RW_SPEED)


class FavorCalculator:
    """
    A class for abstracting favor calculations between two nodes.
    """

    @staticmethod
    def calculate_favor(
            favor_parameters: Union[FavorParameters, Dict[str, float]],
            favor_weights: Union[FavorWeights, Dict[str, float]]
    ) -> float:
        favor = (favor_weights['remaining_storage'] * favor_parameters['remaining_storage']
                 + favor_weights['bandwidth'] * favor_parameters['bandwidth']
                 + favor_weights['rw_speed'] * favor_parameters['rw_speed'])
        return int(favor)

