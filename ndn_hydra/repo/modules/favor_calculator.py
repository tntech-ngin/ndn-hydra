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
favor_weights = (
    0.01,  # NETWORK_COST_PER_GB
    0.014, # STORAGE_COST_PER_GB
)


class FavorParameterTypes:
    RTT = 501
    # NETWORK_COST_PER_GB = 0.01
    # STORAGE_COST_PER_GB = 0.014
    NUM_USERS = 502
    BANDWIDTH = 503
    NETWORK_COST = 504
    STORAGE_COST = 505
    REMAINING_STORAGE = 506
    RW_SPEED = 507

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
    def calculate_favor(self, favor_parameters: FavorParameters) -> float:
        rw_speed = 6.25 if favor_parameters.rw_speed is None else favor_parameters.rw_speed
        favor = .3 * favor_parameters.remaining_storage + .3 * favor_parameters.bandwidth + .4 * rw_speed
        return int(favor)

