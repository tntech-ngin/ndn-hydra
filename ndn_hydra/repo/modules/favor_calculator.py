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


class FavorParameterTypes:
    RTT = 100
    NETWORK_COST_PER_GB = 0.01
    STORAGE_COST_PER_GB = 0.014
    NUM_USERS = 100
    BANDWIDTH = 25000 #Mbps
    NETWORK_COST = NETWORK_COST_PER_GB * (BANDWIDTH/(1000*8)) #0.01 USD/GB  
    RW_SPEED = 6.25
    TOTAL_STORAGE, USED_STORAGE, REMAINING_STORAGE = shutil.disk_usage(__file__)
    STORAGE_COST = REMAINING_STORAGE * STORAGE_COST_PER_GB

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
    def calculate_favor(self, favor_parameters: FavorParameters) -> float:
        favor = 0
        #for param, val in favor_parameters.asdict().items():
            # print(param, ':', val)
        #    favor += int(val)
        # print('favor:', favor)
        favor = (.3*favor_parameters.remaining_storage + .3*favor_parameters.bandwidth + .4*favor_parameters.rw_speed* + 0.0*favor_parameters.num_users
                 + 0.0*favor_parameters.network_cost + 0.0*favor_parameters.storage_cost)
        return int(favor)

