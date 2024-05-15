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

    def encode(self, remaining_storage_str, bandwidth_str, rw_speed_str):
        self.remaining_storage = remaining_storage_str.encode('utf-8')
        self.bandwidth = bandwidth_str.encode('utf-8')
        self.rw_speed = rw_speed_str.encode('utf-8')
        return super().encode()


class FavorParameters(TlvModel):
    rtt = BytesField(FavorParameterTypes.RTT)
    num_users = BytesField(FavorParameterTypes.NUM_USERS)
    bandwidth = BytesField(FavorParameterTypes.BANDWIDTH)
    network_cost = BytesField(FavorParameterTypes.NETWORK_COST)
    storage_cost = BytesField(FavorParameterTypes.STORAGE_COST)
    remaining_storage = BytesField(FavorParameterTypes.REMAINING_STORAGE)
    rw_speed = BytesField(FavorParameterTypes.RW_SPEED)

    def encode(self, rtt_str, num_users_str, bandwidth_str, network_cost_str, storage_cost_str, remaining_storage_str, rw_speed_str):
        print(f'\nEncoding favor parameters')
        print(f'\nrtt_str: {rtt_str}\n')
        print(f'\nnum_users_str: {num_users_str}\n')
        print(f'\nbandwidth_str: {bandwidth_str}\n')
        print(f'\nnetwork_cost_str: {network_cost_str}\n')
        print(f'\nstorage_cost_str: {storage_cost_str}\n')
        print(f'\nremaining_storage_str: {remaining_storage_str}\n')
        print(f'\nrw_speed_str: {rw_speed_str}\n')

        self.rtt = rtt_str.encode('utf-8') if rtt_str else None
        self.num_users = num_users_str.encode('utf-8') if num_users_str else None
        self.bandwidth = bandwidth_str.encode('utf-8') if bandwidth_str else None
        self.network_cost = network_cost_str.encode('utf-8') if network_cost_str else None
        self.storage_cost = storage_cost_str.encode('utf-8') if storage_cost_str else None
        self.remaining_storage = remaining_storage_str.encode('utf-8') if remaining_storage_str else None
        self.rw_speed = rw_speed_str.encode('utf-8') if rw_speed_str else None
        return super().encode()


class FavorCalculator:
    """
    A class for abstracting favor calculations between two nodes.
    """
    @staticmethod
    def calculate_favor(favor_parameters: FavorParameters, favor_weights: FavorWeights) -> float:
        print(f'\nReceived parameters for favor calculation: {favor_parameters}\n')
        print(f'Received weights for favor calculation: {favor_weights}\n')

        favor = (favor_weights['remaining_storage'] * favor_parameters['remaining_storage']
                 + favor_weights['bandwidth'] * favor_parameters['bandwidth']
                 + favor_weights['rw_speed'] * favor_parameters['rw_speed'])
        return int(favor)
