#  @Project: NDN Hydra
#  @Date:    2024-03-04
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/tntech-ngin/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------
import numpy as np
from ndn.encoding import *
from replication_problem import ReplicationProblem
from sklearn.preprocessing import MinMaxScaler
from pymoo.algorithms.moo.nsga3 import NSGA3
from pymoo.optimize import minimize

import shutil


# TODO:  Update favor objectives weights (NOT PARAMETERS)
class FavorWeights:
    NETWORK_COST_PER_GB = 0.01,
    STORAGE_COST_PER_GB = 0.014,


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


def favor_cal(rtt, bandwidth, v, c1, c2, C_N, popularity, user_node, weights):
    problem = ReplicationProblem(rtt, bandwidth, v, c1, c2, C_N, popularity, user_node)
    ref_dirs = np.array([[1, 0, 0, 0]])
    algorithm = NSGA3(pop_size=100, ref_dirs=ref_dirs)
    res = minimize(problem,
                   algorithm,
                   ('n_gen', 600),
                   verbose=True)

    # Apply entropy weight TOPSIS
    F = res.F
    normalized_F = MinMaxScaler().fit_transform(F)
    weighted_F = normalized_F * weights
    ideal_solution = np.max(weighted_F, axis=0)
    anti_ideal_solution = np.min(weighted_F, axis=0)
    d_positive = np.linalg.norm(weighted_F - ideal_solution, axis=1)
    d_negative = np.linalg.norm(weighted_F - anti_ideal_solution, axis=1)

    with np.errstate(divide='ignore', invalid='ignore'):
        relative_closeness = np.where(d_positive + d_negative == 0, 0, d_negative / (d_positive + d_negative))

    best_index = np.argmax(relative_closeness)
    best_solution = res.X[best_index]

    return best_solution, F


def entropy_weight(F):
    normalized_F = MinMaxScaler().fit_transform(F)

    entropy = np.zeros(F.shape[1])
    for j in range(F.shape[1]):
        p = normalized_F[:, j] / np.sum(normalized_F[:, j])
        entropy[j] = -np.sum(np.where(p == 0, 0, p * np.log(p))) / np.log(F.shape[0])

    entropy = np.nan_to_num(entropy)

    weights = (1 - entropy) / np.sum(1 - entropy)

    return weights

