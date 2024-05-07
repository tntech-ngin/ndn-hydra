import numpy as np
from pymoo.core.problem import Problem


class ReplicationProblem(Problem):
    def __init__(self, rtt, bandwidth, v, c1, c2, C_N, popularity, user_node):
        super().__init__(n_var=10, n_obj=4, n_constr=0, xl=0, xu=1)
        self.rtt = rtt
        self.bandwidth = bandwidth
        self.v = v
        self.c1 = c1
        self.c2 = c2
        self.C_N = C_N
        self.popularity = popularity
        self.user_node = user_node
        self.nodes = ["MICH", "UTAH", "TACC", "WASH", "NCSA", "DALL", "MAX", "MASS", "SALT", "STAR"]
        self.node_capacities = [10, 15, 20, 25, 30, 35, 40, 45, 50, 55]  # Example node capacities

    def _evaluate(self, X, out, *args, **kwargs):
        f1 = self.objective1(X)
        f2 = self.objective2(X)
        f3 = self.objective3(X)
        f4 = self.objective4(X)
        out["F"] = np.column_stack([f1, f2, f3, f4])

    def objective1(self, X):  # time
        rep_time = np.zeros(X.shape[0])
        for i in range(X.shape[0]):
            selected_nodes = [self.nodes[j] for j in range(len(X[i])) if X[i][j] > 0.5]
            for node in selected_nodes:
                node_index = self.nodes.index(node)
                user_node_index = self.nodes.index(self.user_node)
                bandwidth_value = self.bandwidth[0] if self.bandwidth[0] != 0 else 1
                rtt_value = self.rtt[user_node_index][node_index] if self.rtt[user_node_index][node_index] != 0 else 1
                rep_time[i] += self.C_N / (bandwidth_value * rtt_value)
        return rep_time

    def objective2(self, X):  # cost
        cost = np.zeros(X.shape[0])
        for i in range(X.shape[0]):
            selected_nodes = [self.nodes[j] for j in range(len(X[i])) if X[i][j] > 0.5]
            cost[i] = len(selected_nodes) * (self.c1[0] * self.C_N + self.c2[0] * self.C_N)
        return cost

    def objective3(self, X):
        fault_tolerance = np.zeros(X.shape[0])
        for i in range(X.shape[0]):
            num_replicas = np.sum(X[i] > 0.5)
            fault_tolerance[i] = num_replicas
        return -fault_tolerance

    def objective4(self, X):
        load_std_dev = np.zeros(X.shape[0])
        for i in range(X.shape[0]):
            load_distribution = np.zeros(len(self.nodes))
            selected_nodes = [self.nodes[j] for j in range(len(X[i])) if X[i][j] > 0.5]
            for node in selected_nodes:
                node_index = self.nodes.index(node)
                load_distribution[node_index] += self.C_N / self.node_capacities[node_index]
            load_std_dev[i] = np.std(load_distribution)
        return load_std_dev

    def has_bounds(self):
        return False
