"""
Read the remaining space from the node
"""

import os
import shutil


def get_remaining_space():
    current_node = os.getcwd()
    disk_usage = shutil.disk_usage(current_node)
    free_space = disk_usage.free

    return free_space

