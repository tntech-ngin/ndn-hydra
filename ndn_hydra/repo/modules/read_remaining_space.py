"""
Read the remaining space from the node
"""

import shutil


def convert_bytes(bytes_value):
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_value < 1024:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024


def get_remaining_space(current_node):
    disk_usage = shutil.disk_usage(current_node)
    free_space = disk_usage.free / 1024 / 1024 / 1024

    return free_space