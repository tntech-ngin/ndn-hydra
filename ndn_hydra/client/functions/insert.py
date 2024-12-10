# -------------------------------------------------------------
# NDN Hydra Insert Client
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/tntech-ngin/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

import logging
import time
from hashlib import blake2b
from ndn.app import NDNApp
from ndn.encoding import Name, Component, FormalName
from ndn_hydra.repo.protocol.base_models import InsertCommand, File
from ndn_hydra.repo.utils.pubsub import PubSub
from ndn_hydra.client.functions.query import HydraQueryClient

SEGMENT_SIZE = 8192


class HydraInsertClient(object):
    def __init__(self, app: NDNApp, client_prefix: FormalName, repo_prefix: FormalName) -> None:
        """
        This client inserts data packets from the remote repo.
        :param app: NDNApp.
        :param client_prefix: NonStrictName. Routable name to client.
        :param repo_prefix: NonStrictName. Routable name to remote repo.
        """
        self.app = app
        self.client_prefix = client_prefix
        self.repo_prefix = repo_prefix
        self.pb = PubSub(self.app, self.client_prefix)
        self.packets = []

    async def insert_file(self, file_name: FormalName, path: str) -> bool:
        """
        Insert a file associated with a file name to the remote repo
        """
        size, seg_cnt = 0, 0
        # The prefix to be stored in the repo
        packet_prefix = self.repo_prefix + file_name
        # The prefix to be published from the client
        publish_prefix = self.client_prefix + file_name

        # Check if the file already exists
        query_client = HydraQueryClient(self.app, self.client_prefix, self.repo_prefix)
        query = [Component.from_str("file")] + file_name
        query_result = await query_client.send_query(query)
        if query_result:
            print('File already exists, aborted insertion.')
            return False

        tic = time.perf_counter()
        with open(path, "rb") as f:
            data = f.read()
            size = len(data)
            seg_cnt = (len(data) + SEGMENT_SIZE - 1) // SEGMENT_SIZE
            final_block_id = Component.from_segment(seg_cnt - 1)
            inner_packets = [self.app.prepare_data(packet_prefix + [Component.from_segment(i)],
                                                  data[i * SEGMENT_SIZE:(i + 1) * SEGMENT_SIZE],
                                                  freshness_period=10000,
                                                  final_block_id=final_block_id)
                            for i in range(seg_cnt)]
            self.packets = [self.app.prepare_data(publish_prefix + [Component.from_segment(i)],
                                                    packet,
                                                    freshness_period=10000,
                                                    final_block_id=final_block_id)
                            for i, packet in enumerate(inner_packets)]

        print(f'\nCreated {seg_cnt} chunks under name {Name.to_str(publish_prefix)}')

        def on_interest(int_name, _int_param, _app_param):
            seg_no = Component.to_number(int_name[-1]) if Component.get_type(
                int_name[-1]) == Component.TYPE_SEGMENT else 0
            if seg_no < seg_cnt:
                self.app.put_raw_packet(self.packets[seg_no])
            if seg_no == (seg_cnt - 1):
                toc = time.perf_counter()
                print(f"The publication is complete! - total time (with disk): {toc - tic:0.4f} secs")

        self.app.route(publish_prefix)(on_interest)

        file = File()
        file.file_name = file_name
        file.packets = seg_cnt
        file.packet_size = SEGMENT_SIZE
        file.size = size
        cmd = InsertCommand()
        cmd.file = file
        cmd.fetch_path = publish_prefix
        cmd_bytes = cmd.encode()

        # publish msg to repo's insert topic
        await self.pb.wait_for_ready()
        is_success = await self.pb.publish(self.repo_prefix + ['insert'], cmd_bytes)
        if is_success:
            logging.debug('\nPublished an insert msg and was acknowledged by a subscriber')
        else:
            logging.debug('\nPublished an insert msg but was not acknowledged by a subscriber')
        return is_success
