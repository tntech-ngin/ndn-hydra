# -------------------------------------------------------------
# NDN Hydra Read Handle
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/tntech-ngin/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

import asyncio as aio
import logging
import time
from secrets import choice
from ndn.app import NDNApp
from ndn.encoding import Name, ContentType, Component, parse_data
from ndn.storage import Storage
from ndn_hydra.repo.main.main_loop import MainLoop
from ndn_hydra.repo.modules.global_view import GlobalView
from ndn_hydra.repo.group_messages.update import UpdateMessageTlv
from ndn_hydra.repo.group_messages.message import Message, MessageTypes


class ReadHandle(object):
    """
    ReadHandle processes ordinary interests, and return corresponding data if exists.
    """
    def __init__(self, app: NDNApp, data_storage: Storage, global_view: GlobalView, main_loop: MainLoop, config: dict):
        """
        :param app: NDNApp.
        :param data_storage: Storage.
        :param global_view: Global View.
        :param config: All config Info.
        """
        self.app = app
        self.data_storage = data_storage
        self.global_view = global_view
        self.main_loop = main_loop
        self.node_name = config['node_name']
        self.repo_prefix = config['repo_prefix']
        self.file_expiration = config['file_expiration']

        self.logger = logging.getLogger()

        self.command_comp = "/fetch"
        self.node_comp = "/node"
        # config file needed

        self.listen(Name.from_str(self.repo_prefix + self.command_comp))
        self.listen(Name.from_str(self.repo_prefix + self.node_comp  + self.node_name + self.command_comp))

    def listen(self, prefix):
        """
        This function needs to be called for prefix of all data stored.
        :param prefix: NonStrictName.
        """
        self.app.route(prefix)(self._on_interest)
        self.logger.info(f'\nRead handle: listening to {Name.to_str(prefix)}')

    def unlisten(self, prefix):
        """
        :param name: NonStrictName.
        """
        aio.ensure_future(self.app.unregister(prefix))
        self.logger.info(f'\nRead handle: stop listening to {Name.to_str(prefix)}')

    def _on_interest(self, int_name, int_param, _app_param):
        """
        Repo should not respond to any interest with MustBeFresh flag set.
        Repo will:
        - Reply with data of its own
        - Nack if data can not be found within the repo
        - Reply with a redirect to another node
        Assumptions:
        - A node on the on list will have the file in complete form
        """
        if int_param.must_be_fresh:
            return

        # get rid of the security part if any on the int_name
        file_name = self._get_file_name_from_interest(Name.to_str(int_name[:-1]))
        best_id = self._best_id_for_file(file_name)
        segment_comp = "/" + Component.to_str(int_name[-1])

        if best_id is None:
            if segment_comp == "/seg=0":
                self.logger.info(f'\n[CMD][FETCH]    nacked due to no file')

            # nack due to lack of avaliability
            self.app.put_data(int_name, content=None, content_type=ContentType.NACK)
            self.logger.debug(f"\nRead handle: data not found {Name.to_str(int_name)}")
            return

        if best_id == self.node_name:
            if segment_comp == "/seg=0":
                self.logger.info(f'\n[CMD][FETCH]    serving file')
                self._reset_file_expiration(file_name)

            # serving my own data
            data_bytes = self.data_storage.get_packet(file_name + segment_comp, int_param.can_be_prefix)
            if data_bytes == None:
                return

            self.logger.debug(f'\nRead handle: serve data {Name.to_str(int_name)}')
            _, _, content, _ = parse_data(data_bytes)
            # print("serve"+file_name + segment_comp+"   "+Name.to_str(name))
            final_id = Component.from_number(int(self.global_view.get_file(file_name)["packets"])-1, Component.TYPE_SEGMENT)
            self.app.put_data(int_name, content=content, content_type=ContentType.BLOB, final_block_id=final_id)
        else:
            if segment_comp == "/seg=0":
                self.logger.info(f'\n[CMD][FETCH]    linked to another node')

            # create a link to a node who has the content
            new_name = self.repo_prefix + self.node_comp + best_id + self.command_comp + file_name
            link_content = bytes(new_name.encode())
            final_id = Component.from_number(int(self.global_view.get_file(file_name)["packets"])-1, Component.TYPE_SEGMENT)
            self.app.put_data(int_name, content=link_content, content_type=ContentType.LINK, final_block_id=final_id)

    def _get_file_name_from_interest(self, int_name):
        file_name = int_name[len(self.repo_prefix):]
        if file_name[0:len(self.node_comp)] == self.node_comp:
            return file_name[(len(self.node_comp)+len(self.node_name)+len(self.command_comp)):]
        else:
            return file_name[(len(self.command_comp)):]

    def _best_id_for_file(self, file_name: str):
        file_info = self.global_view.get_file(file_name)
        if not file_name or not file_info:
            return None
        active_nodes = set( [x['node_name'] for x in self.global_view.get_nodes()] )
        on_list = file_info["stores"]
        if not on_list:
            return None
        if self.node_name in on_list:
            return self.node_name
        else:
            on_list = [x for x in on_list if x in active_nodes]
            return choice(on_list)

    def _reset_file_expiration(self, file_name):
        if self.file_expiration == 0:  # no need to reset if file_expiration in config is set to 0
            return
        expiration_time = int(time.time() + (self.file_expiration * 60 * 60))  # convert hours to seconds

        # update tlv
        favor = self.global_view.get_node(self.node_name)['favor']
        update_message = UpdateMessageTlv()
        update_message.node_name = self.node_name.encode()
        update_message.favor = str(favor).encode()
        update_message.file_name = file_name
        update_message.expiration_time = expiration_time
        # update msg
        message = Message()
        message.type = MessageTypes.UPDATE
        message.value = update_message.encode()

        # publish
        self.global_view.update_file(file_name, expiration_time)
        self.main_loop.svs.publishData(message.encode())