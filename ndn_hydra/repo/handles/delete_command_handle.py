# -------------------------------------------------------------
# NDN Hydra Delete Command Handle
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/tntech-ngin/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

import asyncio as aio
import logging
from ndn.app import NDNApp
from ndn.encoding import Name, NonStrictName, Component
from ndn.storage import Storage
from ndn_hydra.repo.protocol.base_models import DeleteCommand
from ndn_hydra.repo.utils.pubsub import PubSub
from ndn_hydra.repo.group_messages.remove import RemoveMessageTlv
from ndn_hydra.repo.group_messages.message import Message, MessageTypes
from ndn_hydra.repo.main.main_loop import MainLoop
from ndn_hydra.repo.handles.protocol_handle_base import ProtocolHandle
from ndn_hydra.repo.modules.global_view import GlobalView
from ndn_hydra.repo.modules.file_remover import remove_file


class DeleteCommandHandle(ProtocolHandle):
    """
    DeleteCommandHandle processes delete command handles, and deletes corresponding data stored
    in the database.
    TODO: Add validator
    """

    def __init__(self, app: NDNApp, data_storage: Storage, pb: PubSub, config: dict,
                 main_loop: MainLoop, global_view: GlobalView):
        """
        :param app: NDNApp.
        :param data_storage: Storage.
        :param pb: PubSub.
        :param config: All config Info.
        :param main_loop: SVS interface, Group Messages.
        :param global_view: Global View.
        """
        super(DeleteCommandHandle, self).__init__(app, data_storage, pb, config)
        self.prefix = None
        self.main_loop = main_loop
        self.global_view = global_view
        self.repo_prefix = config['repo_prefix']
        # self.register_root = config['repo_config']['register_root']

    async def listen(self, prefix: NonStrictName):
        """
        Register routes for command interests.
        This function needs to be called explicitly after initialization.
        :param name: NonStrictName. The name prefix to listen on.
        """
        self.prefix = prefix
        self.logger.info(f'\nInsert handle: subscribing to {Name.to_str(self.prefix) + "/delete"}')
        self.pb.subscribe(self.prefix + ['delete'], self._on_delete_msg)
        # start to announce process status
        # await self._schedule_announce_process_status(period=3)

    def _on_delete_msg(self, msg):
        try:
            cmd = DeleteCommand.parse(msg)
            # if cmd.name == None:
            #     raise DecodeError()
        except (DecodeError, IndexError) as exc:
            logging.warning('\nParameter interest decoding failed')
            return
        aio.ensure_future(self._process_delete(cmd))

    async def _process_delete(self, cmd: DeleteCommand):
        """
        Process delete command.
        """
        file_name = Name.to_str(cmd.file_name)
        self.logger.info(f"\n[CMD][DELETE]  file={file_name}")
        file = self.global_view.get_file(file_name)
        if file is None:
            self.logger.debug("\nFile does not exist")
            return

        # Delete from global view
        self.global_view.delete_file(file_name)
        # Remove from data_storage from this node if present
        if self.config['node_name'] in file['stores']:
            remove_file(self.data_storage, file, self.config)

        # remove tlv
        favor = self.global_view.get_node(self.config['node_name'])['favor']
        remove_message = RemoveMessageTlv()
        remove_message.node_name = self.config['node_name'].encode()
        remove_message.favor = str(favor).encode()
        remove_message.file_name = cmd.file_name
        # remove msg
        message = Message()
        message.type = MessageTypes.REMOVE
        message.value = remove_message.encode()

        self.main_loop.svs.publishData(message.encode())
        self.logger.info(f"\n[MSG][REMOVE]*  file={file_name}")
