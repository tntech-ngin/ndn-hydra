# -------------------------------------------------------------
# NDN Hydra Query Client
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/tntech-ngin/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

import logging
from ndn.app import NDNApp
from ndn.encoding import FormalName, Component, Name, ContentType
from ndn.types import InterestNack, InterestTimeout, InterestCanceled, ValidationFailure
from ndn_hydra.repo.protocol.base_models import FileList, File


class HydraQueryClient(object):
    def __init__(self, app: NDNApp, client_prefix: FormalName, repo_prefix: FormalName) -> None:
        """
        This client queries a node within the remote repo.
        :param app: NDNApp.
        :param client_prefix: NonStrictName. Routable name to client.
        :param repo_prefix: NonStrictName. Routable name to remote repo.
        """
        self.app = app
        self.client_prefix = client_prefix
        self.repo_prefix = repo_prefix

    async def send_query(self, query: Name, node_name: str = None) -> None:
        """
        Form a certain query and request that info from a node.
        """
        if not node_name:
            named_query = self.repo_prefix + [Component.from_str("query")]
        else:
            # named_query = self.repo_prefix + [Component.from_str("node")] + [Component.from_str(node_name)] + [Component.from_str("query")]
            named_query = self.repo_prefix + [Component.from_str(node_name)] + [Component.from_str("query")]

        try:
            data_name, meta_info, content = await self.app.express_interest(named_query, Name.to_bytes(query), can_be_prefix=True, must_be_fresh=True, lifetime=3000)

            if meta_info.content_type == ContentType.NACK:
                print("Distributed Repo does not know that query.")
            else:
                querytype = Component.to_str(query[0])
                if querytype == "nodes":
                    print(f'List of All Node Names')
                    print(f'{bytes(content).decode().split()}')
                elif querytype == "exnodes":
                    print(f'List of All Expired Node Names')
                    print(f'{bytes(content).decode().split()}')
                elif querytype == "files":
                    filelist = FileList.parse(content)
                    counter = 1
                    if filelist.list:
                        print(f'\nList of All Files')
                        for file in filelist.list:
                            print(f'File {counter} meta-info')
                            print(f'\tfile_name: {Name.to_str(file.file_name)}')
                            print(f'\tsize: {file.size} | packets: {file.packets} | packet_size: {file.packet_size}')
                            counter = counter + 1
                    else:
                        print(f'No files inserted in the remote repo.')
                elif querytype == "file":
                    if content:
                        file = File.parse(content)
                        print(f'File meta-info:')
                        print(f'\tfile_name: {Name.to_str(file.file_name)}')
                        print(f'\tsize: {file.size} | packets: {file.packets} | packet_size: {file.packet_size}')
                        return Name.to_str(file.file_name)
                elif querytype == "prefix":
                    filelist = FileList.parse(content)
                    counter = 1
                    if filelist.list:
                        print(f'List of All Files with prefix {Name.to_str(query[1:])}')
                        for file in filelist.list:
                            print(f'File {counter} meta-info')
                            print(f'\tfile_name: {Name.to_str(file.file_name)}')
                            print(f'\tsize: {file.size} | packets: {file.packets} | packet_size: {file.packet_size}')
                            counter = counter + 1
                    else:
                        print(f'No files inserted in the remote repo with prefix {Name.to_str(query[1:])}.')
                else:
                    print("Client does not know that query.")
        except (InterestNack, InterestTimeout, InterestCanceled, ValidationFailure) as e:
            print("Query command received no data packet back")