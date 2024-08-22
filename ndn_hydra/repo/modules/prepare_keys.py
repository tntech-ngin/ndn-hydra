import errno
import os
import sys
import base64
import logging
from typing import List
from datetime import datetime, timedelta
# NDN Imports
import ndn.encoding as enc
from ndn.encoding import Name
from ndn.utils import timestamp
from ndn.security.tpm import TpmFile
import ndn.app_support.security_v2 as sv2
import ndn.app_support.light_versec.checker as chk
import ndn.app_support.light_versec.compiler as cpl
# Envelope lib
from envelope.impl import EnvelopeImpl
from envelope.impl.storage import Sqlite3Box


async def prepare_keys(group_prefix, nodeids: List, app):
    # /hydra/node/n1/hydra/group/data/1
    lvs_text = r'''
        #KEY: "KEY"/_/_/_
        #site: "hydra"
        #nodePrefix: #site/node/_
        #groupSyncInterestSend: #site/sync/_ <= #node
        #groupSyncInterestReceive: #site/sync/_/_ <= #node
        #groupData1: #nodePrefix/#site/group/_ <= #node
        #groupData2: #nodePrefix/#site/group/data/_ <= #node
        #node: #site/node/_/#KEY <= #root
        #root: #site/#KEY
    '''
    basedir = os.path.dirname(os.path.abspath(sys.argv[0]))
    sec_params_dir = os.path.join(basedir, 'secParams')
    tpm_path = os.path.join(sec_params_dir, 'RepoNodeSecurityPrivKeys')
    os.makedirs(tpm_path, exist_ok=True)

    # Cleaning up
    try:
        os.remove(sec_params_dir)
    except OSError:
        print(f'{OSError} Error removing sec_params_dir: {sec_params_dir}')

    security_manager = EnvelopeImpl(app, TpmFile(tpm_path))

    # Anchor
    anchor_key_name, anchor_key_pub = security_manager.tpm.generate_key(Name.from_str(group_prefix))
    anchor_self_signer = security_manager.tpm.get_signer(anchor_key_name, None)
    anchor_cert_name, anchor_bytes = sv2.self_sign(anchor_key_name, anchor_key_pub, anchor_self_signer)
    logging.info(enc.Name.to_str(anchor_cert_name))
    chk.DEFAULT_USER_FNS.update(
        {'$eq_any': lambda c, args: any(x == c for x in args)}
    )
    model = cpl.compile_lvs(lvs_text)
    await security_manager.set(anchor_bytes, model, chk.DEFAULT_USER_FNS)
    security_manager.index(anchor_bytes)

    with open(os.path.join(sec_params_dir, "anchor.ndncert"), "w") as af:
        af.write(base64.b64encode(anchor_bytes).decode())

    with open(os.path.join(sec_params_dir, "model.lvs"), "w") as mf:
        mf.write(base64.b64encode(model.encode()).decode())

    # node
    for node in nodeids:
        node_key_name, node_key_pub = security_manager.tpm.generate_key(
            Name.from_str(f"{group_prefix}/node/" + node))
        node_cert_name = node_key_name + [enc.Component.from_str("noc"), enc.Component.from_version(timestamp())]
        node_cert_bytes = security_manager.sign_cert(node_cert_name, enc.MetaInfo(content_type=enc.ContentType.KEY,
                                                                                  freshness_period=3600000),
                                                     node_key_pub, datetime.utcnow(),
                                                     datetime.utcnow() + timedelta(days=10))
        Sqlite3Box.initialize(os.path.join(sec_params_dir, f'RepoNodeCerts-{node}.db'))
        node_box = Sqlite3Box(os.path.join(sec_params_dir, f'RepoNodeCerts-{node}.db'))
        node_box.put(node_cert_name, node_cert_bytes)
