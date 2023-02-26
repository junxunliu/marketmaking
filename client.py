import asyncio
import os
import sys

from dydx3 import Client
from dydx3.constants import *
from dydx3.helpers.request_helpers import generate_now_iso

import config

ETHEREUM_ADDRESS = os.environ['ETH_ADDRESS']
API_KEYS = {"key": os.environ['API_KEY'],
            "secret": os.environ['API_secret'],
            "passphrase": os.environ['API_passphrase'],
            "legacySigning": os.environ['API_legacySigning'],
            "walletType": os.environ['API_walletType']}
STARK_PRIVATE_KEY = os.environ['STARK_PRIVATE_KEY']

client = Client(
    network_id=NETWORK_ID_GOERLI,
    host=API_HOST_GOERLI,
    api_key_credentials=API_KEYS,
    stark_private_key=STARK_PRIVATE_KEY,
    default_ethereum_address=ETHEREUM_ADDRESS
)

account = client.private.get_account().data.get('account')
position_id = account.get('positionId')


def create_limit_order(token, side, sz, px, cancel_id=None):
    placed_order = client.private.create_order(
        position_id=position_id,
        market=token + '-USD',
        side=side,
        order_type=ORDER_TYPE_LIMIT,
        post_only=True,
        size=str(sz),
        price=str(px),
        limit_fee='0.0015',
        expiration_epoch_seconds=config.expire_time,
        time_in_force=TIME_IN_FORCE_GTT,
        cancel_id=str(cancel_id)
    )
    return placed_order.data['order']


# client.private.cancel_order('5cd34f43e69a1012bf8ee3570d74e859205c5e3dd6af2b698b07e677685f2df')
print(create_limit_order(config.token, ORDER_SIDE_BUY, config.size, '2',
                         '4fd6aa95e0f3f376f12fc64653c43265e50145527e902fe743efd5aec5ca895'))
