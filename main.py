from dydx3 import Client
from dydx3.constants import *
import crossex_engine
import asyncio
import os


print('\033[32mLqz13Th\033[0m')
ETHEREUM_ADDRESS = os.environ['ETH_ADDRESS']
API_KEYS = {"key": os.environ['API_KEY'],
            "secret": os.environ['API_secret'],
            "passphrase": os.environ['API_passphrase'],
            "legacySigning": os.environ['API_legacySigning'],
            "walletType": os.environ['API_walletType']}
STARK_PRIVATE_KEY = os.environ['STARK_PRIVATE_KEY']

client = Client(network_id=NETWORK_ID_MAINNET,
                host=API_HOST_MAINNET,
                api_key_credentials=API_KEYS,
                stark_private_key=STARK_PRIVATE_KEY,
                default_ethereum_address=ETHEREUM_ADDRESS)

if __name__ == '__main__':
    multi_ws_ex = crossex_engine.Perceiver(client=client)
    multi_ws_ex.init_connect(net=WS_HOST_MAINNET)
    asyncio.run(multi_ws_ex.multi_tasks())
