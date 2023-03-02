print('\033[32mLqz13Th\033[0m')
from dydx3 import Client
from dydx3.constants import *

import crossex_engine
import asyncio


ETHEREUM_ADDRESS = '0x1B4d52f4277936c021C3Bfe23E2D089d02CA2b5A'

API_KEYS = {"key": "a5cf471e-772b-975f-9aa4-e5c02d3f4b8b",
            "secret": "8tYZROA1A-4cdpZmHI1sERVISxv2P_uoGvXQr54k",
            "passphrase": "QmmBBu0eFLdLG4S5vpSD",
            "legacySigning": False, "walletType": "METAMASK"}

STARK_PRIVATE_KEY = '06decbee13a995049937a5e60bd3eb3aeb0164ffb48d84005f0a57b506e5a8ce'

client = Client(network_id=NETWORK_ID_GOERLI,
                host=API_HOST_GOERLI,
                api_key_credentials=API_KEYS,
                stark_private_key=STARK_PRIVATE_KEY,
                default_ethereum_address=ETHEREUM_ADDRESS)


if __name__ == '__main__':
    multi_ws_ex = crossex_engine.Perceiver(client=client)
    multi_ws_ex.init_connect(net=WS_HOST_GOERLI)
    asyncio.run(multi_ws_ex.multi_tasks())


