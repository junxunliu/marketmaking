import asyncio
import os
import sys

from dydx3 import Client
from dydx3.constants import *
from dydx3.helpers.request_helpers import generate_now_iso

import config
import crossex_engine


# this class definition allows us to print error messages and stop the program when needed
class ApiException(Exception):
    pass


# this signal handler allows for a graceful shutdown when CTRL+C is pressed
def signal_handler(sig, frame):
    print('You pressed Ctrl+C!')
    sys.exit(0)


shutdown = False

ETHEREUM_ADDRESS = os.environ['ETH_ADDRESS']
API_KEYS = {"key": os.environ['API_KEY'],
            "secret": os.environ['API_secret'],
            "passphrase": os.environ['API_passphrase'],
            "legacySigning": os.environ['API_legacySigning'],
            "walletType": os.environ['API_walletType']}
STARK_PRIVATE_KEY = os.environ['STARK_PRIVATE_KEY']
# other settings for market making algo
SPREAD = 0.02
BUY_VOLUME = 3500
SELL_VOLUME = 3500
MAX_VOLUME = 3500
TIME = 0.3

# Define the dYdX API endpoint and authentication headers
base_url = "https://api.stage.dydx.exchange"

MID_PRICE = 30
BID_SIZE = 1
ASK_SIZE = 1
market_id = 'FIL-USD'

client = Client(
    network_id=NETWORK_ID_GOERLI,
    host=API_HOST_GOERLI,
    api_key_credentials=API_KEYS,
    stark_private_key=STARK_PRIVATE_KEY,
    default_ethereum_address=ETHEREUM_ADDRESS
)

account = client.private.get_account().data.get('account')

now_iso_string = generate_now_iso()
signature = client.private.sign(
    request_path='/ws/accounts',
    method='GET',
    iso_timestamp=now_iso_string,
    data={},
)

account_channel = {
    'type': 'subscribe',
    'channel': 'v3_accounts',
    'accountNumber': '0',
    'apiKey': client.api_key_credentials['key'],
    'passphrase': client.api_key_credentials['passphrase'],
    'timestamp': now_iso_string,
    'signature': signature,
}

orderbook_channel = {
    "type": "subscribe",
    "channel": "v3_orderbook",
    "id": market_id,
    "includeOffsets": "True"
}

trades_channel = {
    "type": "subscribe",
    "channel": "v3_trades",
    "id": market_id
}

markets_channel = {
    "type": "subscribe",
    "channel": "v3_markets",
    "id": market_id
}

okx_channel = {
    "op": "subscribe",
    "args": [{"channel": "bbo-tbt", "instId": "DOGE-USDT-SWAP"}]
}

# payload = {'market': market_id,
#            'position_id': position_id,
#            'order_type': ORDER_TYPE_LIMIT,
#            'side': ORDER_SIDE_BUY,
#            'post_only': False,
#            'size': str(BID_SIZE),
#            'price': '3.5',
#            'limit_fee': '0.015',
#            'expiration_epoch_seconds': time.time() + 1800,
#            }

if __name__ == "__main__":
    multi_ws_ex = crossex_engine.Perceiver(client=client)
    while True:
        multi_ws_ex.init_connect(token=config.token, net=WS_HOST_GOERLI)
        asyncio.run(multi_ws_ex.multi_tasks())
