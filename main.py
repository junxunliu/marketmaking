import json
import os
import signal
import time

import websocket
from dydx3 import Client
from dydx3.constants import *


# this class definition allows us to print error messages and stop the program when needed
class ApiException(Exception):
    pass


# this signal handler allows for a graceful shutdown when CTRL+C is pressed
def signal_handler(signum, frame):
    global shutdown
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    shutdown = True


ETHEREUM_ADDRESS = os.environ['ETH_ADDRESS']
API_KEYS = {"key": os.environ['API_KEY'],
            "secret": os.environ['API_secret'],
            "passphrase": os.environ['API_passphrase'],
            "legacySigning": os.environ['API_legacySigning'],
            "walletType": os.environ['API_walletType']}
STARK_PRIVATE_KEY = os.environ['STARK_PRIVATE_KEY']
shutdown = False
# other settings for market making algo
SPREAD = 0.02
BUY_VOLUME = 3500
SELL_VOLUME = 3500
MAX_VOLUME = 3500
TIME = 0.3

# Define the dYdX API endpoint and authentication headers
base_url = "https://api.stage.dydx.exchange"
websocket_endpoint = "wss://api.dydx.exchange/v3/ws"

MID_PRICE = 30
BID_SIZE = 1
ASK_SIZE = 1
market_id = 'FIL-USD'


def on_open(ws):
    print("Connected to dYdX WebSocket API")
    ws.send(json.dumps({
        "type": "subscribe",
        "channel": "v3_orderbook",
        "id": "ETH-USD"
    }))


last_bid_price, last_ask_price = 0, 0


def on_message(ws, message):
    msg = json.loads(message)
    contents = msg.get("contents")
    bid_price_size, ask_price_size = contents.get("bids"), contents.get("asks")
    global last_bid_price, last_ask_price
    if bid_price_size:
        bid_price = bid_price_size[0][0]
        last_bid_price = bid_price
    else:
        bid_price = last_bid_price

    if ask_price_size:
        ask_price = ask_price_size[0][0]
        last_ask_price = ask_price
    else:
        ask_price = last_ask_price
    print("Bids: ", bid_price, ", Asks: ", ask_price)


# Define the market making logic
def make_market():
    #
    # Access public API endpoints.
    #
    client = Client(
        network_id=NETWORK_ID_GOERLI,
        host=API_HOST_GOERLI,
        api_key_credentials=API_KEYS,
        stark_private_key=STARK_PRIVATE_KEY,
        default_ethereum_address=ETHEREUM_ADDRESS
    )

    all_orders = client.private.get_orders(
        market=MARKET_FIL_USD,
        status=ORDER_STATUS_OPEN,
        side=ORDER_SIDE_SELL,
        # type=ORDER_TYPE_LIMIT
    )

    all_fills = client.private.get_fills(
        market=MARKET_FIL_USD,
    )

    account = client.private.get_account()

    position_id = account.data.get('account').get('positionId')
    print(position_id)
    payload = {'market': market_id,
               'side': ORDER_SIDE_BUY,
               'order_type': ORDER_TYPE_LIMIT,
               'post_only': False,
               'size': str(BID_SIZE),
               'price': '4.5',
               'limit_fee': '0.015',
               'expiration_epoch_seconds': time.time() + 1800}

    make_order(client, position_id, payload)


def make_order(client, position_id, payload):
    request = client.private.create_order(position_id, **payload)
    print(request.data.get('order').get('id'))


def on_error(ws, error):
    print("WebSocket error:", error)


def on_close(ws):
    print("Connection to dYdX WebSocket API closed")


def main():
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(websocket_endpoint,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    # ws.run_forever()
    make_market()

    # print(API_KEYS)


if __name__ == "__main__":
    main()
