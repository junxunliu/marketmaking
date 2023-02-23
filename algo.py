import time
from decimal import Decimal

from dydx3.constants import ORDER_SIDE_BUY, ORDER_TYPE_LIMIT, ORDER_SIDE_SELL
from dydx3 import Client

import make_order

BID_SIZE = 1
ASK_SIZE = 1
SPREAD = 0.02
MARKET = 'FIL-USD'

bid_price_, ask_price_ = '0', '0'

dicts = {'bids': {}, 'asks': {}}

offsets = {}
offset = 0


def parse_message(msg_):
    global dicts, offsets, offset

    if msg_["type"] == "subscribed":
        for side, data in msg_['contents'].items():
            for entry in data:
                size = entry['size']
                if size > 0:
                    price = entry['price']
                    dicts[str(side)][price] = size

                    offset = entry["offset"]
                    offsets[price] = offset

    if msg_["type"] == "channel_data":
        # parse updates
        for side, data in msg_['contents'].items():
            if side == 'offset':
                offset = int(data)
                continue
            else:
                for entry in data:
                    price = entry[0]
                    amount = entry[1]

                    if price in offsets and offset <= int(offsets[price]):
                        continue

                    offsets[price] = offset
                    if amount == 0:
                        if price in dicts[side]:
                            del dicts[side][price]
                    else:
                        try:
                            dicts[side].append((price, amount))
                        except AttributeError:
                            dicts[side][price] = amount


def get_bid_ask(orderbook: dict):
    best_bid, best_ask = 0, 0
    if orderbook['type'] == 'subscribed':
        best_bid = max(orderbook['contents']['bids'], key=lambda x: x['price'])
        best_ask = min(orderbook['contents']['asks'], key=lambda x: x['price'])
    elif orderbook['type'] == 'channel_data':
        bids = orderbook['contents']['bids']
        asks = orderbook['contents']['bids']
        curr_bid, curr_ask = 0, 0
        if bids:
            curr_bid = orderbook['contents']['bids'][0][0]
        if asks:
            curr_ask = orderbook['contents']['asks'][0][0]
        best_bid = max(best_bid, curr_bid)
        best_ask = min(best_ask, curr_ask)
    print('bids:', best_bid)
    print('asks:', best_ask)
    # try:
    #     best_bid = max(dicts["bids"].keys())
    #     best_ask = min(dicts["asks"].keys())
    #     print('bid:', best_bid)
    #     print('ask:', best_ask)
    #     print('====================================')
    # except Exception as e:
    #     print(e)
    # global bid_price_, ask_price_
    # book_type = orderbook.get('type')
    # contents = orderbook.get("contents")
    # if book_type == 'subscribed':
    #     bid_price_, ask_price_ = contents.get("bids")[0].get('price'), contents.get("asks")[0].get('price')
    # elif book_type == 'channel_data':
    #     bids, asks = contents.get("bids"), contents.get("asks")
    #     if bids:
    #         bid_price_ = bids[0][0]
    #     if asks:
    #         ask_price_ = asks[0][0]
    #
    # return bid_price_, ask_price_


def get_close_price(trades: dict):
    return trades.get("contents").get('price')


class Algo:

    def __init__(self, client: Client):
        self.client = client

    def run_algo(self, okx, dydx_book, dydx_trades, dydx_accounts=None):
        maker = make_order.MakeMarket(self.client)

        close_price = dydx_trades.get('price')
        # buy_price = close_price - SPREAD
        # sell_price = close_price + SPREAD
        get_bid_ask(dydx_book)
        # bid_price, ask_price = get_bid_ask(dydx_book)
        # print("bid: ", bid_price, "ask: ", ask_price)

        # payload = {'market': MARKET,
        #            'order_type': ORDER_TYPE_LIMIT,
        #            'post_only': False,
        #            'size': str(BID_SIZE),
        #            'limit_fee': '0.015',
        #            'expiration_epoch_seconds': time.time() + 1800}
        #
        # # check if it has 0 open orders
        # if not orders:
        #     # buy and sell
        #     maker.post_order(payload, ORDER_SIDE_BUY, buy_price)
        #     maker.post_order(payload, ORDER_SIDE_SELL, sell_price)
        #     orders = self.get_orders()
        #
        # # check if you don't have a pair of open orders
        # # sell orders left in book
        # buy_orders = self.get_orders(ORDER_SIDE_BUY)
        # sell_orders = self.get_orders(ORDER_SIDE_SELL)
        # if not buy_orders and sell_orders:
        #     last_sell_price = sell_orders[0].get('price')
        #     while last_sell_price > bid_price:
        #         pass
