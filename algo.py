import time

from dydx3.constants import ORDER_SIDE_BUY, ORDER_TYPE_LIMIT, ORDER_SIDE_SELL

import main
import make_order

BID_SIZE = 1
ASK_SIZE = 1
SPREAD = 0.02
MARKET = 'FIL-USD'


class Algo:
    from dydx3 import Client

    def __init__(self, client: Client, result: list):
        self.client = client
        self.res = []

    def get_account(self):
        return self.client.private.get_account().data.get('account')

    def get_orders(self, side=None):
        if side == ORDER_SIDE_BUY:
            return self.client.private.get_active_orders(MARKET, ORDER_SIDE_BUY).data.get('orders')
        elif side == ORDER_SIDE_SELL:
            return self.client.private.get_active_orders(MARKET, ORDER_SIDE_SELL).data.get('orders')
        return self.client.private.get_active_orders(MARKET).data.get('orders')

    def run_algo(self):
        maker = make_order.MakeMarket(self.client)
        account = self.get_account()
        orders = self.get_orders()

        close_price = self.res[2]
        order_book = self.res[1].get('contents')
        buy_price = close_price - SPREAD
        sell_price = close_price + SPREAD

        bids_book = order_book.get('bids')
        asks_book = order_book.get('asks')
        bid_price = bids_book[0]
        ask_price = asks_book[0]

        payload = {'market': MARKET,
                   'order_type': ORDER_TYPE_LIMIT,
                   'post_only': False,
                   'size': str(BID_SIZE),
                   'limit_fee': '0.015',
                   'expiration_epoch_seconds': time.time() + 1800}

        # check if it has 0 open orders
        if not orders:
            # buy and sell
            maker.post_order(payload, ORDER_SIDE_BUY, buy_price)
            maker.post_order(payload, ORDER_SIDE_SELL, sell_price)
            orders = self.get_orders()

        # check if you don't have a pair of open orders
        # sell orders left in book
        buy_orders = self.get_orders(ORDER_SIDE_BUY)
        sell_orders = self.get_orders(ORDER_SIDE_SELL)
        if not buy_orders and sell_orders:
            last_sell_price = sell_orders[0].get('price')
            while last_sell_price > bid_price:
                pass
