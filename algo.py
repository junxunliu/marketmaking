from time import time

from dydx3 import Client
from dydx3.constants import ORDER_SIDE_BUY, ORDER_SIDE_SELL, ORDER_TYPE_LIMIT, TIME_IN_FORCE_GTT

import config
from money_printer import Mediator
import datetime as dt

prev_bid, prev_ask = 0.00001, 100000


class Algo(Mediator):
    def __init__(self, client: Client):
        super(Algo, self).__init__(client=client)
        self.local_offset = 0
        self.token = ''
        self.time = 0
        self.pair = False

        self.bid_d = 0
        self.ask_d = 0
        self.bid_o = 0
        self.ask_o = 0
        self.close_price = 0

        self.time_update = ''

        self.bids_orderbook = []
        self.asks_orderbook = []

        self.open_orders = {'buys': [], 'sells': []}
        self.filled_orders = {'buys': [], 'sells': []}

    def build_orderbook(self, res):
        bids, asks = res['contents']['bids'], res['contents']['asks']
        for b, a in zip(bids, asks):
            b_price, b_size, b_offset = b['price'], b['size'], b['offset']
            a_price, a_size, a_offset = a['price'], a['size'], a['offset']
            if float(b_size) != 0:
                temp = {b_price: 'bid_price', b_size: 'bid_size'}
                self.bids_orderbook.append(temp)
            if float(a_size) != 0:
                temp = {a_price: 'ask_price', a_size: 'ask_size'}
                self.asks_orderbook.append(temp)
            # get latest offset
            self.local_offset = max(self.local_offset, float(b_offset), float(a_offset))

        self.bid_d = list(self.bids_orderbook[0].keys())[0]
        self.ask_d = list(self.asks_orderbook[0].keys())[0]

    def maintain_orderbook(self, res):
        orderbook = res['contents']
        offset = orderbook['offset']
        if float(offset) > self.local_offset:
            if orderbook['bids']:
                for bid in orderbook['bids']:
                    b_price, b_size = bid[0], bid[1]
                    # remove canceled orders if exist in orderbook
                    if float(b_size) == 0:
                        self.bids_orderbook = [d for d in self.bids_orderbook if b_price not in d]
                    else:
                        if any(b_price in i for i in self.bids_orderbook):
                            for b in self.bids_orderbook:
                                if b_price in b:
                                    old_size = list(b.keys())[1]
                                    b[b_size] = b.pop(old_size)
                        else:
                            for i, d in enumerate(self.bids_orderbook):
                                price = list(d.keys())[0]
                                if price < b_price:
                                    self.bids_orderbook.insert(i, {b_price: 'bid_price', b_size: 'bid_size'})
                                    break
            if orderbook['asks']:
                for ask in orderbook['asks']:
                    a_price, a_size = ask[0], ask[1]
                    # remove canceled orders if exist in orderbook
                    if float(a_size) == 0:
                        self.asks_orderbook = [d for d in self.asks_orderbook if a_price not in d]
                    else:
                        if any(a_price in j for j in self.asks_orderbook):
                            for a in self.asks_orderbook:
                                if a_price in a:
                                    old_size = list(a.keys())[1]
                                    a[a_size] = a.pop(old_size)
                        else:
                            for i, d in enumerate(self.asks_orderbook):
                                price = list(d.keys())[0]
                                if price > a_price:
                                    self.asks_orderbook.insert(i, {a_price: 'ask_price', a_size: 'ask_size'})
                                    break

            self.bid_d = list(self.bids_orderbook[0].keys())[0]
            self.ask_d = list(self.asks_orderbook[0].keys())[0]

    def distribution_relay(self, res, exg):
        if exg == 'op':
            if res['arg']['channel'] == 'bbo-tbt':
                if res['arg']['instId'][:-10] == self.token:
                    if 'data' in res:
                        okx_bids = float(res['data'][0]['bids'][0][0])
                        okx_asks = float(res['data'][0]['asks'][0][0])
                        # self.temp_risk_manager(bid=okx_bids, ask=okx_asks)
                        self.bid_o = okx_bids
                        self.ask_o = okx_asks

        if exg == 'type':
            if res['type'] == 'subscribed':
                if res['channel'] == 'v3_orderbook':
                    self.build_orderbook(res)

            elif res['type'] == 'channel_data':
                if res['channel'] == 'v3_orderbook':
                    self.maintain_orderbook(res)

                    if self.token != res['id'][:-4]:
                        self.token = res['id'][:-4]
                        print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                              ' | trading token: ', self.token)

                    if self.token != '':
                        self.run_algo()

                elif res['channel'] == 'v3_accounts':
                    self.check_account(res)

    def check_account(self, res):
        orders = res['contents']['orders']
        for order in orders:
            order_id = order['id']
            # if order['status'] == 'CANCELED' or order['status'] == 'FILLED' or order['status'] == 'EXPIRED':
            if order['side'] == ORDER_SIDE_BUY:
                if order['status'] == 'FILLED':
                    self.filled_orders['buys'].append(order)
                    self.del_order('buys', order_id)
                elif order['status'] == 'CANCELED':
                    self.del_order('buys', order_id)
                elif order['status'] == 'OPEN':
                    self.open_orders['buys'].append(order)
            else:
                if order['status'] == 'FILLED':
                    self.filled_orders['sells'].append(order)
                    self.del_order('sells', order_id)
                elif order['status'] == 'CANCELED':
                    self.del_order('sells', order_id)
                elif order['status'] == 'OPEN':
                    self.time = time()
                    self.open_orders['sells'].append(order)
        # print(self.open_orders)

    def del_order(self, buys_or_sells, order_id):
        if self.open_orders[buys_or_sells]:
            self.open_orders[buys_or_sells][:] = [d for d in self.open_orders[buys_or_sells][:] if
                                                  d.get('id') != order_id]

    def buy_sell(self, payload, to_buy, to_sell):
        buy_order = self.post_order(payload, ORDER_SIDE_BUY, to_buy)
        sell_order = self.post_order(payload, ORDER_SIDE_SELL, to_sell)

    def run_algo(self):
        bid_price, ask_price, close_price = float(self.bid_d), float(self.ask_d), float(self.close_price)
        if close_price == 0:
            close_price = (bid_price + ask_price) / 2
        buy_price = round(close_price - config.spread, config.decimal)
        sell_price = round(close_price + config.spread, config.decimal)
        if bid_price != 0 and ask_price != 0:
            # print(self.open_orders)
            global prev_bid, prev_ask
            # current no orders in the book
            if not self.open_orders['buys'] and not self.open_orders['sells'] and not self.pair:
                # reset filled orders
                self.filled_orders['buys'].clear()
                self.filled_orders['sells'].clear()
                # submit a pair of orders, get list of ids for reorder
                self.buy_sell(config.payload, buy_price, sell_price)
                self.pair = True
                prev_bid, prev_ask = bid_price, ask_price
                print("prev_bid, prev_ask", prev_bid, prev_ask)
            if time() - self.time > 1:
                # buy orders still were not filled
                if self.open_orders['buys'] and not self.open_orders['sells'] and self.filled_orders['sells']:
                    to_buy = float(self.open_orders['buys'][0]['price'])
                    prev_sell_price = float(self.filled_orders['sells'][0]['price'])
                    print("to_buy:", to_buy, "prev_sell_price: ", prev_sell_price)
                    print("bid_ask: ", bid_price, ask_price)
                    print("prev_bid_ask: ", prev_bid, prev_ask)

                    order_id = self.open_orders['buys'][0]['id']

                    if bid_price < prev_sell_price:
                        print("prev_sell_price: ", prev_sell_price, "bid_price", bid_price)
                        to_buy = bid_price
                        self.post_order(config.payload, ORDER_SIDE_BUY, to_buy, cancel_id=order_id)
                        # re-post order by cancel id won't have status canceled so del order here:
                        self.del_order('buys', order_id)

                    elif to_buy < prev_ask:
                        print("to_buy: ", to_buy, "prev_ask", prev_ask)
                        to_buy = to_buy + config.spread / 2

                        self.post_order(config.payload, ORDER_SIDE_BUY, to_buy, cancel_id=order_id)
                        # re-post order by cancel id won't have status canceled so del order here:
                        self.del_order('buys', order_id)

                # sell orders still were not filled
                elif self.open_orders['sells'] and not self.open_orders['buys'] and self.filled_orders['buys']:
                    to_sell = float(self.open_orders['sells'][0]['price'])
                    prev_buy_price = float(self.filled_orders['buys'][0]['price'])

                    order_id = self.open_orders['sells'][0]['id']
                    if ask_price > prev_buy_price:
                        print("prev_buy_price: ", prev_buy_price, "ask_price", ask_price)
                        to_sell = ask_price
                        self.post_order(config.payload, ORDER_SIDE_SELL, to_sell, cancel_id=order_id)
                        # re-post order by cancel id won't have status canceled so del order here:
                        self.del_order('sells', order_id)
                    elif to_sell > prev_bid:
                        print("to_sell: ", to_sell, "prev_bid", prev_bid)
                        to_sell = to_sell - config.spread / 2
                        self.post_order(config.payload, ORDER_SIDE_SELL, to_sell, cancel_id=order_id)
                        # re-post order by cancel id won't have status canceled so del order here:
                        self.del_order('sells', order_id)
        #
        # print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f'),
        #       self.bid_d, self.ask_d)
