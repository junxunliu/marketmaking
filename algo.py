from dydx3 import Client
from dydx3.constants import *
import money_printer
import datetime as dt
import config


class Algo(money_printer.Mediator):

    def __init__(self, client: Client):
        super(Algo, self).__init__(client)
        self.bid_d = 0
        self.ask_d = 0
        self.close_price = 0
        self.open_orders = []

        self.bids_orderbook = []
        self.asks_orderbook = []
        self.local_offset = 0
        self.okx_bid = 0
        self.okx_ask = 0
        self.market = ''

    def build_orderbook(self, res):
        orderbook = res['contents']
        cancel_bid = []
        cancel_ask = []
        self.bids_orderbook = orderbook['bids']
        self.asks_orderbook = orderbook['asks']
        for i in range(0, len(self.bids_orderbook)):
            self.bids_orderbook[i]['size'] = float(self.bids_orderbook[i]['size'])
            self.bids_orderbook[i]['price'] = float(self.bids_orderbook[i]['price'])
            self.bids_orderbook[i]['offset'] = float(self.bids_orderbook[i]['offset'])
            if self.bids_orderbook[i]['size'] == 0:
                now_px = self.bids_orderbook[i]['price']
                for j in range(0, len(self.asks_orderbook)):
                    if float(self.asks_orderbook[j]['price']) == now_px:
                        if float(self.asks_orderbook[j]['size']) != 0:
                            cancel_bid.append(i)
                            break

        for m in range(0, len(self.asks_orderbook)):
            self.asks_orderbook[m]['size'] = float(self.asks_orderbook[m]['size'])
            self.asks_orderbook[m]['price'] = float(self.asks_orderbook[m]['price'])
            self.asks_orderbook[m]['offset'] = float(self.asks_orderbook[m]['offset'])
            if self.asks_orderbook[m]['size'] == '0':
                now_px = self.asks_orderbook[m]['price']
                for n in range(0, len(self.bids_orderbook)):
                    if self.bids_orderbook[n]['price'] == now_px:
                        if self.bids_orderbook[n]['size'] != '0':
                            cancel_ask.append(m)
                            break

        if cancel_bid:
            del self.bids_orderbook[:len(cancel_bid)]

        if cancel_ask:
            del self.asks_orderbook[:len(cancel_ask)]

    def maintain_orderbook(self, res):
        orderbook = res['contents']
        if int(orderbook['offset']) > self.local_offset:
            self.local_offset = int(orderbook['offset'])
            bid_top_insert = []
            ask_top_insert = []
            bid_mid_insert = {}
            ask_mid_insert = {}
            if orderbook['bids']:
                for i in range(0, len(orderbook['bids'])):
                    bid_px = float(orderbook['bids'][i][0])
                    bid_sz = float(orderbook['bids'][i][1])
                    elm = {'price': bid_px,
                           'offset': self.local_offset,
                           'size': bid_sz}
                    if bid_px < self.asks_orderbook[0]['price']:
                        for j in range(0, len(self.bids_orderbook)):
                            if bid_px > self.bids_orderbook[j]['price']:
                                bid_top_insert.append(elm)
                                break

                            elif bid_px == self.bids_orderbook[j]['price']:
                                self.bids_orderbook[j] = elm
                                break

                            else:
                                if j + 2 < len(self.bids_orderbook):
                                    if bid_px > self.bids_orderbook[j + 1]['price']:
                                        bid_mid_insert[j + 1] = elm
                                        break

                    else:
                        for k in range(0, len(self.asks_orderbook)):
                            if self.asks_orderbook[k]['price'] == bid_px:
                                del self.asks_orderbook[:k + 1]
                                break

                if bid_mid_insert != {}:
                    for i in bid_mid_insert.keys():
                        self.bids_orderbook.insert(i, bid_mid_insert[i])

                if bid_top_insert:
                    bid_top_insert.reverse()
                    for i in range(0, len(bid_top_insert)):
                        self.bids_orderbook.insert(0, bid_top_insert[i])

            if orderbook['asks']:
                for i in range(0, len(orderbook['asks'])):
                    ask_px = float(orderbook['asks'][i][0])
                    ask_sz = float(orderbook['asks'][i][1])
                    elm = {'price': ask_px,
                           'offset': self.local_offset,
                           'size': ask_sz}
                    if ask_px > self.bids_orderbook[0]['price']:
                        for j in range(0, len(self.asks_orderbook)):
                            if ask_px < self.asks_orderbook[j]['price']:
                                ask_top_insert.append(elm)
                                break

                            elif ask_px == self.asks_orderbook[j]['price']:
                                self.asks_orderbook[j] = elm
                                break

                            else:
                                if j + 2 < len(self.asks_orderbook):
                                    if ask_px < self.asks_orderbook[j + 1]['price']:
                                        ask_mid_insert[j + 1] = elm
                                        break

                    else:
                        for k in range(0, len(self.bids_orderbook)):
                            if self.bids_orderbook[k]['price'] == ask_px:
                                del self.bids_orderbook[:k + 1]
                                break

                if ask_mid_insert != {}:
                    for i in ask_mid_insert.keys():
                        self.asks_orderbook.insert(i, ask_mid_insert[i])

                if ask_top_insert:
                    ask_top_insert.reverse()
                    for i in range(0, len(ask_top_insert)):
                        self.asks_orderbook.insert(0, ask_top_insert[i])

    def distribution_relay(self, res, exg):
        if exg == 'op':
            if res['arg']['channel'] == 'bbo-tbt':
                if res['arg']['instId'][:-10] == self.market:
                    if 'data' in res:
                        okx_bids = res['data'][0]['bids']
                        okx_asks = res['data'][0]['asks']
                        self.okx_bid = float(okx_bids[0][0])
                        self.okx_ask = float(okx_asks[0][0])

        if exg == 'type':
            if res['type'] == 'subscribed':
                if res['channel'] == 'v3_orderbook':
                    self.build_orderbook(res)

            if res['type'] == 'channel_data':
                if res['channel'] == 'v3_orderbook':
                    self.maintain_orderbook(res)
                    self.bid_d = self.bids_orderbook[0]['price']
                    self.ask_d = self.asks_orderbook[0]['price']
                    if self.market != res['id'][:-4]:
                        self.market = res['id'][:-4]
                        print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                              ' | trading token: ', self.market)
                # trade book
                elif res['channel'] == 'v3_trades':
                    self.close_price = res['contents']['trades'][0]['price']
                    # account book
                elif res['channel'] == 'v3_accounts':
                    print(res)
                    orders = res['contents']['orders']
                    for order in orders:
                        if order['status'] == 'OPEN':
                            self.open_orders.append(order)
                        if order['status'] == 'CANCELED' or order['status'] == 'FILLED':
                            self.open_orders[:] = [d for d in self.open_orders[:] if d.get('id') != order['id']]

    def run_algo(self):
        bid_price, ask_price, close_price = self.bid_d, self.ask_d, self.close_price
        if close_price == 0:
            close_price = (float(bid_price) + float(ask_price)) / 2
        buy_price = str(round(float(close_price) - config.spread, 2))
        sell_price = str(round(float(close_price) + config.spread, 2))
        # current no orders in the book
        if len(self.open_orders) == 0 and bid_price != 0 and ask_price != 0:
            print(self.open_orders)
            payload = {
                'market': config.token + '-USD',
                'order_type': config.order_type,
                'post_only': False,
                'size': config.size,
                'limit_fee': config.limit_fee,
                'expiration_epoch_seconds': config.expire_time,
            }
            # submit a pair of orders, get list of ids for reorder
            # buy_order = self.post_order(payload, ORDER_SIDE_BUY, buy_price)
            # sell_order = self.post_order(payload, ORDER_SIDE_SELL, sell_price)
        print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f'),
              self.okx_bid, self.okx_ask, bid_price, ask_price, close_price)
