from dydx3 import Client
from dydx3.constants import *
from dydx3.helpers.request_helpers import generate_now_iso

import datetime as dt
import time


class Mid_relay:
    def __init__(self, client: Client):
        self.client = client
        self.position_id = client.private.get_account().data['account']['positionId']

    def sub_or_unsub(self, method):
        if method == 1:
            return 'subscribe'

        elif method == -1:
            return 'unsubscribe'

    def signature_dYdX(self, path, channel, method):
        request_path = '/' + path + '/' + channel
        now_iso_string = generate_now_iso()
        signature = self.client.private.sign(
            request_path=request_path,
            method=method,
            iso_timestamp=now_iso_string,
            data={},
        )
        return signature

    def connect_accounts_dYdX(self, type, channel):
        sig = self.signature_dYdX(path='ws', channel=channel, method='GET')
        req = {
            'type': self.sub_or_unsub(method=type),
            'channel': 'v3_' + channel,
            'accountNumber': '0',
            'apiKey': self.client.api_key_credentials['key'],
            'passphrase': self.client.api_key_credentials['passphrase'],
            'timestamp': generate_now_iso(),
            'signature': sig,
        }
        return req

    def connect_orderbook_dYdX(self, type, channel, token):
        req = {
            'type': self.sub_or_unsub(method=type),
            'channel': 'v3_' + channel,
            'id': token + '-USD',
            'includeOffsets': True
        }
        return req

    def connect_orderbook_okx(self, type, token):
        okx_req = {
            'op': self.sub_or_unsub(method=type),
            'args': [{'channel': 'bbo-tbt',
                      'instId': token + '-USDT-SWAP'}]
        }
        return okx_req

    def create_limit_order(self, token, side, sz, px, cancel_id=1):
        placed_order = self.client.private.create_order(
            position_id=self.position_id,
            market=token + '-USD',
            side=side,
            order_type=ORDER_TYPE_LIMIT,
            post_only=True,
            size=str(sz),
            price=str(px),
            limit_fee='0.0015',
            expiration_epoch_seconds=time.time() + 120,
            time_in_force=TIME_IN_FORCE_GTT,
            cancel_id = str(cancel_id)
        )
        return placed_order.data['order']['id']

    def cancel_all_order(self, token):
        clean_order = self.client.private.cancel_all_orders(market=token + '-USD')
        return clean_order.data

class M_making(Mid_relay):
    def __init__(self, client: Client):
        super(M_making, self).__init__(client=client)
        self.token = ''
        self.ticks = 0

        self.bid_d = 0
        self.ask_d = 0
        self.bid_o = 0
        self.ask_o = 0


        self.time_update = ''

        self.bids_orderbook = []
        self.asks_orderbook = []

        self.ord_ids_bid = []
        self.ord_ids_ask = []

        self.bid_fill_time = 0
        self.ask_fill_time = 0

        self.pos_side = 0
        self.pos_size = 0

    def start_making(self):
        if self.ticks > 500:
            if generate_now_iso()[:-1] > self.time_update:
                self.order_block()
                self.ticks = 0

        if self.ord_ids_bid == [] or time.time() - self.bid_fill_time > 30:
            bid_lst = []
            ask_lst = []
            for i in range(0, len(self.bids_orderbook)):
                bid_lst.append(self.bids_orderbook[i]['price'])

            for i in range(0, len(self.asks_orderbook)):
                ask_lst.append(self.asks_orderbook[i]['price'])

            print(bid_lst)
            print(ask_lst)
            print(len(self.bids_orderbook), len(self.asks_orderbook))
            print('=====================')

            ccl_id = 1
            if self.ord_ids_bid != []:
                ccl_id = self.ord_ids_bid[0]

            sz = 1
            if self.pos_side == 'SHORT' or self.pos_side == 0:
                sz = abs(self.pos_size) + 1

            bid_ids = self.create_limit_order(token=self.token,
                                                 side='BUY',
                                                 sz=int(sz),
                                                 px=round(self.ask_d - self.bid_d * 0.0005, 3),
                                                 cancel_id=ccl_id)
            self.ord_ids_bid.append(bid_ids)
            self.bid_fill_time = time.time()

        if self.ord_ids_ask == [] or time.time() - self.ask_fill_time > 30:
            bid_lst = []
            ask_lst = []
            for i in range(0, len(self.bids_orderbook)):
                bid_lst.append(self.bids_orderbook[i]['price'])

            for i in range(0, len(self.asks_orderbook)):
                ask_lst.append(self.asks_orderbook[i]['price'])

            print(bid_lst)
            print(ask_lst)
            print(len(self.bids_orderbook), len(self.asks_orderbook))
            print('=====================')

            ccl_id = 1
            if self.ord_ids_ask != []:
                ccl_id = self.ord_ids_ask[0]

            sz = 1
            if self.pos_side == 'LONG' or self.pos_side == 0:
                sz = abs(self.pos_size) + 1

            ask_ids = self.create_limit_order(token=self.token,
                                                 side='SELL',
                                                 sz=int(sz),
                                                 px=round(self.bid_d + self.ask_d * 0.0005, 3),
                                                 cancel_id=ccl_id)
            self.ord_ids_ask.append(ask_ids)
            self.ask_fill_time = time.time()

    def temp_risk_manager(self, bid, ask):
        bid_gap = abs(bid - self.bid_o)
        ask_gap = abs(self.ask_o - ask)
        if bid_gap > ask * 0.0013 or ask_gap > bid * 0.0013:
            self.cancel_all_order(token=self.token)
            self.ord_ids_bid.clear()
            self.ord_ids_ask.clear()




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

        if cancel_bid != []:
            del self.bids_orderbook[:len(cancel_bid)]

        if cancel_ask != []:
            del self.asks_orderbook[:len(cancel_ask)]

    def maintain_orderbook(self, res):
        orderbook = res['contents']
        if int(orderbook['offset']) > self.local_offset:
            self.local_offset = int(orderbook['offset'])
            bid_top_insert = []
            ask_top_insert = []
            bid_mid_insert = {}
            ask_mid_insert = {}
            bid_bot_insert = {}
            ask_bot_insert = {}
            if orderbook['bids'] != []:
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

                            if bid_px == self.bids_orderbook[j]['price']:
                                self.bids_orderbook[j] = elm
                                break

                            if bid_px < self.bids_orderbook[j]['price']:
                                if j + 2 < len(self.bids_orderbook):
                                    if bid_px > self.bids_orderbook[j+1]['price']:
                                        bid_mid_insert[j+1] = elm
                                        break

                                elif j + 1 == len(self.bids_orderbook):
                                    bid_bot_insert[j+1] = elm
                                    break

                    else:
                        for k in range(0, len(self.asks_orderbook)):
                            if self.asks_orderbook[k]['price'] == bid_px:
                                del self.asks_orderbook[:k+1]
                                break

                if bid_bot_insert != {}:
                    for i in bid_bot_insert.keys():
                        self.bids_orderbook.insert(i, bid_bot_insert[i])
                        print(bid_bot_insert, '1')


                if bid_mid_insert != {}:
                    for i in bid_mid_insert.keys():
                        self.bids_orderbook.insert(i, bid_mid_insert[i])
                        print(bid_mid_insert, '2')


                if bid_top_insert != []:
                    bid_top_insert.reverse()
                    for i in range(0, len(bid_top_insert)):
                        self.bids_orderbook.insert(0, bid_top_insert[i])
                        print(bid_top_insert, '3')

            if orderbook['asks'] != []:
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

                            if ask_px == self.asks_orderbook[j]['price']:
                                self.asks_orderbook[j] = elm
                                break

                            if ask_px > self.asks_orderbook[j]['price']:
                                if j + 2 < len(self.asks_orderbook):
                                    if ask_px < self.asks_orderbook[j+1]['price']:
                                        ask_mid_insert[j+1] = elm
                                        break

                                elif j + 1 == len(self.asks_orderbook):
                                    ask_bot_insert[j+1] = elm

                    else:
                        for k in range(0, len(self.bids_orderbook)):
                            if self.bids_orderbook[k]['price'] == ask_px:
                                del self.bids_orderbook[:k+1]
                                break

                if ask_bot_insert != []:
                    for i in ask_bot_insert.keys():
                        self.asks_orderbook.insert(i, ask_bot_insert[i])
                        print(ask_bot_insert, '4')

                if ask_mid_insert != {}:
                    for i in ask_mid_insert.keys():
                        self.asks_orderbook.insert(i, ask_mid_insert[i])
                        print(ask_mid_insert, '5')

                if ask_top_insert != []:
                    ask_top_insert.reverse()
                    for i in range(0, len(ask_top_insert)):
                        self.asks_orderbook.insert(0, ask_top_insert[i])
                        print(ask_top_insert, '6')

        self.bid_d = self.bids_orderbook[0]['price']
        self.ask_d = self.asks_orderbook[0]['price']

    def resolve_account_status(self, res):
        acc_content = res['contents']
        if 'fills' in acc_content:
            if acc_content['fills'] != []:
                for i in range(0, len(acc_content['fills'])):
                    if acc_content['fills'][i]['orderId'] in self.ord_ids_bid:
                        self.ord_ids_bid.remove(acc_content['fills'][i]['orderId'])

                    elif acc_content['fills'][i]['orderId'] in self.ord_ids_ask:
                        self.ord_ids_ask.remove(acc_content['fills'][i]['orderId'])

        if 'positions' in acc_content:
            if acc_content['positions'] != []:
                for i in range(0, len(acc_content['positions'])):
                    self.pos_side = acc_content['positions'][i]['side']
                    self.pos_size = float(acc_content['positions'][i]['size'])

        if acc_content['orders'] != []:
            for i in range(0, len(acc_content['orders'])):
                if acc_content['orders'][i]['status'] == 'CANCELED':
                    if acc_content['orders'][i]['id'] in self.ord_ids_bid:
                        self.ord_ids_bid.remove(acc_content['orders'][i]['id'])

                    elif acc_content['orders'][i]['id'] in self.ord_ids_ask:
                        self.ord_ids_ask.remove(acc_content['orders'][i]['id'])

    def distribution_relay(self, res, exg):
        if exg == 'op':
            if res['arg']['channel'] == 'bbo-tbt':
                if res['arg']['instId'][:-10] == self.token:
                    if 'data' in res:
                        okx_bids = float(res['data'][0]['bids'][0][0])
                        okx_asks = float(res['data'][0]['asks'][0][0])
                        self.temp_risk_manager(bid=okx_bids, ask=okx_asks)
                        self.bid_o = okx_bids
                        self.ask_o = okx_asks

        if exg == 'type':
            if res['type'] == 'subscribed':
                if res['channel'] == 'v3_orderbook':
                    self.build_orderbook(res=res)

            elif res['type'] == 'channel_data':
                if res['channel'] == 'v3_orderbook':
                    self.maintain_orderbook(res=res)
                    if self.token != '':
                        self.start_making()

                    if self.token != res['id'][:-4]:
                        self.token = res['id'][:-4]
                        print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                              ' | trading token: ', self.token)

                elif res['channel'] == 'v3_accounts':
                    self.resolve_account_status(res=res)
