from dydx3 import Client
from dydx3.constants import *
from dydx3.helpers.request_helpers import generate_now_iso

import datetime as dt

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
            'type': type,
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

    def create_limit_order(self, token, side, sz, px):
        placed_order = self.client.private.create_order(
            position_id=self.position_id,
            market=token + '-USD',
            side=side,
            order_type=ORDER_TYPE_LIMIT,
            post_only=False,
            size=sz,
            price=px,
            limit_fee='0.0015',
            expiration_epoch_seconds=600,
            time_in_force=TIME_IN_FORCE_GTT,
        )

    def cancel_all_order(self, token):
        clean_order = self.client.private.cancel_all_orders(market=token + 'USD')


class M_making(Mid_relay):
    def __init__(self, client: Client):
        super(M_making, self).__init__(client)
        self.bid_d = 0
        self.ask_d = 0
        self.bid_o = 0
        self.ask_o = 0

    def parse_message(self, res, exg, dydx_bids, dydx_asks, okx_bid, okx_ask):
        if exg == 'op':
            if res['arg']['channel'] == 'bbo-tbt':
                self.bid_o = okx_bid
                self.ask_o = okx_ask

        if exg == 'type':
            if res['type'] == 'channel_data':
                if res['channel'] == 'v3_orderbook':
                    self.bid_d = dydx_bids[0]['price']
                    self.ask_d = dydx_asks[0]['price']

        print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f'), self.bid_o, self.ask_o, self.bid_d, self.ask_d)