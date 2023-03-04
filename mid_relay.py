from dydx3 import Client
from dydx3.constants import *
from dydx3.helpers.request_helpers import generate_now_iso

import time
from math import ceil
from numpy import clip


class Mediator:
    def __init__(self, client: Client):
        self.client = client
        self.position_id = client.private.get_account().data['account']['positionId']
        self.ord_config = self.order_post_point_config()

    @staticmethod
    def sub_or_unsub(method):
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
            'id': token,
            'includeOffsets': True
        }
        return req

    def connect_orderbook_okx(self, type, tokens):
        channels = []
        if tokens:
            for token in tokens:
                channels.append({
                    'channel': 'bbo-tbt',
                    'instId': token + 'T-SWAP'
                })
        okx_req = {
            'op': self.sub_or_unsub(method=type),
            'args': channels
        }
        return okx_req

    def order_post_point_config(self):
        order_config = self.client.public.get_config()
        p_ord_rate = order_config.data['placeOrderRateLimiting']
        return p_ord_rate

    def ord_pt_consped(self, o_n):
        t_n = self.ord_config['targetNotional']
        min_lord = self.ord_config['minLimitConsumption']
        max_ord = self.ord_config['maxOrderConsumption']
        ord_nation = ceil(t_n / o_n)
        ord_point = clip(ord_nation, min_lord, max_ord)
        return ord_point

    def create_limit_order(self, token, side, sz, px, tm=61, ccl_id=1):
        placed_order = self.client.private.create_order(
            position_id=self.position_id,
            market=token,
            side=side,
            order_type=ORDER_TYPE_LIMIT,
            post_only=True,
            size=str(sz),
            price=str(px),
            limit_fee='0.0015',
            expiration_epoch_seconds=time.time() + tm,
            time_in_force=TIME_IN_FORCE_GTT,
            cancel_id=str(ccl_id)
        )
        return placed_order.data['order']['id']

    def cancel_order(self, token, side, id):
        ccl_order = self.client.private.cancel_active_orders(
            market=token,
            side=side,
            id=id
        )
        return ccl_order.data

    def cancel_all_order(self, token):
        clean_order = self.client.private.cancel_all_orders(market=token)
        return clean_order.data

    def acc_info(self):
        account_info = self.client.private.get_account()
        return account_info.data['account']

