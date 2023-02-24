from dydx3 import Client
from dydx3.helpers.request_helpers import generate_now_iso


def sub_or_unsub(method):
    if method == 1:
        return 'subscribe'

    elif method == -1:
        return 'unsubscribe'


class Mediator:
    def __init__(self, client: Client):
        self.client = client
        self.position_id = client.private.get_account().data['account']['positionId']

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

    def connect_accounts_dYdX(self, sub_type, channel):
        sig = self.signature_dYdX(path='ws', channel=channel, method='GET')
        req = {
            'type': sub_or_unsub(sub_type),
            'channel': 'v3_' + channel,
            'accountNumber': '0',
            'apiKey': self.client.api_key_credentials['key'],
            'passphrase': self.client.api_key_credentials['passphrase'],
            'timestamp': generate_now_iso(),
            'signature': sig,
        }
        return req

    @staticmethod
    def connect_dYdX(sub_type, channel, token=None, includeOffsets=False):
        req = {
            'type': sub_or_unsub(sub_type),
            'channel': 'v3_' + channel,
        }
        if token:
            req.update({'id': token + '-USD'})
        if includeOffsets:
            req.update({'includeOffsets': True})
        return req

    @staticmethod
    def connect_orderbook_okx(sub_type, token):
        okx_req = {
            'op': sub_or_unsub(sub_type),
            'args': [{'channel': 'bbo-tbt',
                      'instId': token + '-USDT-SWAP'}]
        }
        return okx_req

    def post_order(self, payload, side, price, count=1) -> list:
        """
        Post orders
        :param price: price of this order
        :param side: buy or sell
        :param payload: a dictionary of loading parameters
        :param count: number of order want to be posted, default 1
        :return: List of order ids
        """
        payload.update({"position_id": self.position_id})
        payload.update({"side": side})
        payload.update({"price": price})

        # order_ids = []
        # for i in range(count):
        order = self.client.private.create_order(**payload).data.get('order')
        # order_id = order.get('id')
        # order_ids.append(order_id)

        return order

    def re_order(self, order_ids, payload, price, new_size=False, size=None):
        order_ids_ = []
        # repost the order by id to cancel previous order
        for order_id in order_ids:
            if new_size:
                payload.update({"size": str(size)})
            payload.update({"price": str(price)})
            order = self.client.private.create_order(
                self.position_id, cancel_id=order_id, **payload).data.get('order')
            order_id_ = order.get('id')
            order_ids_.append(order_id_)
        return order_ids_

    # cancel an order
    def cancel_order_by_id(self, order_id):
        cancel_orders = self.client.private.cancel_order(order_id)
        return cancel_orders

    # cancel active orders, may specific to side and id
    def cancel_active_order(self, token, order_side=None, order_id=None):
        cancel_orders = self.client.private.cancel_active_orders(token + '-USD', order_side, order_id)
        return cancel_orders

    def cancel_all_order(self, token):
        canceled_orders = self.client.private.cancel_all_orders(market=token + '-USD')
        return canceled_orders
