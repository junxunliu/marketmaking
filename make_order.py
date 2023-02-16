import datetime
import time
from time import sleep

BID_SIZE = 1
ASK_SIZE = 1


class MakeMarket:
    from dydx3 import Client

    def __init__(self, client: Client, result: dict):
        self.client = client
        self.result = result
        self.position_id = client.private.get_account().data.get('account').get('positionId')
        self.market_id = result.get('id')

    def post_order(self, order_side, order_type, price, limit_fee, expire, post=False, count=1):
        """
        Post orders
        :param count: number of order want to be posted, default 1
        :param order_side: ORDER_SIDE_BUY or ORDER_SIDE_BUY
        :param order_type: ORDER_TYPE_LIMIT or ORDER_TYPE_MARKET
        :param price: str
        :param limit_fee: str
        :param expire: addition expire time from today, i.e. 1h == 3600
        :param post: if this order is post only, default False
        :return: None
        """

        payload = {'position_id': self.position_id,
                   'market': self.market_id,
                   'side': order_side,
                   'order_type': order_type,
                   'post_only': post,
                   'size': str(BID_SIZE),
                   'price': price,
                   'limit_fee': limit_fee,
                   'expiration_epoch_seconds': time.time() + expire}
        for i in range(count):
            self.client.private.create_order(**payload)
            sleep(0.1)

