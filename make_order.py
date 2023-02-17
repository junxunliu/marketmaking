from time import sleep

BID_SIZE = 1
ASK_SIZE = 1


# develop a method to calculate the delay time here
def time_slept() -> float:
    return 0.01


class MakeMarket:
    from dydx3 import Client

    def __init__(self, client: Client):
        self.client = client
        self.position_id = client.private.get_account().data.get('account').get('positionId')

    def post_order(self, payload, count=1) -> list:
        """
        Post orders
        :param payload: a dictionary of loading parameters
        :param count: number of order want to be posted, default 1
        :return: List of order ids
        """

        ret = []
        for i in range(count):
            order = self.client.private.create_order(self.position_id, payload).data.get('order')
            order_id = order.get('id')
            ret.append(order_id)
            sleep(time_slept())  # should develop a method to calculate the sleep time

        return ret

    def re_order(self, payload: dict, remain_size=False):
        # get list of canceled orders -> list of dict
        orders = self.cancel_active_order(payload.get('market'), payload.get('side')).data.get('cancelOrders')
        # re-post the orders
        for order in orders:
            size = order.get('remainingSize')
            if remain_size:
                payload.update({"size": str(size)})
            self.post_order(payload)

    # cancel an order
    def cancel_order_by_id(self, order_id):
        cancel_orders = self.client.private.cancel_order(order_id)
        return cancel_orders

    # cancel active orders, may specific to side and id
    def cancel_active_order(self, market, order_side=None, order_id=None):
        cancel_orders = self.client.private.cancel_active_orders(market, order_side, order_id)
        return cancel_orders

    # cancel all orders, if market is not None, cancel that market order
    def cancel_all(self, market=None):
        cancel_orders = self.client.private.cancel_all_orders(market)
        return cancel_orders
