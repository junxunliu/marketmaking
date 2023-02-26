from time import time
from dydx3.constants import ORDER_TYPE_LIMIT

spread = 2.0
token = 'ETH'
decimal = 1
size = '0.1'
limit_fee = '0.015'
expire_time = time() + 1200
order_type = ORDER_TYPE_LIMIT

time = 1

payload = {
    'market': token + '-USD',
    'order_type': order_type,
    'post_only': False,
    'size': size,
    'limit_fee': limit_fee,
    'expiration_epoch_seconds': expire_time
}
