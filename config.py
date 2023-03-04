





# trading_tokens = ['ETH-USD', 'FIL-USD', 'SOL-USD']
trading_tokens = ['SOL-USD']

commissions = 0.0002

order_existing_time = 900
order_numbers = 8

def dYdX_token_config(token):
    if token == 'ETH-USD':
        token_info = {
            'minimal_decimal': 1,
            'minimal_size': 0.01,
            'size_decimal': 2
        }
        return token_info

    if token == 'FIL-USD':
        token_info = {
            'minimal_decimal': 2,
            'minimal_size': 1,
            'size_decimal': 0
        }
        return token_info

    if token == 'SOL-USD':
        token_info = {
            'minimal_decimal': 3,
            'minimal_size': 1,
            'size_decimal': 0
        }
        return token_info

