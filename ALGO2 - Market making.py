# This is a python example algorithm using REST API for the RIT ALGO2 Case
import signal
import requests
import time
from time import sleep

# this class definition allows us to print error messages and stop the program when needed
class ApiException(Exception):
    pass

# this signal handler allows for a graceful shutdown when CTRL+C is pressed
def signal_handler(signum, frame):
    global shutdown
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    shutdown = True

# set your API key to authenticate to the RIT client
API_KEY = {'X-API-Key': 'EY229BG3'}
shutdown = False
# other settings for market making algo
SPREAD = 0.02
BUY_VOLUME = 3500
SELL_VOLUME = 3500  
MAX_VOLUME = 3500
TIME = 0.3

# this helper method returns the current 'tick' of the running case
def get_tick(session):
    resp = session.get('http://localhost:9999/v1/case')
    if resp.status_code == 401:
        raise ApiException('The API key provided in this Python code must match that in the RIT client (please refer to the API hyperlink in the client toolbar and/or the RIT – User Guide – REST API Documentation.pdf)')
    case = resp.json()
    return case['tick']

# this helper method returns the last close price for the given security, one tick ago
def ticker_close(session, ticker):
    payload = {'ticker': ticker, 'limit': 1}
    resp = session.get('http://localhost:9999/v1/securities/history', params=payload)
    if resp.status_code == 401:
        raise ApiException('The API key provided in this Python code must match that in the RIT client (please refer to the API hyperlink in the client toolbar and/or the RIT – User Guide – REST API Documentation.pdf)')
    ticker_history = resp.json()
    if ticker_history:
        return ticker_history[0]['close']
    else:
        raise ApiException('Response error. Unexpected JSON response.')

# this helper method submits a pair of limit orders to buy and sell VOLUME of each security, at the last price +/- SPREAD
def buy_sell(session, to_buy, to_sell, last):
    
        buy_payload = {'ticker': to_buy, 'type': 'LIMIT', 'quantity': BUY_VOLUME, 'action': 'BUY', 'price': last - SPREAD}
        sell_payload = {'ticker': to_sell, 'type': 'LIMIT', 'quantity': SELL_VOLUME, 'action': 'SELL', 'price': last + SPREAD}
        resp_b = session.post('http://localhost:9999/v1/orders', params=buy_payload)
        resp_s = session.post('http://localhost:9999/v1/orders', params=sell_payload)
        print("buy", resp_b)
        print("sell", resp_s)
        if resp_b.status_code == 429:
            sleep(0.1)
            resp_b2 = session.post('http://localhost:9999/v1/orders', params=buy_payload)
            print("buy after 1 sleep: ", resp_b2)
            if resp_b2.status_code == 429:    
                sleep(0.1)
                print("buy after 2 sleep: ", session.post('http://localhost:9999/v1/orders', params=buy_payload))
        if resp_s.status_code == 429:
            sleep(0.1)
            resp_s2 = session.post('http://localhost:9999/v1/orders', params=sell_payload)
            print("sell after 1 sleep: ", resp_s2)
            if resp_s2.status_code == 429:    
                sleep(0.1)
                print("sell after 2 sleep: ", session.post('http://localhost:9999/v1/orders', params=sell_payload))

# this helper method gets all the orders of a given type (OPEN/TRANSACTED/CANCELLED)
def get_orders(session, status):
    payload = {'status': status}
    resp = session.get('http://localhost:9999/v1/orders', params=payload)
    if resp.status_code == 401:
        raise ApiException('The API key provided in this Python code must match that in the RIT client (please refer to the API hyperlink in the client toolbar and/or the RIT – User Guide – REST API Documentation.pdf)')
    orders = resp.json()
    return orders

# get the ticker bid ask spread
def ticker_bid_ask(session, ticker):
    payload = {'ticker': ticker}
    resp = session.get('http://localhost:9999/v1/securities/book', params=payload)
    if resp.ok:
        book = resp.json()
        return book['bids'][0]['price'], book['asks'][0]['price']
    raise ApiException('The API key provided in this Python code must match that in the RIT client (please refer to the API hyperlink in the client toolbar and/or the RIT – User Guide – REST API Documentation.pdf)')

def re_order(session, num_of_orders, ids, price, quantity, quantity_filled, action):
    for i in range(num_of_orders):
        order_id = ids[i]
        volume = quantity[i]
        volume_filled = quantity_filled[i]
        # partially filled
        if(volume_filled != 0):
            volume = MAX_VOLUME - volume_filled
        
        deleted = session.delete('http://localhost:9999/v1/orders/{}'.format(order_id))
        payload = {'ticker': 'ALGO', 'type': 'LIMIT', 'quantity': volume, 'action': action, 'price': price}
        if(deleted.ok):
            resp = session.post('http://localhost:9999/v1/orders', params=payload)
            if resp.status_code == 429:
                sleep(0.1)
                resp2 = session.post('http://localhost:9999/v1/orders', params=payload)
                print("re_order after sleep", resp2)
                if resp2.status_code == 429:    
                    sleep(0.1)
                    print("re_order after 2 sleep: ", session.post('http://localhost:9999/v1/orders', params=payload))
# this is the main method containing the actual market making strategy logic
def main():
    # creates a session to manage connections and requests to the RIT Client
    with requests.Session() as s:
        # add the API key to the session to authenticate during requests
        s.headers.update(API_KEY)
        # get the current time of the case
        tick = get_tick(s)
        # while the time is between 5 and 295, do the following
        while tick > 0 and tick < 300:
            # get the open order book and ALGO last tick's close price
            orders = get_orders(s, 'OPEN')
            buy_orders = [d1 for d1 in orders if d1['action'] == 'BUY']
            sell_orders = [d2 for d2 in orders if d2['action'] == 'SELL']
            algo_close = ticker_close(s, 'ALGO')
            buy_price = algo_close - SPREAD
            sell_price = algo_close + SPREAD

            # check if you have 0 open orders
            if len(orders) == 0:
                # submit a pair of orders and update your order book
                for i in range(3):
                    buy_sell(s, 'ALGO', 'ALGO', algo_close)
                sleep(TIME)
                for i in range(2):
                    buy_sell(s, 'ALGO', 'ALGO', algo_close)
                orders = get_orders(s, 'OPEN')
                sleep(0.2)
                start = time.time()
            
            bid_price, ask_price = ticker_bid_ask(s, 'ALGO')
            # check if you don't have a pair of open orders
            # sell orders left in book
            if len(buy_orders) == 0 and len(sell_orders) != 0:
                # sell_orders ids
                sell_ids = []
                sell_quantity = []
                sell_quantity_filled = []
                for sell_order in sell_orders:
                    sell_ids.append(sell_order['order_id'])
                    sell_quantity.append(sell_order['quantity'])
                    sell_quantity_filled.append(sell_order['quantity_filled'])
                    next_sell_price = sell_order['price']
                # 21 20 19
                while (next_sell_price > bid_price):
                    # if ask_price > buy_price:
                    #     next_sell_price = ask_price
                    #     break
                    next_sell_price = next_sell_price - SPREAD / 2
                    re_order(s, len(sell_ids), sell_ids, next_sell_price, sell_quantity, sell_quantity_filled, 'SELL')
                    sleep(TIME)
                re_order(s, len(sell_ids), sell_ids, next_sell_price, sell_quantity, sell_quantity_filled, 'SELL')
                sleep(TIME)
            
            # buy orders left in book
            elif len(buy_orders) != 0 and len(sell_orders) == 0:
                # buy_orders ids
                buy_ids = []
                buy_quantity = []
                buy_quantity_filled = []
                for buy_order in buy_orders:
                    buy_ids.append(buy_order['order_id'])
                    buy_quantity.append(buy_order['quantity'])
                    buy_quantity_filled.append(buy_order['quantity_filled'])
                    next_buy_price = buy_order['price']
                # 19 20 21
                while (next_buy_price < ask_price):
                    # if bid_price < sell_price:
                    #     next_buy_price = bid_price
                    #     break
                    next_buy_price = next_buy_price + SPREAD / 2
                    re_order(s, len(buy_ids), buy_ids, next_buy_price, buy_quantity, buy_quantity_filled, 'BUY')
                    sleep(TIME)
                re_order(s, len(buy_ids), buy_ids, next_buy_price, buy_quantity, buy_quantity_filled, 'BUY')   
                sleep(TIME)
            else:
                current = time.time()
                stuck_time = current - start
                if stuck_time > 15:
                    s.post('http://localhost:9999/v1/commands/cancel?all=1') 
                    sleep(1)              
            # refresh the case time. THIS IS IMPORTANT FOR THE WHILE LOOP
            tick = get_tick(s)

# this calls the main() method when you type 'python algo2.py' into the command prompt
if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    main()
