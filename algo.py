import datetime as dt
import time

import numpy as np
import pandas as pd
from dydx3 import Client
from dydx3.helpers.request_helpers import generate_now_iso

import config
from mid_relay import Mediator

bid_id, ask_id = None, None
buy_price, sell_price = None, None


class Money_printer(Mediator):
    def __init__(self, client: Client):
        super(Money_printer, self).__init__(client=client)
        self.p_id = {}
        self.timer = {}
        self.re_time = None
        self.account_connection = False

        self.ord_config = self.order_post_point_config()
        self.ord_mx_consump = float(self.ord_config['maxOrderConsumption'])
        self.ord_mx_pts = float(self.ord_config['maxPoints'])
        self.ord_w_sec = float(self.ord_config['windowSec'])

        self.token_numbers = len(config.trading_tokens)
        self.tm = config.order_existing_time
        self.ord_numbs = config.order_numbers
        self.cms = config.commissions

        self.risk_on = {}
        self.ord_pts = {}
        self.pos_size = {}

        self.token = []
        self.bid_d = {}
        self.ask_d = {}
        self.bid_o = {}
        self.ask_o = {}
        self.mid_px = {}
        self.bids_orderbook = {}
        self.asks_orderbook = {}

        self.td_sz = {}
        self.sz_dec = {}
        self.min_sz = {}
        self.min_dec = {}
        self.post_priority = {}
        self.filled_orders = {}

        self.ord_df = {}

        self.ticks = {}
        self.time_update = {}

    def token_initialize(self, token):
        token_info = config.dYdX_token_config(token)
        self.min_dec[token] = token_info['minimal_decimal']
        self.min_sz[token] = token_info['minimal_size']
        self.sz_dec[token] = token_info['size_decimal']
        self.td_sz[token] = self.min_sz[token]
        self.post_priority[token] = 0
        self.ord_pts[token] = self.ord_mx_pts

        self.ord_df[token] = pd.DataFrame(columns=['id', 'sd', 'px', 'sz', 'p_id'])

        self.mid_px[token] = []
        self.bids_orderbook[token] = {'px': [], 'sz': [], 'of': []}
        self.asks_orderbook[token] = {'px': [], 'sz': [], 'of': []}

        self.risk_on[token] = 0
        self.pos_size[token] = 0

        self.time_update[token] = generate_now_iso()[:-1]
        self.ticks[token] = 9999
        self.re_time = time.time()
        self.timer[token] = time.time()

    def acc_balance_distribution(self, token):
        account = self.acc_info()
        equity = float(account['equity'])
        token_equity = equity / self.token_numbers
        maker_equity = token_equity / 4

        if 'openPositions' in account:
            pos_info = account['openPositions']
            if token in pos_info:
                self.pos_size[token] = float(pos_info[token]['size'])

        if self.td_sz[token] == self.min_sz[token]:
            total_sz = maker_equity / self.bid_d[token]
            mx_sz = max(total_sz * 0.01, self.min_sz[token])
            self.td_sz[token] = round(mx_sz, self.sz_dec[token])

    def ord_reset_pts(self, temp_time, token):
        if self.ord_pts[token] == self.ord_mx_pts:
            self.timer[token] = time.time()

        if temp_time - self.timer[token] > self.ord_w_sec:
            self.timer[token] = time.time()
            self.ord_pts[token] = self.ord_mx_pts

    def on_making(self, token):
        temp_time = time.time()
        if self.risk_on[token] != 0:
            self.risk_executioner(token)

        else:
            bid, ask = self.bid_d[token], self.ask_d[token]
            mid_px = abs(bid + ask) / 2
            self.mid_px[token].append(mid_px)
            if len(self.mid_px[token]) > 100:
                self.mid_px[token].pop(0)

            tick = pow(0.1, self.min_dec[token])
            border = self.cms * mid_px * 2 + tick
            self.post_order(mid_px, bid, ask, tick, border, token, temp_time)
            self.ord_reset_pts(temp_time, token)
            if self.ticks[token] > 1000:
                if generate_now_iso()[:-1] > self.time_update[token]:
                    self.acc_balance_distribution(token)
                    self.ticks[token] = 0

    def post_order(self, mid_px, bid, ask, tk, step, token, temp_time):
        sz_dec = self.sz_dec[token]
        balance_re = 0
        buy_px, sel_px = mid_px, mid_px
        vol = self.ticks_vol(token)
        step = step * vol
        px_dec = self.min_dec[token]
        buy_px = round((buy_px - config.sol_spread), px_dec)
        sel_px = round((sel_px + config.sol_spread), px_dec)
        # buy_px = round((buy_px - step), px_dec)
        # sel_px = round((sel_px + step), px_dec)
        cl_rg = (step + tk) * self.ord_numbs * 100
        global bid_id, ask_id, buy_price, sell_price
        b_sz = self.td_sz[token]
        s_sz = self.td_sz[token]

        ord_numbs = len(self.ord_df[token])
        if self.ord_pts[token] > self.ord_mx_consump * 3:
            if ord_numbs == 0:
                try:
                    self.log_displayer(token=token,
                                       points=self.ord_pts[token],
                                       ord_bid=buy_px,
                                       ord_ask=sel_px,
                                       ord_len=ord_numbs)

                    buy_price, bid_id = self.post_bids(b_px=buy_px,
                                                       b_sz=b_sz,
                                                       token=token)

                    sell_price, ask_id = self.post_asks(s_px=sel_px,
                                                        s_sz=s_sz,
                                                        step=step,
                                                        token=token)
                except Exception as e:
                    print(e)
            bid, ask = self.bid_d[token], self.ask_d[token]
            df_s = self.ord_df[token][self.ord_df[token]['sd'] == 'SELL']
            df_b = self.ord_df[token][self.ord_df[token]['sd'] == 'BUY']
            sleep = temp_time - self.re_time
            # buy order not filled
            if len(df_s) == 0 and len(df_b) != 0 and sleep > 1:
                if sell_price > buy_price:
                    if bid < sell_price - self.cms:
                        buy_price = bid
                    self.re_time = time.time()
                    self.log_displayer(token=token,
                                       points=self.ord_pts[token],
                                       ord_bid=buy_price,
                                       ord_len=ord_numbs)
                    buy_price, bid_id = self.post_bids(b_px=buy_price,
                                                       b_sz=b_sz,
                                                       token=token,
                                                       ccl_id=bid_id)
            # sell order not filled
            elif len(df_b) == 0 and len(df_s) != 0 and sleep > 1:
                if sell_price > buy_price:
                    if ask > buy_price + self.cms:
                        sell_price = ask
                    self.re_time = time.time()
                    self.log_displayer(token=token,
                                       points=self.ord_pts[token],
                                       ord_ask=sell_price,
                                       ord_len=ord_numbs)
                    sell_price, ask_id = self.post_asks(s_px=sell_price,
                                                        s_sz=s_sz,
                                                        step=step,
                                                        token=token,
                                                        ccl_id=ask_id)

    def post_bids(self, b_px, b_sz, token, ccl_id=None):
        pts_consumpt = self.ord_pt_consped(b_sz * b_px)
        self.ord_pts[token] -= pts_consumpt

        bid_id_ = self.create_limit_order(token=token,
                                          side='BUY',
                                          sz=b_sz,
                                          px=b_px,
                                          tm=self.tm,
                                          ccl_id=ccl_id)
        print("ccl", ccl_id)

        new_ord = {'id': bid_id_,
                   'sd': 'BUY',
                   'px': b_px,
                   'sz': b_sz}
        self.adjust_df_row('ord', 'add', new_ord, token)
        self.adjust_df_row('ord', 'ccl', ccl_id, token)

        b_px = round(b_px + config.sol_spread / 3, self.min_dec[token])
        # b_px = round((b_px - step), self.min_dec[token])

        return b_px, bid_id_

    def post_asks(self, s_px, s_sz, step, token, ccl_id=None):
        pts_consumpt = self.ord_pt_consped(s_sz * s_px)
        self.ord_pts[token] -= pts_consumpt

        ask_id_ = self.create_limit_order(token=token,
                                          side='SELL',
                                          sz=s_sz,
                                          px=s_px,
                                          tm=self.tm,
                                          ccl_id=ccl_id)
        print("ccl", ccl_id)

        new_ord = {'id': ask_id_,
                   'sd': 'SELL',
                   'px': s_px,
                   'sz': s_sz}
        self.adjust_df_row('ord', 'add', new_ord, token)
        self.adjust_df_row('ord', 'ccl', ccl_id, token)

        s_px = round(s_px - config.sol_spread / 3, self.min_dec[token])
        # s_px = round((s_px + step), self.min_dec[token])
        return s_px, ask_id_

    def adjust_df_row(self, type, method, object, token):
        if type == 'ord':
            if method == 'ccl':
                idx = self.ord_df[token][self.ord_df[token]['id'] == object].index
                self.ord_df[token].drop(idx, inplace=True)
                self.ord_df[token].reset_index(drop=True, inplace=True)

            elif method == 'add':
                self.ord_df[token].loc[len(self.ord_df[token])] = object

    def check_order_fills(self, ord_id, fill_sz, token):
        ord_idx = self.ord_df[token][self.ord_df[token]['id'] == ord_id].index
        if not ord_idx.empty:
            for i in ord_idx:
                sz = abs(float(self.ord_df[token].iloc[i, 3]))
                self.ord_df[token].iloc[i, 3] = sz - float(fill_sz)
                if self.ord_df[token].iloc[i, 3] <= 0:
                    self.adjust_df_row('ord', 'ccl', ord_id, token)

    def risk_executioner(self, token):
        if self.risk_on[token] == 1:
            self.risk_on[token] = time.time()
            self.cancel_all_order(token=token)
            self.ord_df[token] = self.ord_df[token][:0]
            self.log_displayer(token=token,
                               orders='cancelled',
                               risk='executed')

            pos_sz = abs(self.pos_size[token])
            if self.pos_size[token] < 0:
                tm = self.tm * 100
                bid = self.bid_d[token]
                px_rg = bid - bid * self.cms * 2
                buy_px = round(px_rg, self.min_dec[token])
                bid_id = self.create_limit_order(token=token,
                                                 side='BUY',
                                                 sz=pos_sz,
                                                 px=buy_px,
                                                 tm=tm)

            elif self.pos_size[token] > 0:
                tm = self.tm * 100
                ask = self.ask_d[token]
                px_rg = ask + ask * self.cms * 2
                sel_px = round(px_rg, self.min_dec[token])
                ask_id = self.create_limit_order(token=token,
                                                 side='SELL',
                                                 sz=pos_sz,
                                                 px=sel_px,
                                                 tm=tm)

        elif time.time() > self.risk_on[token] + 3600:
            self.risk_on[token] = 0
            self.log_displayer(token=token,
                               risk='off')

    def temp_risk_manager(self, risk, bid, ask, token):
        if token in self.bid_o and token in self.ask_o:
            bid_gap = abs(bid - self.bid_o[token])
            ask_gap = abs(self.ask_o[token] - ask)
            if max(bid_gap, ask_gap) > ask * 0.0031:
                self.risk_on[token] = 1
                self.log_displayer(token=token,
                                   risk='on',
                                   type='gap')

            elif self.mid_px[token]:
                mid_std = np.std(self.mid_px[token])
                mid_mean = np.mean(self.mid_px[token])
                risk_factor = mid_std / mid_mean
                if risk_factor > mid_mean * 0.0013:
                    self.risk_on[token] = 1
                    self.log_displayer(token=token,
                                       risk='on',
                                       type='vol')

    def ticks_vol(self, token):
        px_mean = np.mean(self.mid_px[token])
        px_latest = self.mid_px[token][-1]
        vol = abs(px_latest - px_mean) / max(px_mean, px_latest)
        return pow(px_latest, vol * 10)

    def build_orderbook(self, res, token):
        self.token_initialize(token)
        msg = res['contents']
        bids, asks = msg['bids'], msg['asks']
        for b, a in zip(bids, asks):
            b_px, b_sz, b_of = float(b['price']), float(b['size']), int(b['offset'])
            a_px, a_sz, a_of = float(a['price']), float(a['size']), int(a['offset'])
            if b_sz != 0:
                self.bids_orderbook[token]['px'].append(b_px)
                self.bids_orderbook[token]['sz'].append(b_sz)
                self.bids_orderbook[token]['of'].append(b_of)

            if a_sz != 0:
                self.asks_orderbook[token]['px'].append(a_px)
                self.asks_orderbook[token]['sz'].append(a_sz)
                self.asks_orderbook[token]['of'].append(a_of)

        self.bid_d[token] = self.bids_orderbook[token]['px'][0]
        self.ask_d[token] = self.asks_orderbook[token]['px'][0]

    def maintain_orderbook(self, res, token):
        orderbook = res['contents']
        new_of = int(orderbook['offset'])
        if orderbook['bids']:
            for i in range(0, len(orderbook['bids'])):
                bid_px = float(orderbook['bids'][i][0])
                bid_sz = float(orderbook['bids'][i][1])
                bids_length = len(self.bids_orderbook[token]['px'])
                if bid_sz == 0:
                    for j in range(0, bids_length):
                        if new_of > self.bids_orderbook[token]['of'][j]:
                            if bid_px == self.bids_orderbook[token]['px'][j]:
                                px_remove = self.bids_orderbook[token]['px'][j]
                                sz_remove = self.bids_orderbook[token]['sz'][j]
                                of_remove = self.bids_orderbook[token]['of'][j]
                                self.bids_orderbook[token]['px'].remove(px_remove)
                                self.bids_orderbook[token]['sz'].remove(sz_remove)
                                self.bids_orderbook[token]['of'].remove(of_remove)
                                break

                else:
                    for j in range(0, bids_length):
                        if new_of > self.bids_orderbook[token]['of'][j]:
                            if bid_px > self.bids_orderbook[token]['px'][j]:
                                self.bids_orderbook[token]['px'].insert(0, bid_px)
                                self.bids_orderbook[token]['sz'].insert(0, bid_sz)
                                self.bids_orderbook[token]['of'].insert(0, new_of)
                                break

                            elif bid_px == self.bids_orderbook[token]['px'][j]:
                                self.bids_orderbook[token]['sz'][j] = bid_sz
                                self.bids_orderbook[token]['of'][j] = new_of
                                break

                            else:
                                if j + 1 < bids_length:
                                    if bid_px > self.bids_orderbook[token]['px'][j + 1]:
                                        self.bids_orderbook[token]['px'].insert(j + 1, bid_px)
                                        self.bids_orderbook[token]['sz'].insert(j + 1, bid_sz)
                                        self.bids_orderbook[token]['of'].insert(j + 1, new_of)
                                        break

                                elif j + 1 == bids_length:
                                    self.bids_orderbook[token]['px'].append(bid_px)
                                    self.bids_orderbook[token]['sz'].append(bid_sz)
                                    self.bids_orderbook[token]['of'].append(new_of)
                                    break

        if orderbook['asks']:
            for i in range(0, len(orderbook['asks'])):
                ask_px = float(orderbook['asks'][i][0])
                ask_sz = float(orderbook['asks'][i][1])
                asks_length = len(self.asks_orderbook[token]['px'])
                if ask_sz == 0:
                    for j in range(0, asks_length):
                        if new_of > self.asks_orderbook[token]['of'][j]:
                            if ask_px == self.asks_orderbook[token]['px'][j]:
                                px_remove = self.asks_orderbook[token]['px'][j]
                                sz_remove = self.asks_orderbook[token]['sz'][j]
                                of_remove = self.asks_orderbook[token]['of'][j]
                                self.asks_orderbook[token]['px'].remove(px_remove)
                                self.asks_orderbook[token]['sz'].remove(sz_remove)
                                self.asks_orderbook[token]['of'].remove(of_remove)
                                break

                else:
                    for j in range(0, asks_length):
                        if new_of > self.asks_orderbook[token]['of'][j]:
                            if ask_px < self.asks_orderbook[token]['px'][j]:
                                self.asks_orderbook[token]['px'].insert(0, ask_px)
                                self.asks_orderbook[token]['sz'].insert(0, ask_sz)
                                self.asks_orderbook[token]['of'].insert(0, new_of)
                                break

                            elif ask_px == self.asks_orderbook[token]['px'][j]:
                                self.asks_orderbook[token]['sz'][j] = ask_sz
                                self.asks_orderbook[token]['of'][j] = new_of
                                break

                            else:
                                if j + 1 < asks_length:
                                    if ask_px < self.asks_orderbook[token]['px'][j + 1]:
                                        self.asks_orderbook[token]['px'].insert(j + 1, ask_px)
                                        self.asks_orderbook[token]['sz'].insert(j + 1, ask_sz)
                                        self.asks_orderbook[token]['of'].insert(j + 1, new_of)
                                        break

                                elif j + 1 == asks_length:
                                    self.asks_orderbook[token]['px'].append(ask_px)
                                    self.asks_orderbook[token]['sz'].append(ask_sz)
                                    self.asks_orderbook[token]['of'].append(new_of)
                                    break

        self.ticks[token] += 1
        self.bid_d[token] = self.bids_orderbook[token]['px'][0]
        self.ask_d[token] = self.asks_orderbook[token]['px'][0]

    def resolve_account_status(self, res):
        acc_content = res['contents']
        if 'fills' in acc_content:
            if acc_content['fills']:
                for i in range(0, len(acc_content['fills'])):
                    fill_sz = acc_content['fills'][i]['size']
                    fill_id = acc_content['fills'][i]['orderId']
                    fill_token = acc_content['fills'][i]['market']
                    if fill_token in self.token:
                        self.check_order_fills(ord_id=fill_id,
                                               fill_sz=fill_sz,
                                               token=fill_token)

        if 'positions' in acc_content:
            if acc_content['positions']:
                for i in range(0, len(acc_content['positions'])):
                    pos_token = acc_content['positions'][i]['market']
                    pos_size = float(acc_content['positions'][i]['size'])
                    if pos_token in self.token:
                        self.pos_size[pos_token] = float(pos_size)

        if acc_content.get('orders'):
            for i in range(0, len(acc_content['orders'])):
                status = acc_content['orders'][i]['status']
                ord_token = acc_content['orders'][i]['market']
                ord_id = acc_content['orders'][i]['id']
                if status == 'CANCELED':
                    if ord_token in self.token:
                        self.adjust_df_row(type='ord',
                                           method='ccl',
                                           object=ord_id,
                                           token=ord_token)
                elif status == 'FILLED':
                    if ord_token in self.token:
                        self.adjust_df_row(type='ord',
                                           method='ccl',
                                           object=ord_id,
                                           token=ord_token)

    def distribution_relay(self, res, exg):
        if exg == 'op':
            if res['arg']['channel'] == 'bbo-tbt':
                okx_token = res['arg']['instId'][:-6]
                if 'data' in res:
                    if okx_token in self.token:
                        okx_bids = float(res['data'][0]['bids'][0][0])
                        okx_asks = float(res['data'][0]['asks'][0][0])
                        if self.account_connection and okx_token in self.token:
                            self.temp_risk_manager(risk=exg,
                                                   bid=okx_bids,
                                                   ask=okx_asks,
                                                   token=okx_token)
                            self.bid_o[okx_token] = okx_bids
                            self.ask_o[okx_token] = okx_asks

                elif 'event' in res:
                    self.log_displayer(exchange=exg,
                                       token=okx_token,
                                       event=res['event'],
                                       channel=res['arg']['channel'])

        if exg == 'type':
            if res['type'] == 'channel_data':
                if res['channel'] == 'v3_orderbook' and self.token:
                    self.maintain_orderbook(res, res['id'])
                    if self.account_connection:
                        self.on_making(res['id'])

                elif res['channel'] == 'v3_accounts':
                    self.resolve_account_status(res)

            elif res['type'] == 'subscribed':
                if res['channel'] == 'v3_orderbook':
                    if res['id'] not in self.token:
                        self.token.append(res['id'])
                        self.build_orderbook(res, res['id'])
                        self.log_displayer(exchange=exg,
                                           token=res['id'],
                                           event=res['type'],
                                           channel=res['channel'])

                elif res['channel'] == 'v3_accounts':
                    self.account_connection = True
                    self.log_displayer(exchange=exg,
                                       connection_id=res['id'],
                                       event=res['type'],
                                       channel=res['channel'])

    @staticmethod
    def log_displayer(**kwargs):
        log_lst = []
        for key, value in kwargs.items():
            if key == 'exchange':
                if value == 'op':
                    value = 'okx'

                elif value == 'type':
                    value = 'dYdX'

            log_val = str(key) + ' : ' + str(value)
            log_lst.append(log_val)

        print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), end='')
        for i in log_lst:
            print(' |', i, end='')

        print('')
