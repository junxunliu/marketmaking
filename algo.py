from dydx3 import Client
from dydx3.constants import *
from dydx3.helpers.request_helpers import generate_now_iso

from mid_relay import Mediator
import config

import datetime as dt
import pandas as pd
import numpy as np
import time


class Money_printer(Mediator):
    def __init__(self, client: Client):
        super(Money_printer, self).__init__(client=client)
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

        self.ord_df[token] = pd.DataFrame(columns=['id', 'sd', 'px', 'sz'])

        self.mid_px[token] = []
        self.bids_orderbook[token] = {'px': [], 'sz': [], 'of': []}
        self.asks_orderbook[token] = {'px': [], 'sz': [], 'of': []}

        self.risk_on[token] = 0
        self.pos_size[token] = 0

        self.time_update[token] = generate_now_iso()[:-1]
        self.ticks[token] = 9999

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

    def ord_reset_pts(self, token):
        if self.ord_pts[token] == self.ord_mx_pts:
            self.timer[token] = time.time()

        if time.time() - self.timer[token] > self.ord_w_sec:
            self.timer[token] = time.time()
            self.ord_pts[token] = self.ord_mx_pts

    def on_making(self, token):
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
            self.post_order(mid_px, bid, ask, tick, border, token)
            if self.ticks[token] > 1000:
                if generate_now_iso()[:-1] > self.time_update[token]:
                    self.acc_balance_distribution(token)
                    self.ticks[token] = 0

    def post_order(self, mid_px, bid, ask, tk, step, token):
        sz_dec = self.sz_dec[token]
        balance_re = 0
        buy_px, sel_px = mid_px, mid_px
        vol = self.ticks_vol(token)
        step = step * vol
        px_dec = self.min_dec[token]
        buy_px = round((buy_px - step), px_dec)
        sel_px = round((sel_px + step), px_dec)
        cl_rg = (step + tk) * self.ord_numbs
        while True:
            post_on = 0
            b_re, a_re = {}, {}
            ccl_b, ccl_a = 0, 0
            b_sz = self.td_sz[token]
            s_sz = self.td_sz[token]
            for i in range(0, len(self.ord_df[token])):
                ord_id = self.ord_df[token].iloc[i, 0]
                ord_sd = self.ord_df[token].iloc[i, 1]
                ord_px = self.ord_df[token].iloc[i, 2]
                if ord_sd == 'SELL':
                    a_re[ord_id] = 0
                    if ask < ord_px - cl_rg:
                        a_re[ord_id] = 1
                        post_on = 1

                elif ord_sd == 'BUY':
                    b_re[ord_id] = 0
                    if bid > ord_px + cl_rg:
                        b_re[ord_id] = 1
                        post_on = 1

            ord_numbs = len(self.ord_df[token])
            if self.ord_pts[token] > self.ord_mx_consump * 3:
                if ord_numbs <= self.ord_numbs or post_on == 1:
                    acc_sz = abs(self.pos_size[token])
                    pos_sz = acc_sz - balance_re
                    if self.pos_size[token] < 0:
                        sz_ra = pos_sz / b_sz
                        px_rg = sel_px + step * sz_ra
                        sel_px = round(px_rg, px_dec)
                        self.post_priority[token] = 1
                        if pos_sz > b_sz + b_sz:
                            # b_sz = s_sz + s_sz
                            b_sz = s_sz + s_sz
                            b_sz = round(b_sz, sz_dec)
                            b_sz = abs(b_sz)
                            balance_re += b_sz

                    elif self.pos_size[token] > 0:
                        sz_ra = pos_sz / s_sz
                        px_rg = buy_px - step * sz_ra
                        buy_px = round(px_rg, px_dec)
                        self.post_priority[token] = -1
                        if pos_sz > s_sz + s_sz:
                            # s_sz = b_sz + b_sz
                            s_sz = b_sz + b_sz
                            s_sz = round(s_sz, sz_dec)
                            s_sz = abs(s_sz)
                            balance_re += s_sz

                    try:
                        self.log_displayer(token=token,
                                           points=self.ord_pts[token],
                                           ord_bid=buy_px,
                                           ord_ask=sel_px,
                                           bid=bid,
                                           ask=ask)

                        if self.post_priority[token] >= 0:
                            buy_px = self.post_bids(b_re=b_re,
                                                    ccl_b=ccl_b,
                                                    b_px=buy_px,
                                                    b_sz=b_sz,
                                                    step=step,
                                                    token=token)

                            sel_px = self.post_asks(a_re=a_re,
                                                    ccl_a=ccl_a,
                                                    s_px=sel_px,
                                                    s_sz=s_sz,
                                                    step=step,
                                                    token=token)

                        elif self.post_priority[token] <= 0:
                            sel_px = self.post_asks(a_re=a_re,
                                                    ccl_a=ccl_a,
                                                    s_px=sel_px,
                                                    s_sz=s_sz,
                                                    step=step,
                                                    token=token)

                            buy_px = self.post_bids(b_re=b_re,
                                                    ccl_b=ccl_b,
                                                    b_px=buy_px,
                                                    b_sz=b_sz,
                                                    step=step,
                                                    token=token)

                    except:
                        break

                else:
                    break

            else:
                break

    def post_bids(self, b_re, ccl_b, b_px, b_sz, step, token):
        if b_re:
            if 1 in b_re.values():
                cl_idx = list(b_re.values()).index(1)
                ccl_b = list(b_re.keys())[cl_idx]

            elif len(self.ord_df[token]) > self.ord_numbs:
                cl_idx = list(b_re.values()).index(0)
                ccl_b = list(b_re.keys())[cl_idx]

        bid_id = self.create_limit_order(token=token,
                                         side='BUY',
                                         sz=b_sz,
                                         px=b_px,
                                         tm=self.tm,
                                         ccl_id=ccl_b)

        new_ord = {'id': bid_id,
                   'sd': 'BUY',
                   'px': b_px,
                   'sz': b_sz}
        self.adjust_df_row('ord', 'add', new_ord, token)
        self.adjust_df_row('ord', 'ccl', ccl_b, token)
        pts_consumpt = self.ord_pt_consped(b_sz * b_px)
        self.ord_pts[token] -= pts_consumpt

        b_px = round((b_px - step), self.min_dec[token])
        return b_px

    def post_asks(self, a_re, ccl_a, s_px, s_sz, step, token):
        if a_re:
            if 1 in a_re.values():
                cl_idx = list(a_re.values()).index(1)
                ccl_a = list(a_re.keys())[cl_idx]

            elif len(self.ord_df[token]) > self.ord_numbs:
                cl_idx = list(a_re.values()).index(0)
                ccl_a = list(a_re.keys())[cl_idx]

        ask_id = self.create_limit_order(token=token,
                                         side='SELL',
                                         sz=s_sz,
                                         px=s_px,
                                         tm=self.tm,
                                         ccl_id=ccl_a)

        new_ord = {'id': ask_id,
                   'sd': 'SELL',
                   'px': s_px,
                   'sz': s_sz}
        self.adjust_df_row('ord', 'add', new_ord, token)
        self.adjust_df_row('ord', 'ccl', ccl_a, token)
        pts_consumpt = self.ord_pt_consped(s_sz * s_px)
        self.ord_pts[token] -= pts_consumpt

        s_px = round((s_px + step), self.min_dec[token])
        return s_px


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
            self.rist_on[token] = 0
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
            if acc_content['fills'] != []:
                for i in range(0, len(acc_content['fills'])):
                    fill_sz = acc_content['fills'][i]['size']
                    fill_id = acc_content['fills'][i]['orderId']
                    fill_token = acc_content['fills'][i]['market']
                    if fill_token in self.token:
                        self.check_order_fills(ord_id=fill_id,
                                               fill_sz=fill_sz,
                                               token=fill_token)

        if 'positions' in acc_content:
            if acc_content['positions'] != []:
                for i in range(0, len(acc_content['positions'])):
                    pos_token = acc_content['positions'][i]['market']
                    pos_size = float(acc_content['positions'][i]['size'])
                    if pos_token in self.token:
                        self.pos_size[pos_token] = float(pos_size)

        if acc_content['orders'] != []:
            for i in range(0, len(acc_content['orders'])):
                if acc_content['orders'][i]['status'] == 'CANCELED':
                    ord_token = acc_content['orders'][i]['market']
                    ord_id = acc_content['orders'][i]['id']
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

    def log_displayer(self, **kwargs):
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

