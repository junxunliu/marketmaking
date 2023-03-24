from dydx3 import Client
from dydx3.helpers.request_helpers import generate_now_iso

from mid_relay import Mediator
import config

import datetime as dt
import pandas as pd
import numpy as np
import random
import time


class Money_printer(Mediator):
    def __init__(self, client: Client):
        super(Money_printer, self).__init__(client=client)
        self.account_connection = 0

        self.ord_config = self.order_post_point_config()
        self.ord_mx_consump = float(self.ord_config['maxOrderConsumption'])
        self.ord_mx_pts = float(self.ord_config['maxPoints'])
        self.ord_w_sec = float(self.ord_config['windowSec'])

        self.token_numbers = len(config.trading_tokens)
        self.ord_numbs = config.order_numbers
        self.tm = config.order_existing_time
        self.cms = config.commissions

        self.timer = {}
        self.ticks = {}
        self.cln_tm = {}
        self.ord_pts = {}
        self.risk_on = {}
        self.pos_size = {}

        self.token = []
        self.bid_d = {}
        self.ask_d = {}
        self.bid_o = {}
        self.ask_o = {}
        self.mid_px = {}
        self.trades_p = {}
        self.bids_orderbook = {}
        self.asks_orderbook = {}

        self.td_sz = {}
        self.sz_dec = {}
        self.min_sz = {}
        self.min_dec = {}
        self.post_priority = {}

        self.ord_df = {}
        self.cln_df = {}

        self.reap = {}
        self.on_field = {}
        self.avail_bal = {}

    def token_initialize(self, token):
        token_info = config.dYdX_token_config(token)
        self.min_dec[token] = token_info['minimal_decimal']
        self.min_sz[token] = token_info['minimal_size']
        self.sz_dec[token] = token_info['size_decimal']
        self.td_sz[token] = self.min_sz[token]
        self.post_priority[token] = 0

        self.ord_df[token] = pd.DataFrame(columns=['id', 'sd', 'px', 'sz'])
        self.cln_df[token] = pd.DataFrame(columns=['id', 'sd', 'px', 'sz', 'md'])

        self.mid_px[token] = []
        self.bids_orderbook[token] = {'px': [], 'sz': [], 'of': []}
        self.asks_orderbook[token] = {'px': [], 'sz': [], 'of': []}

        self.ord_pts[token] = 0
        self.risk_on[token] = 0
        self.pos_size[token] = 0

        self.avail_bal[token] = 0
        self.ticks[token] = 9999
        self.timer[token] = time.time()
        self.cln_tm[token] = time.time()

    def acc_balance_distribution(self, token):
        account = self.acc_info()
        equity = float(account['equity'])
        self.avail_bal[token] = equity / self.token_numbers

        if 'openPositions' in account:
            pos_info = account['openPositions']
            if token in pos_info:
                self.pos_size[token] = float(pos_info[token]['size'])

        acc_ids, ord_ids, cln_ids = [], [], []
        ord_info = self.check_orders(token)
        for i in ord_info:
            acc_ids.append(i['id'])

        if not self.ord_df[token].empty:
            c_idx = self.cln_df[token][self.cln_df[token]['md'] == 'clean'].index
            for i in c_idx:
                ord_id = self.ord_df[token].iloc[i, 0]
                if ord_id not in acc_ids:
                    ord_ids.append(ord_id)

        if ord_ids != []:
            for i in ord_ids:
                self.adjust_df_row('ord', 'ccl', i, token)

        if not self.cln_df[token].empty:
            for i in range(0, len(self.cln_df[token])):
                cln_id = self.cln_df[token].iloc[i, 0]
                if cln_id not in acc_ids:
                    cln_ids.append(cln_id)

        if cln_ids != []:
            for i in cln_ids:
                self.adjust_df_row('cln', 'ccl', i, token)

        if self.td_sz[token] == self.min_sz[token]:
            total_sz = self.avail_bal[token] / self.bid_d[token]
            mx_sz = max(total_sz * 0.02, self.min_sz[token])
            self.td_sz[token] = round(mx_sz, self.sz_dec[token])

    def ord_reset_pts(self, temp_tm, token):
        if self.ord_pts[token] == self.ord_mx_pts:
            self.timer[token] = temp_tm

        if temp_tm - self.timer[token] > self.ord_w_sec:
            self.timer[token] = temp_tm
            self.ord_pts[token] = self.ord_mx_pts

    def on_making(self, token):
        now_time = time.time()
        self.ord_reset_pts(now_time, token)

        bid, ask = self.bid_d[token], self.ask_d[token]
        mid_px = abs(bid + ask) / 2

        tick = pow(0.1, self.min_dec[token])
        self.mid_px[token].append(mid_px)
        if len(self.mid_px[token]) > 1000:
            self.mid_px[token].pop(0)

        if self.ticks[token] > 1000:
            self.ticks[token] = 0
            self.acc_balance_distribution(token)

        if self.risk_on[token] == 0:
            self.on_post(mid_px=mid_px,
                         tick=tick,
                         now_tm=now_time,
                         token=token)

            self.on_clean(mid_px=mid_px,
                          tick=tick,
                          now_tm=now_time,
                          token=token)

        else:
            self.risk_executioner(now_time, token)

    def on_clean(self, mid_px, tick, now_tm, token):
        td_lst = self.trades_p[token]
        px_dec = self.min_dec[token]
        sz_dec = self.sz_dec[token]
        vol = self.ticks_vol(token)
        step = (self.cms * mid_px * 3 + tick / 2)
        p_idx = self.cln_df[token][self.cln_df[token]['md'] == 'place'].index
        c_idx = self.cln_df[token][self.cln_df[token]['md'] == 'clean'].index
        if self.ord_pts[token] > self.ord_mx_consump * 5:
            inv_killer = 0
            tick = tick * vol
            ord_sz = round(self.td_sz[token] * 2, sz_dec)
            if not self.cln_df[token].empty:
                if len(self.cln_df[token]['sd'].unique()) == 1 and len(self.cln_df) == 2:
                    inv_killer = 1

            if abs(self.pos_size[token]) * mid_px > self.avail_bal[token] * 3:
                inv_killer = 1

            if not c_idx.empty and inv_killer == 0:
                for i in c_idx:
                    ccl_id = self.cln_df[token].iloc[i, 0]
                    ord_sd = self.cln_df[token].iloc[i, 1]
                    ord_px = self.cln_df[token].iloc[i, 2]
                    if ord_sd == 'SELL':
                        px_adjust = min(ord_px - step, mid_px - tick * 2)
                        ord_px = round(px_adjust, px_dec)
                        ord_sd = 'BUY'

                    elif ord_sd == 'BUY':
                        px_adjust = max(ord_px + step, mid_px + tick * 2)
                        ord_px = round(px_adjust, px_dec)
                        ord_sd = 'SELL'

                    self.log_displayer(token=token,
                                       go='tp',
                                       sd=ord_sd,
                                       px=ord_px)

                    self.post_ord(ccl_id=ccl_id,
                                  sd=ord_sd,
                                  px=ord_px,
                                  sz=ord_sz,
                                  tk=tick,
                                  stg='cln',
                                  md='cleaned',
                                  tm=now_tm,
                                  token=token)

            if now_tm - self.cln_tm[token] > random.uniform(0.31, 1.3):
                if not p_idx.empty:
                    for i in p_idx:
                        aprh = 0
                        ccl_id = self.cln_df[token].iloc[i, 0]
                        ord_sd = self.cln_df[token].iloc[i, 1]
                        ord_px = self.cln_df[token].iloc[i, 2]
                        ord_px = round(ord_px, px_dec)
                        re_pos = abs(ord_px - mid_px) / mid_px
                        if mid_px > td_lst[-1] and self.micro_trend(td_lst) == 1:
                            if ord_px < mid_px - tick * 2:
                                aprh = 1
                                if re_pos > 0.0031:
                                    px_rg = min(mid_px - step * 0.5, mid_px - tick)
                                    ord_px = round(px_rg, px_dec)

                            elif ord_px > mid_px + tick * 2:
                                if re_pos < 0.0013 and ord_sd == 'SELL':
                                    ord_px = round(ord_px + tick * 2, px_dec)
                                    aprh = 1

                        elif mid_px < td_lst[-1] and self.micro_trend(td_lst) == -1:
                            if ord_px > mid_px + tick * 2:
                                aprh = 1
                                if re_pos > 0.0031:
                                    px_rg = max(mid_px + step * 0.5, mid_px + tick)
                                    ord_px = round(px_rg, px_dec)

                            elif ord_px < mid_px - tick * 2:
                                if re_pos < 0.0013 and ord_sd == 'BUY':
                                    ord_px = round(ord_px - tick * 2, px_dec)
                                    aprh = 1

                        if aprh == 1:
                            if inv_killer == 1:
                                if ord_sd == 'SELL':
                                    px_rg = max(mid_px + step * 0.5, mid_px + tick)
                                    ord_px = round(px_rg, px_dec)

                                elif ord_sd == 'BUY':
                                    px_rg = min(mid_px - step * 0.5, mid_px - tick)
                                    ord_px = round(px_rg, px_dec)

                            self.post_ord(ccl_id=ccl_id,
                                          sd=ord_sd,
                                          px=ord_px,
                                          sz=ord_sz,
                                          tk=tick,
                                          stg='cln',
                                          md='place',
                                          tm=now_tm,
                                          token=token)

                if len(self.cln_df[token]) < 2 or inv_killer == 1:
                    if (max(td_lst[-20:]) - min(td_lst[-20:])) / mid_px > 0.0013:
                        ccl_id, clean_on, ord_px, plc_px, plc_sd = 0, 0, 0, 0, ''
                        if not p_idx.empty:
                            ccl_id = self.cln_df[token].iloc[p_idx[0], 0]
                            plc_sd = self.cln_df[token].iloc[p_idx[0], 1]

                        ord_unique = self.ord_df[token]['sd'].unique()
                        b_vol, a_vol = self.micro_volume(mid_px, step, token)
                        if self.micro_trend(td_lst) == 1 and b_vol < a_vol:
                            if self.pos_size[token] <= 0 and mid_px > td_lst[-1]:
                                if len(ord_unique) == 1 and ord_unique[0] == 'BUY':
                                    px_rg = min(mid_px - step * 0.5, td_lst[-1])
                                    ord_px = round(px_rg, px_dec)
                                    re_pos = abs(plc_px - ord_px) / mid_px
                                    ord_sd = 'BUY'
                                    clean_on = 1

                        elif self.micro_trend(td_lst) == -1 and b_vol > a_vol:
                            if self.pos_size[token] >= 0 and mid_px < td_lst[-1]:
                                if len(ord_unique) == 1 and ord_unique[0] == 'SELL':
                                    px_rg = max(mid_px + step * 0.5, td_lst[-1])
                                    ord_px = round(px_rg, px_dec)
                                    re_pos = abs(plc_px - ord_px) / mid_px
                                    ord_sd = 'SELL'
                                    clean_on = 1

                        if clean_on == 1 and ord_sd != plc_sd:
                            self.log_displayer(token=token,
                                               do='aprh',
                                               sd=ord_sd,
                                               px=ord_px)

                            self.post_ord(ccl_id=ccl_id,
                                          sd=ord_sd,
                                          px=ord_px,
                                          sz=ord_sz,
                                          tk=tick,
                                          stg='cln',
                                          md='place',
                                          tm=now_tm,
                                          token=token)

    def on_post(self, mid_px, tick, now_tm, token):
        pos_sz = abs(self.pos_size[token])
        px_dec = self.min_dec[token]
        vol = self.ticks_vol(token)

        ccl_b, ccl_a = 0, 0
        step = (self.cms * mid_px * 5 + tick / 2) * vol
        b_rg = mid_px - min(self.mid_px[token])
        a_rg = max(self.mid_px[token]) - mid_px
        tpx = max(mid_px + step, mid_px + b_rg)
        btx = min(mid_px - step, mid_px - a_rg)
        buy_px = round(btx, px_dec)
        sel_px = round(tpx, px_dec)
        b_idx = self.ord_df[token][self.ord_df[token]['sd'] == 'BUY'].index
        a_idx = self.ord_df[token][self.ord_df[token]['sd'] == 'SELL'].index
        while self.ord_pts[token] > self.ord_mx_consump * 3:
            if len(self.ord_df[token]) >= self.ord_numbs:
                if not b_idx.empty:
                    ccl_b = self.ord_df[token].iloc[b_idx[0], 0]

                if not a_idx.empty:
                    ccl_a = self.ord_df[token].iloc[a_idx[0], 0]

            if len(self.ord_df[token]) < self.ord_numbs:
                if mid_px > np.mean(self.mid_px[token]):
                    self.post_priority[token] = 1

                elif mid_px < np.mean(self.mid_px[token]):
                    self.post_priority[token] = -1

                b_sz = self.td_sz[token]
                s_sz = self.td_sz[token]
                if self.pos_size[token] < 0:
                    sz_ra = (pos_sz / b_sz) * step * 0.1
                    sel_rg = sel_px + sz_ra
                    buy_rg = buy_px - sz_ra * 2
                    sel_px = round(sel_rg, px_dec)
                    buy_px = round(buy_px, px_dec)
                    self.post_priority[token] = 1

                elif self.pos_size[token] > 0:
                    sz_ra = (pos_sz / s_sz) * step * 0.1
                    buy_rg = buy_px - sz_ra
                    sel_rg = sel_px + sz_ra * 2
                    buy_px = round(buy_rg, px_dec)
                    sel_px = round(sel_rg, px_dec)
                    self.post_priority[token] = -1

                print(self.ord_df[token])
                self.log_displayer(token=token,
                                   pos=self.pos_size[token],
                                   points=self.ord_pts[token],
                                   ord_bid=buy_px,
                                   ord_ask=sel_px,
                                   ord_len=len(self.ord_df[token]),
                                   bid=self.bid_d[token],
                                   ask=self.ask_d[token],
                                   step=step,
                                   vol=vol)

                if self.post_priority[token] >= 0:
                    self.post_ord(ccl_id=ccl_b,
                                  sd='BUY',
                                  px=buy_px,
                                  sz=b_sz,
                                  tk=tick,
                                  stg='ord',
                                  md='N/A',
                                  tm=now_tm,
                                  token=token)

                    self.post_ord(ccl_id=ccl_a,
                                  sd='SELL',
                                  px=sel_px,
                                  sz=s_sz,
                                  tk=tick,
                                  stg='ord',
                                  md='N/A',
                                  tm=now_tm,
                                  token=token)


                elif self.post_priority[token] <= 0:
                    self.post_ord(ccl_id=ccl_a,
                                  sd='SELL',
                                  px=sel_px,
                                  sz=s_sz,
                                  tk=tick,
                                  stg='ord',
                                  md='N/A',
                                  tm=now_tm,
                                  token=token)

                    self.post_ord(ccl_id=ccl_b,
                                  sd='BUY',
                                  px=buy_px,
                                  sz=b_sz,
                                  tk=tick,
                                  stg='ord',
                                  md='N/A',
                                  tm=now_tm,
                                  token=token)

                sel_px = round(sel_px + step, px_dec)
                buy_px = round(buy_px - step, px_dec)
                print(sel_px, buy_px)

            else:
                break

    def post_ord(self, ccl_id, sd, px, sz, tk, stg, md, tm, token):
        pts_consumpt = self.ord_pt_consped(px * sz)
        self.ord_pts[token] -= pts_consumpt
        if stg == 'ord':
            ord_tm = self.tm * 4

        elif stg == 'cln':
            ord_tm = self.tm
            self.cln_tm[token] = tm

        ord_px = 0
        ord_id = self.create_limit_order(token=token,
                                         side=sd,
                                         sz=sz,
                                         px=px,
                                         tm=ord_tm,
                                         ccl_id=ccl_id)
        if sd == 'SELL':
            px_adjust = - tk

        elif sd == 'BUY':
            px_adjust = tk

        px = round(px + px_adjust, self.min_dec[token])
        if stg == 'cln':
            new_ord = {'id': ord_id,
                       'sd': sd,
                       'px': px,
                       'sz': sz,
                       'md': md}

        elif stg == 'ord':
            new_ord = {'id': ord_id,
                       'sd': sd,
                       'px': px,
                       'sz': sz}

        self.adjust_df_row(stg, 'add', new_ord, token)
        self.adjust_df_row(stg, 'ccl', ccl_id, token)

    def risk_executioner(self, temp_tm, token):
        if self.ord_pts[token] > self.ord_mx_consump:
            if self.risk_on[token] == 1:
                self.cancel_all_order(token=token)
                self.acc_balance_distribution(token)
                pos_sz = abs(self.pos_size[token])
                if self.pos_size[token] < 0:
                    self.create_market_order(token=token,
                                             side='BUY',
                                             sz=pos_sz)
                    self.pos_size[token] += pos_sz

                elif self.pos_size[token] > 0:
                    self.create_market_order(token=token,
                                             side='SELL',
                                             sz=pos_sz)
                    self.pos_size[token] -= pos_sz

                else:
                    self.risk_on[token] = temp_tm
                    self.cancel_all_order(token=token)
                    self.ord_df[token] = self.ord_df[token][:0]
                    self.log_displayer(token=token,
                                       orders='cancelled',
                                       risk='executed')

            elif temp_tm > self.risk_on[token] + 3600:
                if self.risk_on[token] != 1:
                    self.risk_on[token] = 0
                    self.cancel_all_order(token=token)
                    self.log_displayer(token=token,
                                       risk='off')

    def temp_risk_manager(self, bid, ask, token):
        if self.risk_on[token] == 0:
            if token in self.bid_o and token in self.ask_o:
                bid_gap = abs(bid - self.bid_o[token])
                ask_gap = abs(self.ask_o[token] - ask)
                if max(bid_gap, ask_gap) > ask * 0.0031:
                    self.risk_on[token] = 1
                    self.log_displayer(token=token,
                                       risk='on',
                                       type='gap',
                                       gap=max(bid_gap, ask_gap))

                elif self.mid_px[token]:
                    if self.ticks_vol(token) > 5:
                        self.risk_on[token] = 1
                        self.log_displayer(token=token,
                                           risk='on',
                                           type='vol')

    def ticks_vol(self, token):
        mid_std = np.std(self.mid_px[token])
        mid_mean = np.mean(self.mid_px[token])
        risk_factor = mid_std / mid_mean
        return risk_factor * 1000 + 1

    def micro_volume(self, mid_px, step, token):
        ask_lst = self.asks_orderbook[token]['px']
        bid_lst = self.bids_orderbook[token]['px']
        ask_rg = mid_px + step
        bid_rg = mid_px - step
        b_vol, a_vol = 0, 0
        for i in ask_lst:
            if i > ask_rg:
                idx = ask_lst.index(i)
                a_vol = sum(self.asks_orderbook[token]['sz'][:idx])
                break

        for i in bid_lst:
            if i < bid_rg:
                idx = bid_lst.index(i)
                b_vol = sum(self.bids_orderbook[token]['sz'][:idx])
                break

        return b_vol, a_vol

    def micro_trend(self, lst):
        diffs = np.diff(np.array(lst[::-1]))
        index_down_trend = np.where(diffs < 0)[0][0]
        index_up_trend = np.where(diffs > 0)[0][0]
        if index_down_trend > 1:
            return -1

        elif index_up_trend > 1:
            return 1

        else:
            return 0

    def trades_analyzer(self, channel, res, token):
        trades_info = res['contents']['trades']
        if channel == 1:
            new_trade = float(trades_info[0]['price'])
            self.trades_p[token].append(new_trade)
            if len(self.trades_p[token]) > 100:
                self.trades_p[token].pop(0)

        elif channel == 0:
            trades_info.reverse()
            self.trades_p[token] = []
            self.account_connection += 1

            for i in trades_info:
                self.trades_p[token].append(float(i['price']))
                if len(self.trades_p[token]) > 100:
                    self.trades_p[token].pop(0)

    def check_order_fills(self, ord_id, fill_sz, token):
        ord_idx = self.ord_df[token][self.ord_df[token]['id'] == ord_id].index
        cln_idx = self.cln_df[token][self.cln_df[token]['id'] == ord_id].index
        if not ord_idx.empty:
            for i in ord_idx:
                self.ord_df[token].iloc[i, 3] -= abs(float(fill_sz))
                if self.ord_df[token].iloc[i, 3] <= 0:
                    self.adjust_df_row('ord', 'ccl', ord_id, token)

        if not cln_idx.empty:
            for i in cln_idx:
                self.cln_df[token].iloc[i, 3] -= abs(float(fill_sz))
                if self.cln_df[token].iloc[i, 3] <= 0:
                    print(self.cln_df[token])
                    if self.cln_df[token].iloc[i, 4] == 'place':
                        self.cln_df[token].iloc[i, 4] = 'clean'

                    elif self.cln_df[token].iloc[i, 4] == 'cleaned':
                        self.adjust_df_row('cln', 'ccl', ord_id, token)

    def adjust_df_row(self, stg, method, target, token):
        if stg == 'ord':
            if method == 'ccl':
                idx = self.ord_df[token][self.ord_df[token]['id'] == target].index
                self.ord_df[token].drop(idx, inplace=True)
                self.ord_df[token].reset_index(drop=True, inplace=True)

            elif method == 'add':
                self.ord_df[token].loc[len(self.ord_df[token])] = target

        elif stg == 'cln':
            if method == 'ccl':
                idx = self.cln_df[token][self.cln_df[token]['id'] == target].index
                self.cln_df[token].drop(idx, inplace=True)
                self.cln_df[token].reset_index(drop=True, inplace=True)

            elif method == 'add':
                self.cln_df[token].loc[len(self.cln_df[token])] = target

    def build_orderbook(self, res, token):
        self.token_initialize(token)
        msg = res['contents']
        bids, asks = msg['bids'], msg['asks']
        for b, a in zip(bids, asks):
            b_px = round(float(b['price']), self.min_dec[token])
            b_sz = round(float(b['size']), self.min_dec[token])
            b_of = int(b['offset'])

            a_px = round(float(a['price']), self.min_dec[token])
            a_sz = round(float(a['size']), self.min_dec[token])
            a_of = int(a['offset'])

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
        self.account_connection += 1
        self.order_block(token)

    def maintain_orderbook(self, res, token):
        orderbook = res['contents']
        new_of = int(orderbook['offset'])
        if orderbook['bids']:
            for i in range(0, len(orderbook['bids'])):
                bid_px = round(float(orderbook['bids'][i][0]), self.min_dec[token])
                bid_sz = round(float(orderbook['bids'][i][1]), self.min_dec[token])
                bids_length = len(self.bids_orderbook[token]['px'])
                for j in range(0, len(self.asks_orderbook[token])):
                    if bid_px < self.asks_orderbook[token]['px'][j]:
                        self.asks_orderbook[token]['px'] = self.asks_orderbook[token]['px'][j:]
                        self.asks_orderbook[token]['sz'] = self.asks_orderbook[token]['sz'][j:]
                        self.asks_orderbook[token]['of'] = self.asks_orderbook[token]['of'][j:]
                        break

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
                    if bid_px > self.bids_orderbook[token]['px'][0]:
                        self.bids_orderbook[token]['px'].insert(0, bid_px)
                        self.bids_orderbook[token]['sz'].insert(0, bid_sz)
                        self.bids_orderbook[token]['of'].insert(0, new_of)

                    else:
                        for j in range(0, bids_length):
                            if new_of > self.bids_orderbook[token]['of'][j]:
                                if bid_px == self.bids_orderbook[token]['px'][j]:
                                    self.bids_orderbook[token]['sz'][j] = bid_sz
                                    self.bids_orderbook[token]['of'][j] = new_of
                                    break

                                elif bid_px < self.bids_orderbook[token]['px'][j]:
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
                ask_px = round(float(orderbook['asks'][i][0]), self.min_dec[token])
                ask_sz = round(float(orderbook['asks'][i][1]), self.min_dec[token])
                asks_length = len(self.asks_orderbook[token]['px'])
                for j in range(0, len(self.bids_orderbook[token])):
                    if ask_px > self.bids_orderbook[token]['px'][j]:
                        self.bids_orderbook[token]['px'] = self.bids_orderbook[token]['px'][j:]
                        self.bids_orderbook[token]['sz'] = self.bids_orderbook[token]['sz'][j:]
                        self.bids_orderbook[token]['of'] = self.bids_orderbook[token]['of'][j:]
                        break

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
                    if ask_px < self.asks_orderbook[token]['px'][0]:
                        self.asks_orderbook[token]['px'].insert(0, ask_px)
                        self.asks_orderbook[token]['sz'].insert(0, ask_sz)
                        self.asks_orderbook[token]['of'].insert(0, new_of)

                    else:
                        for j in range(0, asks_length):
                            if new_of > self.asks_orderbook[token]['of'][j]:
                                if ask_px == self.asks_orderbook[token]['px'][j]:
                                    self.asks_orderbook[token]['sz'][j] = ask_sz
                                    self.asks_orderbook[token]['of'][j] = new_of
                                    break

                                elif ask_px > self.asks_orderbook[token]['px'][j]:
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
                        self.adjust_df_row(stg='ord',
                                           method='ccl',
                                           target=ord_id,
                                           token=ord_token)
                        self.adjust_df_row(stg='cln',
                                           method='ccl',
                                           target=ord_id,
                                           token=ord_token)

    def distribution_relay(self, res, exg):
        if exg == 'op':
            if res['arg']['channel'] == 'bbo-tbt':
                okx_token = res['arg']['instId'][:-6]
                if 'data' in res:
                    if okx_token in self.token:
                        okx_bids = float(res['data'][0]['bids'][0][0])
                        okx_asks = float(res['data'][0]['asks'][0][0])
                        if self.account_connection >= 3 and okx_token in self.token:
                            self.temp_risk_manager(bid=okx_bids,
                                                   ask=okx_asks,
                                                   token=okx_token)
                            self.bid_o[okx_token] = okx_bids
                            self.ask_o[okx_token] = okx_asks

                elif 'event' in res:
                    self.log_displayer(exchange=exg,
                                       token=okx_token,
                                       event=res['event'],
                                       channel=res['arg']['channel'])

            else:
                self.log_displayer(exchange=exg,
                                   message=res)

        if exg == 'type':
            if res['type'] == 'channel_data':
                if self.account_connection >= 3:
                    if res['channel'] == 'v3_orderbook':
                        self.maintain_orderbook(res, res['id'])
                        self.on_making(res['id'])

                    elif res['channel'] == 'v3_trades':
                        self.trades_analyzer(1, res, res['id'])

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
                    self.account_connection += 1
                    self.log_displayer(exchange=exg,
                                       connection_id=res['id'],
                                       event=res['type'],
                                       channel=res['channel'])

                elif res['channel'] == 'v3_trades':
                    self.trades_analyzer(0, res, res['id'])
                    self.log_displayer(exchange=exg,
                                       token=res['id'],
                                       event=res['type'],
                                       channel=res['channel'])

            elif res['type'] != 'pong' and res['type'] != 'connected':
                self.log_displayer(exchange=exg,
                                   message=res)

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

