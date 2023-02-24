from dydx3 import Client

import websockets
import asyncio
import json

import datetime as dt
import money_printer


class Perceiver:
    def __init__(self, client: Client):
        self.token = ''
        self.timeing = 0
        self.servers = []
        self.task_break = 0
        self.local_offset = 0

        self.okx_bid = 0
        self.okx_ask = 0
        self.bids_orderbook = []
        self.asks_orderbook = []

        self.mm = money_printer.M_making(client=client)
        self.okx_ws = 'wss://ws.okx.com:8443/ws/v5/public'

    def init_connect(self, token, net):
        ord_okx = self.mm.connect_orderbook_okx(type=1, token=token)
        acc_req = self.mm.connect_accounts_dYdX(type='ws', channel='accounts')
        ord_dydx = self.mm.connect_orderbook_dYdX(type=1, channel='orderbook', token=token)
        self.add_server(url=self.okx_ws, req=ord_okx)
        self.add_server(url=net, req=ord_dydx)
        self.add_server(url=net, req=acc_req)

    def build_orderbook(self, res):
        orderbook = res['contents']
        cancel_bid = []
        cancel_ask = []
        self.bids_orderbook = orderbook['bids']
        self.asks_orderbook = orderbook['asks']
        for i in range(0, len(self.bids_orderbook)):
            self.bids_orderbook[i]['size'] = float(self.bids_orderbook[i]['size'])
            self.bids_orderbook[i]['price'] = float(self.bids_orderbook[i]['price'])
            self.bids_orderbook[i]['offset'] = float(self.bids_orderbook[i]['offset'])
            if self.bids_orderbook[i]['size'] == 0:
                now_px = self.bids_orderbook[i]['price']
                for j in range(0, len(self.asks_orderbook)):
                    if float(self.asks_orderbook[j]['price']) == now_px:
                        if float(self.asks_orderbook[j]['size']) != 0:
                            cancel_bid.append(i)
                            break

        for m in range(0, len(self.asks_orderbook)):
            self.asks_orderbook[m]['size'] = float(self.asks_orderbook[m]['size'])
            self.asks_orderbook[m]['price'] = float(self.asks_orderbook[m]['price'])
            self.asks_orderbook[m]['offset'] = float(self.asks_orderbook[m]['offset'])
            if self.asks_orderbook[m]['size'] == '0':
                now_px = self.asks_orderbook[m]['price']
                for n in range(0, len(self.bids_orderbook)):
                    if self.bids_orderbook[n]['price'] == now_px:
                        if self.bids_orderbook[n]['size'] != '0':
                            cancel_ask.append(m)
                            break

        if cancel_bid != []:
            del self.bids_orderbook[:len(cancel_bid)]

        if cancel_ask != []:
            del self.asks_orderbook[:len(cancel_ask)]

    def maintain_orderbook(self, res):
        orderbook = res['contents']
        if int(orderbook['offset']) > self.local_offset:
            self.local_offset = int(orderbook['offset'])
            bid_top_insert = []
            ask_top_insert = []
            bid_mid_insert = {}
            ask_mid_insert = {}
            if orderbook['bids'] != []:
                for i in range(0, len(orderbook['bids'])):
                    bid_px = float(orderbook['bids'][i][0])
                    bid_sz = float(orderbook['bids'][i][1])
                    elm = {'price': bid_px,
                           'offset': self.local_offset,
                           'size': bid_sz}
                    if bid_px < self.asks_orderbook[0]['price']:
                        for j in range(0, len(self.bids_orderbook)):
                            if bid_px > self.bids_orderbook[j]['price']:
                                bid_top_insert.append(elm)
                                break

                            if bid_px == self.bids_orderbook[j]['price']:
                                self.bids_orderbook[j] = elm
                                break

                            if bid_px < self.bids_orderbook[j]['price']:
                                if j + 2 < len(self.bids_orderbook):
                                    if bid_px > self.bids_orderbook[j+1]['price']:
                                        bid_mid_insert[j+1] = elm
                                        break

                    if bid_px >= self.asks_orderbook[0]['price']:
                        for k in range(0, len(self.asks_orderbook)):
                            if self.asks_orderbook[k]['price'] == bid_px:
                                del self.asks_orderbook[:k+1]
                                break

                if bid_mid_insert != {}:
                    for i in bid_mid_insert.keys():
                        self.bids_orderbook.insert(i, bid_mid_insert[i])

                if bid_top_insert != []:
                    bid_top_insert.reverse()
                    for i in range(0, len(bid_top_insert)):
                        self.bids_orderbook.insert(0, bid_top_insert[i])

            if orderbook['asks'] != []:
                for i in range(0, len(orderbook['asks'])):
                    ask_px = float(orderbook['asks'][i][0])
                    ask_sz = float(orderbook['asks'][i][1])
                    elm = {'price': ask_px,
                           'offset': self.local_offset,
                           'size': ask_sz}
                    if ask_px > self.bids_orderbook[0]['price']:
                        for j in range(0, len(self.asks_orderbook)):
                            if ask_px < self.asks_orderbook[j]['price']:
                                ask_top_insert.append(elm)
                                break

                            if ask_px == self.asks_orderbook[j]['price']:
                                self.asks_orderbook[j] = elm
                                break

                            if ask_px > self.asks_orderbook[j]['price']:
                                if j + 2 < len(self.asks_orderbook):
                                    if ask_px < self.asks_orderbook[j + 1]['price']:
                                        ask_mid_insert[j + 1] = elm
                                        break

                    if ask_px <= self.bids_orderbook[0]['price']:
                        for k in range(0, len(self.bids_orderbook)):
                            if self.bids_orderbook[k]['price'] == ask_px:
                                del self.bids_orderbook[:k+1]
                                break

                if ask_mid_insert != {}:
                    for i in ask_mid_insert.keys():
                        self.asks_orderbook.insert(i, ask_mid_insert[i])

                if ask_top_insert != []:
                    ask_top_insert.reverse()
                    for i in range(0, len(ask_top_insert)):
                        self.asks_orderbook.insert(0, ask_top_insert[i])

    def distribution_relay(self, res, exg):
        if exg == 'op' and self.token != '':
            if res['arg']['channel'] == 'bbo-tbt':
                if res['arg']['instId'][:-10] == self.token:
                    if 'data' in res:
                        okx_bids = res['data'][0]['bids']
                        okx_asks = res['data'][0]['asks']
                        self.okx_bid = float(okx_bids[0][0])
                        self.okx_ask = float(okx_asks[0][0])

        if exg == 'type':
            if res['type'] == 'subscribed':
                if res['channel'] == 'v3_orderbook':
                    self.build_orderbook(res=res)

            if res['type'] == 'channel_data':
                if res['channel'] == 'v3_orderbook':
                    self.maintain_orderbook(res=res)
                    if self.token != res['id'][:-4]:
                        self.token = res['id'][:-4]
                        print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                              ' | trading token: ', self.token)

    async def connect_to_server(self, server_url, req):
        try:
            async with websockets.connect(server_url) as ws:
                await ws.send(json.dumps(req))
                exg = next(iter(req))
                while True:
                    try:
                        js_res = await asyncio.wait_for(ws.recv(), timeout=25)
                        res = json.loads(js_res)
                        self.distribution_relay(res=res, exg=exg)
                        self.mm.parse_message(res=res,
                                              exg=exg,
                                              dydx_bids=self.bids_orderbook,
                                              dydx_asks=self.asks_orderbook,
                                              okx_bid=self.okx_bid,
                                              okx_ask=self.okx_ask)
                        if self.task_break == 1:
                            self.task_break = 0
                            break

                    except asyncio.TimeoutError as e:
                        try:
                            if exg == 'op':
                                ping = 'ping'

                            elif exg == 'type':
                                ping = {'type': 'ping'}

                            await ws.send(json.dumps(ping))
                            res = await ws.recv()
                            print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), res)
                            continue

                        except Exception as e:
                            print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '连接关闭，正在重连...')
                            continue

        except Exception as e:
            print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            print(e)
            print("连接断开，正在重连……")

    async def multi_tasks(self):
        tasks = []
        for server in self.servers:
            tasks.append(asyncio.create_task(self.connect_to_server(server['url'], server['req'])))
            exg = next(iter(server['req']))
            if exg == 'type':
                print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), ' |',
                      'connecting exchange: ', 'dYdX', ' |',
                      'req: ', server['req'])

            if exg == 'op':
                print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), ' |',
                      'connecting exchange: ', 'okx', ' |',
                      'req: ', server['req'])

        await asyncio.gather(*tasks)

    async def check_if_cancel_server(self, ws, url, req):
        server = {'url': url, 'req': req}
        self.servers.remove(server)
        await ws.send(json.dumps(req))
        self.task_break = 1

    def add_server(self, url, req):
        self.servers.append({'url': url, 'req': req})


