import asyncio
import datetime as dt
import json
import time

import websockets
from dydx3 import Client

import algo
import config


class Perceiver:
    def __init__(self, client: Client):
        self.okx_ws = 'wss://ws.okx.com:8443/ws/v5/public'
        self.mp = algo.Money_printer(client=client)
        self.tokens = config.trading_tokens
        self.servers = []

    def init_connect(self, net):
        for token in self.tokens:
            ord_dydx = self.mp.connect_orderbook_dYdX(type=1,
                                                      channel='orderbook',
                                                      token=token)
            self.add_server(url=net, req=ord_dydx)

        ord_okx = self.mp.connect_orderbook_okx(type=1, tokens=self.tokens)
        acc_req = self.mp.connect_accounts_dYdX(type=1, channel='accounts')
        self.add_server(url=self.okx_ws, req=ord_okx)
        self.add_server(url=net, req=acc_req)

    async def connect_to_server(self, server_url, req):
        while True:
            try:
                async with websockets.connect(server_url) as ws:
                    exg = next(iter(req))
                    msg = self.adjust_subscribe_server(status=1,
                                                       ws=ws,
                                                       exg=exg,
                                                       req=req,
                                                       url=server_url)
                    await ws.send(json.dumps(msg))
                    while True:
                        try:
                            js_res = await asyncio.wait_for(ws.recv(), timeout=9)
                            res = json.loads(js_res)
                            self.mp.distribution_relay(res=res, exg=exg)

                        except asyncio.TimeoutError as e:
                            try:
                                if exg == 'op':
                                    ping = 'ping'

                                elif exg == 'type':
                                    ping = {'type': 'ping'}

                                await ws.send(json.dumps(ping))
                                res = await ws.recv()
                                continue

                            except Exception as e:
                                print(e, flush=True)
                                print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                      '============================================',
                                      '| task_failure |', flush=True)

            except Exception as e:
                print(e, flush=True)
                print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                      '============================================',
                      '| connection_shutdown | resubscribe |', flush=True)
                time.sleep(1)

    async def multi_tasks(self):
        tasks = []
        for server in self.servers:
            tasks.append(asyncio.create_task(self.connect_to_server(server['url'], server['req'])))

        await asyncio.gather(*tasks)

    def adjust_subscribe_server(self, status, ws, exg, req, url):
        server = {'url': url, 'req': req}
        if server in self.servers and status == -1:
            self.servers.remove(server)

        if exg == 'type':
            if 'channel' in req:
                if req['channel'] == 'v3_orderbook':
                    msg = self.mp.connect_orderbook_dYdX(type=status,
                                                         channel='orderbook',
                                                         token=req['id'])
                    return msg

                elif req['channel'] == 'v3_accounts':
                    msg = self.mp.connect_accounts_dYdX(type=status,
                                                        channel='accounts')
                    return msg

        elif exg == 'op':
            if 'args' in req:
                msg = self.mp.connect_orderbook_okx(type=status,
                                                    tokens=self.tokens)
                return msg

    def add_server(self, url, req):
        self.servers.append({'url': url, 'req': req})


