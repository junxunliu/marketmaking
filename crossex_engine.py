from dydx3 import Client

import mid_relay
import config
import algo

import datetime as dt
import websockets
import asyncio
import json

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
        # while True:
            # try:
        async with websockets.connect(server_url) as ws:
            await ws.send(json.dumps(req))
            exg = next(iter(req))
            while True:
                try:
                    js_res = await asyncio.wait_for(ws.recv(), timeout=9)
                    res = json.loads(js_res)
                    self.mp.distribution_relay(res=res, exg=exg)

                except asyncio.TimeoutError as e:
                    # try:
                        if exg == 'op':
                            ping = 'ping'

                        elif exg == 'type':
                            ping = {'type': 'ping'}

                        await ws.send(json.dumps(ping))
                        res = await ws.recv()
                        continue

                    # except Exception as e:
                    #     print(e)
                    #     print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    #           '============================================',
                    #           '| task_failure |')
                    #     continue

            # except Exception as e:
            #     print(e)
            #     print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            #           '============================================',
            #           '| connection_shutdown | resubscribe |')
            #     await self.unsubscribe_server(ws, server_url, req)

    async def multi_tasks(self):
        tasks = []
        for server in self.servers:
            tasks.append(asyncio.create_task(self.connect_to_server(server['url'], server['req'])))

        await asyncio.gather(*tasks)

    async def unsubscribe_server(self, ws, url, req):
        server = {'url': url, 'req': req}
        self.servers.remove(server)
        exg = next(iter(req))
        print('==', req)
        if exg == 'type':
            if 'channel' in req:
                if req['channel'] == 'v3_orderbook':
                    msg = self.mp.connect_orderbook_dYdX(type=-1,
                                                         channel='orderbook',
                                                         token=req['id'])
                    return json.dumps(msg)

                elif req['channel'] == 'v3_accounts':
                    msg = self.mp.connect_accounts_dYdX(type=-1,
                                                        channel='accounts')
                    return json.dumps(msg)

        elif exg == 'op':
            if 'args' in req:
                msg = self.mp.connect_orderbook_okx(type=-1,
                                                    tokens=self.tokens)
                return json.dumps(msg)

    def add_server(self, url, req):
        self.servers.append({'url': url, 'req': req})


