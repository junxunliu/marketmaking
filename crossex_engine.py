import asyncio
import datetime as dt
import json

import websockets
from dydx3 import Client

from algo import Algo


class Perceiver:
    def __init__(self, client: Client):
        self.timeing = 0
        self.servers = []
        self.task_break = 0

        self.algo = Algo(client)
        self.okx_ws = 'wss://ws.okx.com:8443/ws/v5/public'

    def init_connect(self, token, net):
        ord_okx = self.algo.connect_orderbook_okx(sub_type=1, token=token)
        acc_req = self.algo.connect_accounts_dYdX(sub_type=1, channel='accounts')
        ord_dydx = self.algo.connect_dYdX(sub_type=1, channel='orderbook', token=token, includeOffsets=True)
        trd_dydx = self.algo.connect_dYdX(sub_type=1, channel='trades', token=token)
        self.add_server(url=net, req=acc_req)
        self.add_server(url=self.okx_ws, req=ord_okx)
        self.add_server(url=net, req=ord_dydx)
        self.add_server(url=net, req=trd_dydx)

    async def connect_to_server(self, server_url, req):
        try:
            async with websockets.connect(server_url) as ws:
                await ws.send(json.dumps(req))
                exg = next(iter(req))
                while True:
                    try:
                        js_res = await asyncio.wait_for(ws.recv(), timeout=25)
                        res = json.loads(js_res)
                        self.algo.distribution_relay(res=res, exg=exg)
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
