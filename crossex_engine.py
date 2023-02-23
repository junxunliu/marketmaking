import websockets
import asyncio
import json
import algo

import datetime as dt
from dydx3 import Client


def dict_check(okx, dydx_book, dydx_trade, dydx_account):
    if okx and dydx_book and dydx_trade and dydx_account:
        return 1
    elif okx and (dydx_book or dydx_trade or dydx_account):
        return 2
    return 0


class Perceiver:
    def __init__(self, client: Client):
        self.client = client
        self.servers = []
        self.dict_okx = {}
        self.dict_dydx_book = {}
        self.dict_dydx_trade = {}
        self.dict_dydx_account = {}
        self.dict_dydx_market = {}

    def add_server(self, url, req):
        self.servers.append({"url": url, "req": req})

    async def multi_tasks(self):
        tasks = []
        for server in self.servers:
            tasks.append(asyncio.create_task(self.connect_to_server(server['url'], server['req'])))
            print('===========')

        await asyncio.gather(*tasks)

    async def connect_to_server(self, server_url, req):
        try:
            async with websockets.connect(server_url) as ws:
                await ws.send(json.dumps(req))
                while True:
                    try:
                        res = await asyncio.wait_for(ws.recv(), timeout=25)
                        res = json.loads(res)
                        exg = next(iter(req))
                        if exg == "op":
                            self.dict_okx = res
                        elif exg == "type":
                            try:
                                channel = res.get('channel')
                                if channel == "v3_orderbook":
                                    self.dict_dydx_book = res
                                elif channel == "v3_trades":
                                    self.dict_dydx_trade = res
                                elif channel == "v3_accounts":
                                    self.dict_dydx_account = res
                                elif channel == "v3_markets":
                                    self.dict_dydx_market = res

                            except KeyError:
                                print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "result has no key 'channel'")
                                continue
                        check = dict_check(self.dict_okx, self.dict_dydx_book, self.dict_dydx_trade,
                                           self.dict_dydx_account)
                        # if check == 1:
                        #     algo.Algo(self.client).run_algo(self.dict_okx,
                        #                                     self.dict_dydx_book,
                        #                                     self.dict_dydx_trade)
                        # print("okx===================", self.dict_okx)
                        print("book===================", self.dict_dydx_book)
                        # print("trade===================", self.dict_dydx_trade)
                        # print("account===================", self.dict_dydx_account)
                        # print("markets===================", self.dict_dydx_market)

                    except asyncio.TimeoutError:
                        try:
                            exg = next(iter(req))

                            if exg == "op":
                                ping = "ping"
                            elif exg == "type":
                                ping = {
                                    "type": "ping"
                                }
                            await ws.send(json.dumps(ping))
                            res = await ws.recv()
                            print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), res)
                            continue

                        except Exception:
                            print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '连接关闭，正在重连...')
                            continue

        except Exception as e:
            print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            print(e)
            print("连接断开，正在重连……")


'''
multi_ws_client = perceiver()
multi_ws_client.add_server("ws://localhost:8000", "updates")
multi_ws_client.add_server("ws://localhost:8001", "notifications")
multi_ws_client.add_server("ws://localhost:8002", "messages")

asyncio.run(perceiver.multi_tasks())
'''
