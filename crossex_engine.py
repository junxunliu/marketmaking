import websockets
import asyncio
import json

import datetime as dt


async def connect_to_server(server_url, req):
    try:
        async with websockets.connect(server_url) as ws:
            await ws.send(json.dumps(req))
            while True:
                try:
                    res = await asyncio.wait_for(ws.recv(), timeout=2)
                    print(res)
                    # algo

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


class Perceiver:
    def __init__(self):
        self.servers = []

    def add_server(self, url, req):
        self.servers.append({"url": url, "req": req})

    async def multi_tasks(self):
        tasks = []
        for server in self.servers:
            tasks.append(asyncio.create_task(connect_to_server(server['url'], server['req'])))
            print('===========')

        await asyncio.gather(*tasks)


'''
multi_ws_client = perceiver()
multi_ws_client.add_server("ws://localhost:8000", "updates")
multi_ws_client.add_server("ws://localhost:8001", "notifications")
multi_ws_client.add_server("ws://localhost:8002", "messages")

asyncio.run(perceiver.multi_tasks())
'''
