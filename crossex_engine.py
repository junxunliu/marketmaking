import websockets
import asyncio
import json

import datetime as dt

class perceiver:
    def __init__(self):
        self.servers = []

    def add_server(self, url, req):
        self.servers.append({"url": url, "req": req})

    async def connect_to_server(self, server_url, req):
        try:
            async with websockets.connect(server_url) as ws:
                await ws.send(json.dumps(req))
                while True:
                    try:
                        res = await asyncio.wait_for(ws.recv(), timeout=25)
                        print(res)

                    except asyncio.TimeoutError as e:
                        try:
                            await ws.send('ping')
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
            print('===========')

        await asyncio.gather(*tasks)
'''
multi_ws_client = perceiver()
multi_ws_client.add_server("ws://localhost:8000", "updates")
multi_ws_client.add_server("ws://localhost:8001", "notifications")
multi_ws_client.add_server("ws://localhost:8002", "messages")

asyncio.run(perceiver.multi_tasks())
'''