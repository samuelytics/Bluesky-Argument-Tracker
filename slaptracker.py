import os
from atproto import Client
from convenient_pickle import *
from atproto import FirehoseSubscribeReposClient, firehose_models, parse_subscribe_repos_message
from atproto import CAR, models
from atproto_client.models.network.bsky.jetstream.subscribe_events import Commit
from atproto import AsyncJetstreamClient, jetstream_models, models, IdResolver
import time
import asyncio
import queue
import base64
from sortedcontainers import SortedList
import pandas as pd
from datetime import datetime
import aiohttp
import json


from starlette.applications import Starlette
from starlette.responses import HTMLResponse
from starlette.routing import Route, WebSocketRoute
from starlette.endpoints import WebSocketEndpoint
import uvicorn

SERVER_PORT=8000

index_str = """<!DOCTYPE HTML>
<html>
<head>
    <script type = "text/javascript">
    const websocket = new WebSocket("ws://127.0.0.1:%s");
    window.addEventListener("DOMContentLoaded", () => {
        websocket.onmessage = ({ data }) => {
            const docdiv = document.getElementById("place");
            docdiv.innerHTML = "";
            let process_data = JSON.parse(data)
            let keys = Object.keys(process_data)
            console.log(keys)
            for (let i = 0; i < process_data.subject.length; i++){
                console.log(process_data.subject[i] + ", " + process_data.rev[i])
            }
            console.log(process_data)
            let data_table = `
                <table border=1>
                    <tr> 
                        <th> Blocked Person </th>
                        <th> Number of Blocks </th>
                    </tr>
            `
            for (let j = 0; j < process_data.subject.length; j++){
                data_table += "<tr> <td>" + process_data.subject[j] + "</td>" + "<td>" + process_data.rev[j] + "</td> </tr>"
            }
            data_table += "</table>"
            docdiv.innerHTML = data_table


        };
    });
    </script>
</head>
<body>
    <div id="place"></div>
</body>
</html>
""" % (SERVER_PORT)

def homepage(request):
    return HTMLResponse(index_str)

class Consumer(WebSocketEndpoint):
    encoding = 'text'
    task = None
    client = AsyncJetstreamClient(params={'kinds':['commit']})
    
    msg = asyncio.Queue()
    outdict = dict()
    messagecount = 0
    stop_seconds = 500
    output_interval = 1
    overtime = None
    initialized = False
    finished = False
    last_second = -1
    start_time = None
    blocked_df = pd.DataFrame(columns =['collection', 'did', 'operation', 'rev', 'rkey', 'seq', 'subject','time', 'year', 'month','day','hour','minute'])
    blockedest = None
    blocked_rows = []
    
    
    
    async def on_message_handler(self,event):
        #print('or maybe here?')
        try:
            await self.msg.put(event)
        except: 
            pass
    
    async def stop_after_n_sec(self):
        #await ws.send_text('ayyyy')
        await asyncio.sleep(self.stop_seconds)
        await self.client.stop()
        self.overtime = time.time()
        await self.msg.put(None)
        self.finished = True
    
    async def start_the_client(self):
        await self.client.start(self.on_message_handler)
    
    
    async def pop_em_over(self):
        
        while True:
            message = await self.msg.get()
            if message is None:
                break
            self.messagecount += 1
            if message.collection not in self.outdict.keys():
                self.outdict[message.collection] = asyncio.Queue()
                await self.outdict[message.collection].put(message)
            else: 
                await self.outdict[message.collection].put(message)
    

    async def assemble_blocked_rows(self): 
        while True: 
            q = self.outdict.get('app.bsky.graph.block')
            if q is None: 
                if self.finished: 
                    break
                await asyncio.sleep(.05)
                continue
            try:
                blocking = await asyncio.wait_for(q.get(), timeout=0.5)
            except asyncio.TimeoutError:
                if self.finished and q.empty():
                    break
                continue
            
            if blocking.record is not None:
                date = datetime.fromisoformat(blocking.time)
                new_row = {
                    'collection': blocking.collection,
                    'did': blocking.did,
                    'operation': blocking.operation,
                    'rev': blocking.rev,
                    'rkey': blocking.rkey,
                    'seq': blocking.seq,
                    'subject': blocking.record.subject,
                    'time': date,
                    'year': date.year,
                    'month': date.month,
                    'day': date.day,
                    'hour': date.hour,
                    'minute': date.minute
                }
                self.blocked_rows.append(new_row)
                
    
    
    
    async def track_the_blockedest(self,ws): 
        while not self.finished: 
            if len(self.blocked_rows) > 0: 
                self.blocked_df = pd.DataFrame(self.blocked_rows)
                temp_bdf = self.blocked_df.copy() 
                blockedest = temp_bdf.groupby('subject')['rev'].count().reset_index().sort_values(by='rev',ascending=False).iloc[0:5]
                blockedest = blockedest.to_dict(orient='list')
                await ws.send_text(json.dumps(blockedest))
            await asyncio.sleep(self.output_interval)

    
    async def  on_connect(self, ws):
        await ws.accept()
        stop = asyncio.create_task(self.stop_after_n_sec())
        then_pop = asyncio.create_task(self.pop_em_over())
        assemble = asyncio.create_task(self.assemble_blocked_rows())
        track= asyncio.create_task(self.track_the_blockedest(ws))
        start = asyncio.create_task(self.start_the_client())
        await stop
        await then_pop
        await assemble
        await track
        await start


    async def simulate_long_task(self, ws):
        await ws.send_text('start long process')
        await asyncio.sleep(5)
        await ws.send_text('finish long process')
        self.task = None

    async def on_disconnect(self, ws, close_code):
        pass



routes = [
    Route('/', homepage),
    WebSocketRoute('/', Consumer) ]

app = Starlette(debug=True, routes=routes)
