import os
from atproto import Client
from convenient_pickle import *
from atproto import FirehoseSubscribeReposClient, firehose_models, parse_subscribe_repos_message
from atproto import CAR, models
from atproto_client.models.network.bsky.jetstream.subscribe_events import Commit
from atproto import AsyncJetstreamClient, jetstream_models, models, IdResolver
import time
import asyncio
import contextlib
import queue
import base64
from sortedcontainers import SortedList
import duckdb
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
            let process_data = JSON.parse(data)

            if(process_data.type === "blocked_table"){
                const docdiv = document.getElementById("blocked_table");
                docdiv.innerHTML = "";

                let data_table = `
                    <table border=1>
                        <tr>
                            <th> Blocked Person </th>
                            <th> Number of Blocks </th>
                            <th> Post </th>
                            <th> Time of Post </th>
                        </tr>
                `
                for (let j = 0; j < process_data.subject.length; j++){
                    const did_link = "<a href='" + process_data.profile_url[j] + "'>" + process_data.subject[j] + "</a>"
                    const post_cell = process_data.post_url[j]
                        ? "<a href='" + process_data.post_url[j] + "'>" + process_data.post_rkey[j] + "</a>"
                        : ""
                    const post_time = process_data.post_time[j] || ""
                    data_table += "<tr> <td>" + did_link + "</td>" + "<td>" + process_data.rev[j] + "</td>" + "<td>" + post_cell + "</td> <td>" + post_time + "</td> </tr>"
                }
                data_table += "</table>"
                docdiv.innerHTML = data_table
            }
        };
    });
    </script>
</head>
<body>
    <div id="blocked_table"></div>
</body>
</html>
""" % (SERVER_PORT)

def homepage(request):
    return HTMLResponse(index_str)

#Needed to use the jetstream us-west on account of te default base_uri not working for some reason.
client = AsyncJetstreamClient(base_uri="wss://jetstream.us-west.bsky.network/xrpc", params={'kinds': ['commit']})

msg = asyncio.Queue()
outdict = dict()
messagecount = 0
output_interval = 1
finished = False
important_items = ['follow', 'post', 'repost', 'block']

db = duckdb.connect(':memory:')
db.execute("""
    CREATE TABLE blocked (
        collection VARCHAR,
        did VARCHAR,
        url VARCHAR,
        operation VARCHAR,
        rev VARCHAR,
        rkey VARCHAR,
        seq BIGINT,
        subject VARCHAR,
        time TIMESTAMP,
        year INTEGER,
        month INTEGER,
        day INTEGER,
        hour INTEGER,
        minute INTEGER
    )
""")

db.execute("""
    CREATE TABLE posts (
        collection VARCHAR,
        did VARCHAR,
        operation VARCHAR,
        rev VARCHAR,
        rkey VARCHAR,
        seq BIGINT,
        text VARCHAR,
        replydid VARCHAR,
        replypost VARCHAR,
        time TIMESTAMP,
        year INTEGER,
        month INTEGER,
        day INTEGER,
        hour INTEGER,
        minute INTEGER
    )
""")


# Live websocket connections to broadcast to. Populated/depleted by
# Consumer.on_connect / Consumer.on_disconnect.
connected_clients: set = set()


async def on_message_handler(event):
    try:
        await msg.put(event)
    except Exception:
        pass


async def start_the_client():
    await client.start(on_message_handler)


async def pop_em_over():
    global messagecount
    print('popping em over')
    while True:
        message = await msg.get()
        if message is None:
            break
        messagecount += 1
        splitmessage = message.collection.split('.')[-1]
        if splitmessage not in outdict.keys() and splitmessage.lower() in important_items:
            outdict[splitmessage] = asyncio.Queue()
            await outdict[splitmessage].put(message)
        elif splitmessage in outdict.keys():
            await outdict[splitmessage].put(message)


async def assemble_blocked_rows():
    while True:
        q = outdict.get('block')
        if q is None:
            if finished:
                break
            await asyncio.sleep(.05)
            continue
        try:
            blocking = await asyncio.wait_for(q.get(), timeout=0.5)
        except asyncio.TimeoutError:
            if finished and q.empty():
                break
            continue

        if blocking.record is not None:
            date = datetime.fromisoformat(blocking.time)
            url = f"<a href='https://bsky.app/profile/{blocking.did}'>https://bsky.app/profile/{blocking.did}</a>"
            db.execute(
                """
                INSERT INTO blocked
                (collection, did, url, operation, rev, rkey, seq, subject, time, year, month, day, hour, minute)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    blocking.collection,
                    blocking.did,
                    url,
                    blocking.operation,
                    blocking.rev,
                    blocking.rkey,
                    blocking.seq,
                    blocking.record.subject,
                    date,
                    date.year,
                    date.month,
                    date.day,
                    date.hour,
                    date.minute,
                ]
            )

async def assemble_post_rows():
    while True:
        q = outdict.get('post')
        if q is None: 
            if finished: 
                break
            await asyncio.sleep(.05)
            continue
        try:
            posting = await asyncio.wait_for(q.get(), timeout=0.5)
        except asyncio.TimeoutError:
            if finished and q.empty():
                break
            continue

        if posting.record is not None:
            try: 
                replydid = posting.record.reply.parent.uri.split('/')[2]
                replypost = posting.record.reply.parent.uri.split('/')[4]
            except:
                replydid = None
                replypost = None
            date = datetime.fromisoformat(posting.time)
            db.execute(
                """
                INSERT INTO posts
                (collection, did, operation, rev, rkey, seq, replydid, replypost, time, year, month, day, hour, minute)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    posting.collection,
                    posting.did,
                    posting.operation,
                    posting.rev,
                    posting.rkey,
                    posting.seq,
                    replydid,
                    replypost,
                    date,
                    date.year,
                    date.month,
                    date.day,
                    date.hour,
                    date.minute,
                ]
            )

async def broadcast_blocked_data():
    while not finished:
        count = db.execute("SELECT COUNT(*) FROM blocked").fetchone()[0]
        if count > 0:
            rows = db.execute("""
                WITH top_blocked AS (
                    SELECT subject, COUNT(rev) AS rev, MIN(time) AS min_time
                    FROM blocked
                    GROUP BY subject
                    ORDER BY rev DESC
                    LIMIT 5
                ),
                candidates AS (
                    SELECT
                        b.subject,
                        b.rev,
                        b.min_time,
                        p.did      AS post_did,
                        p.rkey     AS post_rkey,
                        p.replydid,
                        p.replypost,
                        p.time     AS post_time,
                        ROW_NUMBER() OVER (
                            PARTITION BY b.subject
                            ORDER BY
                                -- priority 0: posts before this subject's
                                -- own min block time, latest first.
                                -- priority 1 (only reached if no post
                                -- qualifies for priority 0): all remaining
                                -- posts, earliest first.
                                CASE WHEN p.time < b.min_time THEN 0 ELSE 1 END,
                                CASE WHEN p.time < b.min_time
                                     THEN -epoch(p.time)
                                     ELSE epoch(p.time)
                                END
                        ) AS rn
                    FROM top_blocked b
                    LEFT JOIN posts p ON p.did = b.subject
                )
                SELECT * EXCLUDE (rn)
                FROM candidates
                WHERE rn = 1
                ORDER BY rev DESC
            """).fetchall()

            payload = json.dumps({
                'type': 'blocked_table',
                'subject': [r[0] for r in rows],
                'rev': [r[1] for r in rows],
                'profile_url': [f"https://bsky.app/profile/{r[0]}" for r in rows],
                'post_rkey': [r[4] for r in rows],
                'post_url': [
                    f"https://bsky.app/profile/{r[0]}/post/{r[4]}" if r[4] is not None else None
                    for r in rows
                ],
                'post_time': [
                    r[7].strftime('%m-%d %H:%M') if r[7] is not None else None
                    for r in rows
                ],
            })

            dead = set()
            for ws in connected_clients:
                try:
                    await ws.send_text(payload)
                except Exception:
                    dead.add(ws)
            connected_clients.difference_update(dead)
        await asyncio.sleep(output_interval)


@contextlib.asynccontextmanager
async def lifespan(app: Starlette):
    app.state.client_task = asyncio.create_task(start_the_client())
    app.state.pop_task = asyncio.create_task(pop_em_over())
    app.state.assemble_blocked_task = asyncio.create_task(assemble_blocked_rows())
    app.state.assemble_post_task = asyncio.create_task(assemble_post_rows())
    app.state.broadcast_task = asyncio.create_task(broadcast_blocked_data())
    yield
    # Shutdown
    global finished
    finished = True
    await client.stop()
    await msg.put(None)
    for task in (
        app.state.client_task,
        app.state.pop_task,
        app.state.assemble_blocked_task,
        app.state.assemble_post_task,
        app.state.broadcast_task,
    ):
        task.cancel()


class Consumer(WebSocketEndpoint):
    encoding = 'text'

    async def on_connect(self, ws):
        await ws.accept()
        connected_clients.add(ws)

    async def on_disconnect(self, ws, close_code):
        connected_clients.discard(ws)


routes = [
    Route('/', homepage),
    WebSocketRoute('/', Consumer)]

app = Starlette(debug=True, routes=routes, lifespan=lifespan)