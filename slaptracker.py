import os
from atproto import Client
from convenient_pickle import *
from atproto import FirehoseSubscribeReposClient, firehose_models, parse_subscribe_repos_message
from atproto import CAR, models
from atproto_client.models.network.bsky.jetstream.subscribe_events import Commit
from atproto import AsyncJetstreamClient, jetstream_models, models, IdResolver, AsyncClient
import time
import asyncio
import contextlib
import queue
import base64
from sortedcontainers import SortedList
import duckdb
from datetime import datetime, timedelta, timezone
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

# Unauthenticated public AppView client, used to look up a blocked account's
# own posts (and their reply/quote counts) at broadcast time.
bsky_public_client = AsyncClient(base_url="https://public.api.bsky.app")

msg = asyncio.Queue()
outdict = dict()
messagecount = 0
output_interval = 60
finished = False
important_items = ['follow', 'repost', 'block']

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

async def find_triggering_post(did):
    """Look up did's own posts from the last 24h and return the (rkey, created_at)
    of whichever has the highest reply_count + 2 * quote_count, or None."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    try:
        response = await bsky_public_client.get_author_feed(
            actor=did, filter='posts_no_replies', limit=100
        )
    except Exception:
        return None

    best_rkey = None
    best_time = None
    best_score = -1
    for item in response.feed:
        post = item.post
        try:
            created_at = datetime.fromisoformat(post.record.created_at.replace('Z', '+00:00'))
        except (AttributeError, ValueError):
            continue
        if created_at < cutoff:
            continue
        score = (post.reply_count or 0) + 2 * (post.quote_count or 0)
        if score > best_score:
            best_score = score
            best_rkey = post.uri.split('/')[-1]
            best_time = created_at

    if best_rkey is None:
        return None
    return best_rkey, best_time

async def broadcast_blocked_data():
    while not finished:
        count = db.execute("SELECT COUNT(*) FROM blocked").fetchone()[0]
        if count > 0:
            top_blocked = db.execute("""
                SELECT subject, COUNT(rev) AS rev
                FROM blocked
                GROUP BY subject
                ORDER BY rev DESC
                LIMIT 5
            """).fetchall()

            subjects = [r[0] for r in top_blocked]
            revs = [r[1] for r in top_blocked]
            triggering_posts = await asyncio.gather(
                *(find_triggering_post(did) for did in subjects)
            )
            post_rkeys = [tp[0] if tp else None for tp in triggering_posts]
            post_times = [tp[1] if tp else None for tp in triggering_posts]

            payload = json.dumps({
                'type': 'blocked_table',
                'subject': subjects,
                'rev': revs,
                'profile_url': [f"https://bsky.app/profile/{s}" for s in subjects],
                'post_rkey': post_rkeys,
                'post_url': [
                    f"https://bsky.app/profile/{s}/post/{rkey}" if rkey is not None else None
                    for s, rkey in zip(subjects, post_rkeys)
                ],
                'post_time': [
                    t.strftime('%m-%d %H:%M') if t is not None else None
                    for t in post_times
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