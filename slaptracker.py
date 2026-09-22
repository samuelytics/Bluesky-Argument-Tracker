import os
from atproto import Client
from convenient_pickle import *
from atproto import FirehoseSubscribeReposClient, firehose_models, parse_subscribe_repos_message
from atproto import CAR, models
from atproto_client.models.network.bsky.jetstream.subscribe_events import Commit
from atproto import AsyncJetstreamClient, jetstream_models, models, IdResolver, AsyncClient, AsyncIdResolver
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
                            <th> Display Name </th>
                            <th> Number of Blocks </th>
                            <th> Follows (24h) </th>
                            <th> Post </th>
                            <th> Time of Post </th>
                        </tr>
                `
                for (let j = 0; j < process_data.subject.length; j++){
                    const handle_link = "<a href='" + process_data.profile_url[j] + "'>" + process_data.handle[j] + "</a>"
                    const display_name = process_data.display_name[j] || ""
                    const post_cell = process_data.post_url[j]
                        ? "<a href='" + process_data.post_url[j] + "'>" + process_data.post_rkey[j] + "</a>"
                        : ""
                    const post_time = process_data.post_time[j] || ""
                    data_table += "<tr> <td>" + handle_link + "</td>" + "<td>" + display_name + "</td>" + "<td>" + process_data.rev[j] + "</td>" + "<td>" + process_data.recent_follows[j] + "</td>" + "<td>" + post_cell + "</td> <td>" + post_time + "</td> </tr>"
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
# profile and own posts (and their reply/quote counts) at broadcast time.
bsky_public_client = AsyncClient(base_url="https://public.api.bsky.app")

# Resolves a DID to its PDS endpoint, so we can read that account's raw
# app.bsky.graph.follow records (the AppView doesn't expose follow timing).
id_resolver = AsyncIdResolver()

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

async def count_recent_follows(did, hours=24):
    """Count did's outgoing app.bsky.graph.follow records created in the last `hours`,
    read straight from their PDS (the AppView doesn't expose follow timestamps)."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    try:
        did_doc = await id_resolver.did.resolve(did)
    except Exception:
        did_doc = None
    pds_endpoint = did_doc.get_pds_endpoint() if did_doc else None
    if not pds_endpoint:
        return 0

    count = 0
    cursor = None
    list_records_url = f"{pds_endpoint}/xrpc/com.atproto.repo.listRecords"
    try:
        async with aiohttp.ClientSession() as session:
            while True:
                params = {
                    'repo': did,
                    'collection': 'app.bsky.graph.follow',
                    'limit': 100,
                    'reverse': 'true',
                }
                if cursor:
                    params['cursor'] = cursor
                async with session.get(list_records_url, params=params) as resp:
                    if resp.status != 200:
                        break
                    data = await resp.json()

                records = data.get('records', [])
                if not records:
                    break

                stop = False
                for record in records:
                    created_at_str = record.get('value', {}).get('createdAt')
                    if not created_at_str:
                        continue
                    try:
                        created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                    except ValueError:
                        continue
                    if created_at < cutoff:
                        stop = True
                        break
                    count += 1

                cursor = data.get('cursor')
                if stop or not cursor:
                    break
    except Exception:
        pass
    return count

async def fetch_account_data(did):
    """Gather everything the table needs about a blocked account: handle,
    display name, recent follow count, and the post that likely set off the
    blocks -- all looked up concurrently."""
    profile_result, follow_count, triggering_post = await asyncio.gather(
        bsky_public_client.get_profile(actor=did),
        count_recent_follows(did),
        find_triggering_post(did),
        return_exceptions=True,
    )

    if isinstance(profile_result, Exception):
        handle, display_name = None, None
    else:
        handle, display_name = profile_result.handle, profile_result.display_name

    if isinstance(follow_count, Exception):
        follow_count = 0

    if isinstance(triggering_post, Exception):
        triggering_post = None

    return {
        'handle': handle,
        'display_name': display_name,
        'recent_follows': follow_count,
        'triggering_post': triggering_post,
    }

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
            account_data = await asyncio.gather(
                *(fetch_account_data(did) for did in subjects)
            )

            handles = [a['handle'] or s for s, a in zip(subjects, account_data)]
            display_names = [a['display_name'] for a in account_data]
            recent_follows = [a['recent_follows'] for a in account_data]
            post_rkeys = [
                a['triggering_post'][0] if a['triggering_post'] else None
                for a in account_data
            ]
            post_times = [
                a['triggering_post'][1] if a['triggering_post'] else None
                for a in account_data
            ]

            payload = json.dumps({
                'type': 'blocked_table',
                'subject': subjects,
                'handle': handles,
                'display_name': display_names,
                'rev': revs,
                'recent_follows': recent_follows,
                'profile_url': [f"https://bsky.app/profile/{h}" for h in handles],
                'post_rkey': post_rkeys,
                'post_url': [
                    f"https://bsky.app/profile/{h}/post/{rkey}" if rkey is not None else None
                    for h, rkey in zip(handles, post_rkeys)
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