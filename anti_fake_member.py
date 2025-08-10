import asyncio
import time
import logging
from dataclasses import dataclass
from typing import List, Optional, Tuple
from collections import defaultdict

import aiosqlite
from telethon import TelegramClient
from telethon.tl.types import User

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
log = logging.getLogger('anti_fake')
API_ID = ...
API_HASH = ...
SESSION_USER = 'anti_fake_user'
SESSION_BOT = 'anti_fake_bot'
DB_PATH = 'telethon_joins.db'
@dataclass
class JoinRecord:
    chat_id: int
    user_id: int
    username: Optional[str]
    ts: float
async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            'CREATE TABLE IF NOT EXISTS joins (chat_id INTEGER, user_id INTEGER, username TEXT, joined_ts REAL, PRIMARY KEY(chat_id, user_id, joined_ts))'
        )
        await db.commit()
async def save_join_record(rec: JoinRecord) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT OR IGNORE INTO joins(chat_id, user_id, username, joined_ts) VALUES(?,?,?,?)',
                         (rec.chat_id, rec.user_id, rec.username, rec.ts))
        await db.commit()
async def fetch_joins(chat_id: int) -> List[JoinRecord]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('SELECT chat_id, user_id, username, joined_ts FROM joins WHERE chat_id=? ORDER BY joined_ts ASC', (chat_id,)) as cur:
            rows = await cur.fetchall()
    return [JoinRecord(chat_id=r[0], user_id=r[1], username=r[2], ts=r[3]) for r in rows]

async def scan_history_for_joins(client: TelegramClient, chat_id: str, limit: int = 2000) -> int:
    found = 0
    try:
        if isinstance(chat_id, str) and not chat_id.lstrip('-').isdigit():
            try:
                ent = await client.get_entity(chat_id)
                target_chat_id = int(getattr(ent, 'id'))
            except Exception:
                target_chat_id = 0
        else:
            target_chat_id = int(chat_id)
        async for msg in client.iter_messages(chat_id, limit=limit):
            action = getattr(msg, 'action', None)
            if not action:
                continue
            ts = msg.date.timestamp() if getattr(msg, 'date', None) else time.time()
            user_ids: List[int] = []
            if getattr(action, 'users', None):
                for u in action.users:
                    if isinstance(u, int):
                        user_ids.append(int(u))
                    elif getattr(u, 'id', None):
                        user_ids.append(int(u.id))
            elif getattr(action, 'user_id', None):
                try:
                    user_ids.append(int(action.user_id))
                except Exception:
                    pass
            elif getattr(action, 'user_ids', None):
                for u in action.user_ids:
                    try:
                        user_ids.append(int(u))
                    except Exception:
                        pass
            else:
                fid = getattr(msg, 'from_id', None)
                if fid is not None:
                    try:
                        if hasattr(fid, 'user_id'):
                            user_ids.append(int(fid.user_id))
                        else:
                            user_ids.append(int(fid))
                    except Exception:
                        pass
            for uid in user_ids:
                username = None
                try:
                    ent = await client.get_entity(uid)
                    username = getattr(ent, 'username', None)
                except Exception:
                    username = None
                rec = JoinRecord(chat_id=target_chat_id, user_id=int(uid), username=username, ts=ts)
                await save_join_record(rec)
                found += 1
    except Exception as e:
        log.exception('Error scanning history: %s', e)
    return found

def detect_waves_from_timestamps(timestamps: List[float], window_seconds: int, threshold: int) -> List[Tuple[int, int, int]]:
    if not timestamps:
        return []
    timestamps = sorted(timestamps)
    n = len(timestamps)
    waves = []
    j = 0
    for i in range(n):
        if j < i:
            j = i
        while j < n and timestamps[j] - timestamps[i] <= window_seconds:
            j += 1
        count = j - i
        if count >= threshold:
            waves.append((i, j - 1, count))
    merged = []
    for s, e, _ in waves:
        if not merged or s > merged[-1][1] + 1:
            merged.append((s, e, e - s + 1))
        else:
            ns = merged[-1][0]
            ne = max(merged[-1][1], e)
            merged[-1] = (ns, ne, ne - ns + 1)
    return merged

async def analyze_chat_waves(chat_id: int, window_seconds: int = 30, threshold: int = 5) -> None:
    joins = await fetch_joins(chat_id)
    if not joins:
        print('No join records for chat', chat_id)
        return
    timestamps = [float(j.ts) for j in joins]
    waves = detect_waves_from_timestamps(timestamps, window_seconds, threshold)
    if not waves:
        print('No waves detected for chat', chat_id)
        return
    print(f'Detected {len(waves)} wave(s) in chat {chat_id}:')
    for idx, (s, e, cnt) in enumerate(waves, 1):
        start_ts = timestamps[s]
        end_ts = timestamps[e]
        members = joins[s:e+1]
        names = [m.username or str(m.user_id) for m in members]
        print(f'Wave #{idx}: {cnt} joins from {time.ctime(start_ts)} to {time.ctime(end_ts)}')
        print('Members:', ', '.join(names))


async def cli_loop(client: TelegramClient):
    loop = asyncio.get_event_loop()
    def _inp(prompt: str = '') -> str:
        try:
            return input(prompt)
        except EOFError:
            return 'quit'
    print('Commands: scan_history <chat_id> [limit] | analyze <chat_id> [window_seconds] [threshold] | quit')
    while True:
        cmd = await loop.run_in_executor(None, _inp, 'anti-fake> ')
        if not cmd:
            continue
        parts = cmd.strip().split()
        if not parts:
            continue
        c = parts[0].lower()
        if c in ('quit', 'exit'):
            await client.disconnect()
            break
        elif c == 'scan_history':
            if len(parts) < 2:
                print('Usage: scan_history <chat_id> [limit]')
                continue
            cid = parts[1]
            limit = int(parts[2]) if len(parts) > 2 else 2000
            found = await scan_history_for_joins(client, cid, limit=limit)
            print(f'Scanned history and recorded {found} join events for chat {cid}')
        elif c == 'analyze':
            if len(parts) < 2:
                print('Usage: analyze <chat_id> [window_seconds] [threshold]')
                continue
            cid = int(parts[1])
            window = int(parts[2]) if len(parts) > 2 else 30
            thresh = int(parts[3]) if len(parts) > 3 else 5
            await analyze_chat_waves(cid, window_seconds=window, threshold=thresh)
        else:
            print('Unknown command')


async def start_interactive() -> None:
    print('Telethon Anti-Fake (history waves)')
    print('1) Bot token')
    print('2) User login')
    choice = input('Choose 1 or 2: ').strip()
    if choice == '1':
        token = input('Bot token: ').strip()
        client = TelegramClient(SESSION_BOT, API_ID, API_HASH)
        await client.start(bot_token=token)
    else:
        client = TelegramClient(SESSION_USER, API_ID, API_HASH)
        await client.start()

    await init_db()
    cli_task = asyncio.create_task(cli_loop(client))
    try:
        await client.run_until_disconnected()
    finally:
        cli_task.cancel()
        try:
            await cli_task
        except Exception:
            pass


if __name__ == '__main__':
    try:
        asyncio.run(start_interactive())
    except (KeyboardInterrupt, SystemExit):
        print('Exiting')

