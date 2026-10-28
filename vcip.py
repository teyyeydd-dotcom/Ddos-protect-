"""
𓆩🎙️𓆪 VC MEMBER JOIN BOT — v19
- Sessions stored as STRING (never deleted, no re-add)
- Only: /join /addsession /removesession /listsessions /protact /status /leaveall
- Hidden progress animation in /join
- Alternate mic: odd joins = UNMUTE (green), even joins = MUTE (red)
- /protact <chat_id> <count> — auto VC watcher, alternate mute/unmute
- /leaveall — leave all VCs and stop watchers
- /status — live status of all watchers
- Crash-proof: no silent off, no auto restart, no auto-leave
- FIXED: ENTITY_BOUNDS_INVALID (plain text status)
- FIXED: users.GetFullUser flood wait (no_sync=True on PyTgCalls)
- FIXED: FLOOD_WAIT_X on phone.JoinGroupCall (auto retry + batch rest)
- FIXED: 'NoneType' object has no attribute 'write' (safe clone stop)
- FIXED: Base sessions NEVER leave — only /leaveall /stopall
- Count limit: 1-200
"""
import sys
if hasattr(sys.stdout, "reconfigure"):
    try: sys.stdout.reconfigure(encoding="utf-8")
    except Exception: pass
if hasattr(sys.stderr, "reconfigure"):
    try: sys.stderr.reconfigure(encoding="utf-8")
    except Exception: pass

import os, re, json, logging, asyncio, time, random, warnings, subprocess, signal
from typing import Dict, List, Optional, Tuple
from datetime import datetime

warnings.filterwarnings("ignore", category=DeprecationWarning)

from pyrogram import Client, filters as pyro_filters
from pyrogram.types import Message

# Pyrogram peer-id fix
from pyrogram import utils as _pyro_utils
def _get_peer_type_compat(peer_id: int) -> str:
    v = str(peer_id)
    if not v.startswith("-"): return "user"
    if v.startswith("-100"): return "channel"
    return "chat"
_pyro_utils.get_peer_type = _get_peer_type_compat

from pyrogram.errors import (
    FloodWait, UserNotParticipant, UserAlreadyParticipant,
    InviteHashExpired, InviteHashInvalid, AuthKeyUnregistered,
    UserDeactivated, SessionPasswordNeeded
)
from pytgcalls import PyTgCalls, idle
from pytgcalls.types import ExternalMedia, MediaStream
from pytgcalls.types.raw import AudioParameters
from pytgcalls.exceptions import NoActiveGroupCall

# ==================== CONFIG ====================
API_ID = int(os.getenv("API_ID", "30434687"))
API_HASH = os.getenv("API_HASH", "d4adb52774efd1cbbbebdfc1647b4ee9")
BOT_TOKEN = os.getenv("BOT_TOKEN", "9107262141:AAEn5GNcJJ7yBjjlIC_J3eZOXuvh_gDkaOs")
OWNER_ID = int(os.getenv("OWNER_ID", "5498096445"))

BOT_SESSION_NAME = "bot_dhsaster_session"
ACCOUNTS_FILE = "acdhecounts.json"
STATE_FILE = "stdhshate.json"
WATCH_FILE = "watch_targets.json"

AUDIO_PARAMETERS = AudioParameters(bitrate=48000, channels=2)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("pytgcalls").setLevel(logging.WARNING)

bot_app = Client(BOT_SESSION_NAME, api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# ==================== STATE ====================
MULTI_ACCOUNTS: Dict[str, dict] = {}
ACTIVE_ACCOUNT: Optional[str] = None
bot_start_time = time.time()

VC_CLONES: List[dict] = []
WATCHERS: Dict[int, dict] = {}

OFF_MODE = False
OFF_UNTIL = 0

PROGRESS_FRAMES = ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]
HIDDEN_STEPS = [
    "Initializing",
    "Connecting",
    "Syncing",
    "Verifying",
    "Processing",
    "Finalizing",
]

MAX_COUNT = 200


# ==================== SAFE SEND ====================
async def safe_send(message_or_client, text, reply_to=None, is_reply=True):
    try:
        if is_reply and hasattr(message_or_client, "reply"):
            return await message_or_client.reply(text, parse_mode=None)
        else:
            return await message_or_client.send_message(
                chat_id=OWNER_ID if not reply_to else reply_to,
                text=text,
                parse_mode=None,
            )
    except Exception as e:
        logger.debug(f"safe_send fail: {e}")
        return None


# ==================== SAFE CLONE STOP (fix NoneType.write) ====================
async def safe_stop_client(client, label: str = "clone"):
    """
    Safely stop a Pyrogram client without raising NoneType.write error.
    Never raises. Never touches base sessions.
    """
    if client is None:
        return
    try:
        # First try graceful stop
        try:
            if hasattr(client, "is_connected") and client.is_connected:
                await client.stop()
                return
        except Exception as e:
            logger.debug(f"safe_stop_client stop() {label}: {e}")

        # Fallback: try disconnect
        try:
            if hasattr(client, "disconnect"):
                await client.disconnect()
        except Exception as e:
            logger.debug(f"safe_stop_client disconnect() {label}: {e}")

        # Fallback: try to close session directly
        try:
            if hasattr(client, "session") and client.session:
                if hasattr(client.session, "stop"):
                    await client.session.stop()
                elif hasattr(client.session, "close"):
                    client.session.close()
        except Exception as e:
            logger.debug(f"safe_stop_client session {label}: {e}")

    except Exception as e:
        logger.debug(f"safe_stop_client outer {label}: {e}")


async def safe_stop_vc(vc, chat_id: int = None, label: str = "clone"):
    """
    Safely leave a PyTgCalls call without raising.
    """
    if vc is None:
        return
    try:
        if chat_id is not None:
            try:
                await vc.leave_call(int(chat_id))
            except Exception as e:
                logger.debug(f"safe_stop_vc leave_call {label}: {e}")
        else:
            try:
                if hasattr(vc, "leave_all_calls"):
                    await vc.leave_all_calls()
            except Exception as e:
                logger.debug(f"safe_stop_vc leave_all {label}: {e}")
    except Exception as e:
        logger.debug(f"safe_stop_vc outer {label}: {e}")


# ==================== PERSISTENCE ====================
def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump({"active_account": ACTIVE_ACCOUNT}, f)
    except Exception as e:
        logger.error(f"State save: {e}")


def load_state():
    global ACTIVE_ACCOUNT
    try:
        with open(STATE_FILE) as f:
            ACTIVE_ACCOUNT = json.load(f).get("active_account")
    except Exception:
        pass


def save_accounts():
    try:
        data = {}
        for acc_id, acc in MULTI_ACCOUNTS.items():
            data[acc_id] = {
                "session_string": acc.get("session_string", ""),
                "first_name": acc.get("first_name", ""),
                "user_id": acc.get("user_id", ""),
                "username": acc.get("username", ""),
                "added_at": acc.get("added_at", time.time()),
            }
        with open(ACCOUNTS_FILE, "w") as f:
            json.dump(data, f, indent=2)
        logger.info(f"💾 Saved {len(data)} accounts")
    except Exception as e:
        logger.error(f"Accounts save: {e}")


def load_accounts():
    global MULTI_ACCOUNTS
    MULTI_ACCOUNTS = {}
    try:
        if not os.path.exists(ACCOUNTS_FILE):
            return
        with open(ACCOUNTS_FILE) as f:
            data = json.load(f)
        for acc_id, info in data.items():
            sess = info.get("session_string", "")
            if not sess:
                continue
            try:
                client = Client(
                    f"acc_{info.get('user_id', acc_id)}",
                    api_id=API_ID, api_hash=API_HASH,
                    session_string=sess,
                    in_memory=True,
                )
                MULTI_ACCOUNTS[acc_id] = {
                    "client": client,
                    "session_string": sess,
                    "first_name": info.get("first_name", ""),
                    "user_id": info.get("user_id", ""),
                    "username": info.get("username", ""),
                    "added_at": info.get("added_at", time.time()),
                    "vc": None,
                    "in_vc": False,
                    "vc_chats": set(),
                }
                logger.info(f"📂 Loaded account: {info.get('first_name')}")
            except Exception as e:
                logger.error(f"Load {acc_id}: {e}")
    except Exception as e:
        logger.error(f"Accounts load: {e}")


def save_watchers():
    try:
        data = {}
        for cid, w in WATCHERS.items():
            data[str(cid)] = {
                "chat_id": cid,
                "count": w.get("count", 1),
                "acc_id": w.get("acc_id", ""),
                "active": w.get("active", True),
                "created_at": w.get("created_at", time.time()),
                "total_joins": w.get("total_joins", 0),
                "total_fails": w.get("total_fails", 0),
                "vc_on_count": w.get("vc_on_count", 0),
                "vc_off_count": w.get("vc_off_count", 0),
            }
        with open(WATCH_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.error(f"Watchers save: {e}")


def load_watchers():
    try:
        if not os.path.exists(WATCH_FILE):
            return
        with open(WATCH_FILE) as f:
            data = json.load(f)
        for k, v in data.items():
            try:
                cid = int(v.get("chat_id", k))
            except Exception:
                continue
            WATCHERS[cid] = {
                "chat_id": cid,
                "count": max(1, min(MAX_COUNT, int(v.get("count", 1)))),
                "acc_id": v.get("acc_id", ""),
                "active": bool(v.get("active", True)),
                "task": None,
                "joined": False,
                "created_at": v.get("created_at", time.time()),
                "vc_online": False,
                "last_check": 0,
                "last_event": "Loaded from disk",
                "last_event_time": time.time(),
                "total_joins": v.get("total_joins", 0),
                "total_fails": v.get("total_fails", 0),
                "vc_on_count": v.get("vc_on_count", 0),
                "vc_off_count": v.get("vc_off_count", 0),
                "status_msg_id": None,
            }
        logger.info(f"👁 Loaded {len(WATCHERS)} watchers")
    except Exception as e:
        logger.error(f"Watchers load: {e}")


# ==================== HELPERS ====================
def is_owner(uid): return uid == OWNER_ID


async def _owner_filter(_, __, m):
    return bool(m.from_user and m.from_user.id == OWNER_ID)


def owner_only():
    return pyro_filters.create(_owner_filter)


def get_uptime():
    s = int(time.time() - bot_start_time)
    d, h, m = s // 86400, (s % 86400) // 3600, (s % 3600) // 60
    if d: return f"{d}d {h}h {m}m"
    if h: return f"{h}h {m}m"
    if m: return f"{m}m"
    return f"{s}s"


def fmt_time(ts):
    try:
        return datetime.fromtimestamp(ts).strftime("%H:%M:%S")
    except Exception:
        return "?"


def fmt_ago(ts):
    if not ts:
        return "never"
    s = int(time.time() - ts)
    if s < 60:
        return f"{s}s ago"
    if s < 3600:
        return f"{s // 60}m ago"
    if s < 86400:
        return f"{s // 3600}h ago"
    return f"{s // 86400}d ago"


def pick_account():
    if ACTIVE_ACCOUNT and ACTIVE_ACCOUNT in MULTI_ACCOUNTS:
        return ACTIVE_ACCOUNT
    if MULTI_ACCOUNTS:
        return next(iter(MULTI_ACCOUNTS.keys()))
    return None


def set_event(chat_id: int, event: str):
    w = WATCHERS.get(chat_id)
    if w:
        w["last_event"] = event
        w["last_event_time"] = time.time()


async def ensure_base_connected(acc_id: str) -> bool:
    """
    Make sure base account client is connected.
    NEVER stops or removes it — only reconnects if disconnected.
    """
    acc = MULTI_ACCOUNTS.get(acc_id)
    if not acc:
        return False
    client = acc.get("client")
    if not client:
        return False
    try:
        if not client.is_connected:
            await client.start()
        return True
    except Exception as e:
        logger.error(f"Reconnect {acc_id}: {e}")
        return False


# ==================== STATUS TEXT ====================
def build_status_text(chat_id: int) -> str:
    w = WATCHERS.get(chat_id)
    if not w:
        return f"❌ No watcher for {chat_id}"

    online = "🟢 ONLINE" if w.get("vc_online") else "🔴 OFFLINE"
    joined = "✅ JOINED" if w.get("joined") else "⏳ WAITING"
    active = "🛡 ACTIVE" if w.get("active") else "⏸ STOPPED"

    uptime = fmt_ago(w.get("created_at", time.time()))
    last_chk = fmt_ago(w.get("last_check", 0))
    last_evt_time = fmt_time(w.get("last_event_time", 0))

    acc_id = w.get("acc_id", "?")
    acc_name = "?"
    if acc_id in MULTI_ACCOUNTS:
        acc_name = MULTI_ACCOUNTS[acc_id].get("first_name", "?")

    txt = (
        f"🛡 Watcher Status — {chat_id}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"State: {active}\n"
        f"VC: {online}\n"
        f"Join: {joined}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 Account: {acc_name} ({acc_id})\n"
        f"🔢 Target count: {w.get('count', 1)}\n"
        f"🎤 Mode: alternate (1=unmute, 2=mute)\n"
        f"⏱ Watcher uptime: {uptime}\n"
        f"🔄 Last check: {last_chk}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Stats:\n"
        f"  ✅ Total joins: {w.get('total_joins', 0)}\n"
        f"  ❌ Total fails: {w.get('total_fails', 0)}\n"
        f"  🟢 VC ON seen: {w.get('vc_on_count', 0)}\n"
        f"  🔴 VC OFF seen: {w.get('vc_off_count', 0)}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📝 Last event ({last_evt_time}):\n"
        f"  {w.get('last_event', 'none')}\n"
    )
    return txt


def build_all_status_text() -> str:
    if not WATCHERS:
        return "📭 No watchers active.\n\nUse /protact <chat_id> <count>"
    txt = f"🛡 All Watchers ({len(WATCHERS)})\n━━━━━━━━━━━━━━━━━━━━\n"
    for cid, w in WATCHERS.items():
        online = "🟢" if w.get("vc_online") else "🔴"
        joined = "✅" if w.get("joined") else "⏳"
        active = "🛡" if w.get("active") else "⏸"
        txt += (
            f"{active} {online} {joined} {cid} — count {w.get('count', 1)}\n"
            f"   joins: {w.get('total_joins', 0)} | fails: {w.get('total_fails', 0)}\n"
            f"   last: {w.get('last_event', 'none')[:40]}\n\n"
        )
    txt += "━━━━━━━━━━━━━━━━━━━━\n"
    txt += "Detail: /status <chat_id>"
    return txt[:4000]


async def push_status_update(chat_id: int):
    w = WATCHERS.get(chat_id)
    if not w:
        return
    msg_id = w.get("status_msg_id")
    if not msg_id:
        return
    try:
        await bot_app.edit_message_text(
            chat_id=OWNER_ID,
            message_id=msg_id,
            text=build_status_text(chat_id),
            parse_mode=None,
        )
    except Exception:
        pass


# ==================== HIDDEN PROGRESS ====================
async def hidden_progress(status_msg, duration: int, header: str = "⏳ Please wait"):
    end_time = time.time() + duration
    i = 0
    while time.time() < end_time:
        frame = PROGRESS_FRAMES[i % len(PROGRESS_FRAMES)]
        step = HIDDEN_STEPS[i % len(HIDDEN_STEPS)]
        bar_len = 10
        filled = int((i % (bar_len + 1)))
        bar = "█" * filled + "░" * (bar_len - filled)
        text = f"{frame} {header}...\n\n{bar}\n{step}"
        try:
            await status_msg.edit_text(text, parse_mode=None)
        except Exception:
            pass
        await asyncio.sleep(0.5)
        i += 1


# ==================== CHAT JOIN ====================
async def join_chat_for_account(account_id: str, input_str: str) -> Tuple[bool, Optional[int], str]:
    acc = MULTI_ACCOUNTS.get(account_id)
    if not acc:
        return False, None, "Account not found"
    client = acc["client"]
    name = acc.get("first_name", "Unknown")
    input_str = str(input_str).strip()

    try:
        if not client.is_connected:
            await client.start()

        if re.fullmatch(r"-?\d+", input_str):
            chat_id = int(input_str)
            try:
                chat = await client.get_chat(chat_id)
                return True, int(chat.id), f"{name} already in chat"
            except UserNotParticipant:
                try:
                    chat = await client.join_chat(chat_id)
                    return True, int(chat.id), f"{name} joined"
                except Exception as e:
                    return False, None, f"{name} join fail: {str(e)[:100]}"
            except Exception as e:
                return False, None, f"{name} resolve fail: {str(e)[:100]}"

        if input_str.startswith("@") or re.match(r"^https?://t\.me/[A-Za-z0-9_]+(?:/.*)?$", input_str):
            target = input_str
            if "t.me/" in target:
                tail = target.split("t.me/", 1)[1].split("?", 1)[0].strip("/")
                if tail and not tail.startswith(("+", "joinchat/")):
                    target = "@" + tail.split("/", 1)[0].lstrip("@")
            try:
                chat = await client.get_chat(target)
                return True, int(chat.id), f"{name} already in chat"
            except UserNotParticipant:
                try:
                    chat = await client.join_chat(target)
                    return True, int(chat.id), f"{name} joined"
                except Exception as e:
                    return False, None, f"{name} join fail: {str(e)[:100]}"
            except Exception as e:
                return False, None, f"{name} resolve fail: {str(e)[:100]}"

        if re.match(r"^https?://(?:t|telegram)\.me/(?:\+|joinchat/)", input_str):
            try:
                chat = await client.join_chat(input_str)
                return True, int(chat.id), f"{name} joined via invite"
            except Exception as e:
                t = str(e).upper()
                if "INVITE_REQUEST_SENT" in t:
                    return False, None, f"{name} request sent"
                if "USER_ALREADY_PARTICIPANT" in t or isinstance(e, UserAlreadyParticipant):
                    return False, None, f"{name} already joined"
                if isinstance(e, InviteHashExpired):
                    return False, None, f"{name} invite expired"
                if isinstance(e, InviteHashInvalid):
                    return False, None, f"{name} invalid invite"
                return False, None, f"{name} invite fail: {str(e)[:100]}"

        return False, None, f"{name} unsupported input"
    except FloodWait as e:
        return False, None, f"{name} flood {e.x}s"
    except Exception as e:
        logger.exception("Join fail")
        return False, None, f"{name} error: {str(e)[:100]}"


# ==================== BASE VC JOIN ====================
async def join_vc_for_account(account_id: str, chat_id: int) -> Tuple[bool, str]:
    acc = MULTI_ACCOUNTS.get(account_id)
    if not acc:
        return False, "Account not found"
    client = acc["client"]
    name = acc.get("first_name", "Unknown")
    chat_id = int(chat_id)

    try:
        if not client.is_connected:
            await client.start()

        chat = await client.get_chat(chat_id)
        chat_id = int(chat.id)

        vc_chats = acc.setdefault("vc_chats", set())
        if chat_id in vc_chats:
            acc["in_vc"] = True
            return True, f"{name} already in VC"

        vc = acc.get("vc")
        if vc is None:
            try:
                vc = PyTgCalls(client, no_sync=True)
            except TypeError:
                vc = PyTgCalls(client)
            await vc.start()
            acc["vc"] = vc

        await vc.play(chat_id, MediaStream(ExternalMedia.AUDIO, AUDIO_PARAMETERS))
        try:
            await vc.mute(chat_id)
        except Exception:
            pass

        vc_chats.add(chat_id)
        acc["in_vc"] = True
        return True, f"{name} joined VC"
    except NoActiveGroupCall:
        return False, "No active VC"
    except Exception as e:
        low = str(e).lower()
        if "already" in low and "participat" in low:
            acc.setdefault("vc_chats", set()).add(chat_id)
            acc["in_vc"] = True
            return True, f"{name} already in VC"
        logger.exception("VC fail")
        return False, f"{type(e).__name__}: {str(e)[:150]}"


async def join_all_accounts_to_vc(chat_id: int) -> Dict:
    results = {}
    for acc_id in list(MULTI_ACCOUNTS.keys()):
        ok, msg = await join_vc_for_account(acc_id, chat_id)
        results[acc_id] = {"success": ok, "message": msg}
        await asyncio.sleep(0.25)
    return results


# ==================== SAME-SESSION MULTI VC JOIN (PROTECTED) ====================
async def join_same_session_multiple_vc(
    acc_id: str, chat_id: int, count: int
) -> List[dict]:
    """
    Same session string se `count` baar VC join — bina leave kiye.
    ALTERNATE MIC:
      - Join #1, #3, #5 ... → UNMUTE (green mic)
      - Join #2, #4, #6 ... → MUTE   (red mic)

    SAFETY:
      - Base account session is NEVER touched.
      - Clone clients are safely stopped on failure (no NoneType.write).
      - Retry only 2 times on flood (avoid 51s waits).
      - Batch rest 5 joins → 10s.
      - Failures do NOT stop other clones.
    """
    acc = MULTI_ACCOUNTS.get(acc_id)
    if not acc:
        return [{"success": False, "message": "Account not found"}]

    sess = acc.get("session_string", "")
    if not sess:
        return [{"success": False, "message": "No session string"}]

    first_name = acc.get("first_name", "Unknown")
    user_id = acc.get("user_id", "")
    results = []

    # Make sure base account is connected (never stop it!)
    await ensure_base_connected(acc_id)

    # Resolve chat_id ONCE using main account
    real_chat_id = int(chat_id)
    try:
        main_client = acc["client"]
        try:
            peer = await main_client.resolve_peer(real_chat_id)
            if hasattr(peer, "channel_id"):
                real_chat_id = int(f"-100{peer.channel_id}")
            elif hasattr(peer, "chat_id"):
                real_chat_id = int(f"-{peer.chat_id}")
        except Exception:
            pass
    except Exception:
        pass

    gap = 1.0
    joined_since_rest = 0
    REST_EVERY = 5
    REST_TIME = 10.0
    MAX_ATTEMPTS = 2  # reduced from 3 → 2

    for i in range(1, count + 1):
        clone_id = f"{acc_id}_vc{i}_{int(time.time())}"
        should_mute = (i % 2 == 0)
        mode_label = "MUTE 🔴" if should_mute else "UNMUTE 🟢"

        ok = False
        last_err = ""
        clone_client = None
        vc = None

        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                clone_client = Client(
                    f"vcjoin_{user_id}_{i}_{int(time.time())}_{attempt}",
                    api_id=API_ID,
                    api_hash=API_HASH,
                    session_string=sess,
                    in_memory=True,
                )
                await clone_client.start()

                try:
                    vc = PyTgCalls(clone_client, no_sync=True)
                except TypeError:
                    vc = PyTgCalls(clone_client)

                await vc.start()
                await vc.play(
                    real_chat_id,
                    MediaStream(ExternalMedia.AUDIO, AUDIO_PARAMETERS)
                )

                if should_mute:
                    try:
                        await vc.mute(real_chat_id)
                    except Exception:
                        pass
                else:
                    try:
                        await vc.unmute(real_chat_id)
                    except Exception:
                        try:
                            await vc.mute(real_chat_id, False)
                        except Exception:
                            pass

                VC_CLONES.append({
                    "clone_id": clone_id,
                    "client": clone_client,
                    "vc": vc,
                    "chat_id": real_chat_id,
                    "acc_id": acc_id,
                    "index": i,
                    "muted": should_mute,
                })

                results.append({
                    "success": True,
                    "message": f"{first_name} VC#{i} joined ({mode_label})",
                    "clone_id": clone_id,
                    "muted": should_mute,
                })
                logger.info(f"✅ VC#{i} joined for {first_name} in {real_chat_id} — {mode_label}")
                ok = True
                break

            except FloodWait as e:
                wait_s = int(getattr(e, "x", 15)) + 2
                last_err = f"FloodWait {wait_s}s"
                logger.warning(f"⏳ VC#{i} flood {wait_s}s, attempt {attempt}/{MAX_ATTEMPTS}")
                await safe_stop_vc(vc, real_chat_id, f"vc{i}_a{attempt}")
                await safe_stop_client(clone_client, f"clone{i}_a{attempt}")
                clone_client = None
                vc = None
                # Cap wait to 20s max to avoid long hangs
                await asyncio.sleep(min(wait_s, 20))

            except NoActiveGroupCall:
                results.append({
                    "success": False,
                    "message": f"{first_name} VC#{i}: No active VC",
                })
                last_err = "NoActiveGroupCall"
                await safe_stop_vc(vc, real_chat_id, f"vc{i}")
                await safe_stop_client(clone_client, f"clone{i}")
                clone_client = None
                vc = None
                break

            except Exception as e:
                err_str = str(e)
                low = err_str.lower()

                if "already" in low and "participat" in low:
                    results.append({
                        "success": True,
                        "message": f"{first_name} VC#{i} already in VC ({mode_label})",
                        "muted": should_mute,
                    })
                    ok = True
                    last_err = "already"
                    break

                # Handle NoneType.write silently
                if "nonetype" in low and "write" in low:
                    last_err = "socket error (ignored)"
                    logger.debug(f"VC#{i} socket write error (ignored)")
                    await safe_stop_vc(vc, real_chat_id, f"vc{i}_sock")
                    await safe_stop_client(clone_client, f"clone{i}_sock")
                    clone_client = None
                    vc = None
                    continue

                if "FLOOD_WAIT" in err_str.upper() or "flood" in low:
                    last_err = f"flood: {err_str[:80]}"
                    logger.warning(f"⏳ VC#{i} flood, attempt {attempt}/{MAX_ATTEMPTS}")
                    await safe_stop_vc(vc, real_chat_id, f"vc{i}_f")
                    await safe_stop_client(clone_client, f"clone{i}_f")
                    clone_client = None
                    vc = None
                    await asyncio.sleep(15)
                    continue

                logger.error(f"VC#{i} fail: {err_str[:150]}")
                last_err = f"{type(e).__name__}: {err_str[:120]}"
                await safe_stop_vc(vc, real_chat_id, f"vc{i}_err")
                await safe_stop_client(clone_client, f"clone{i}_err")
                clone_client = None
                vc = None
                break

        if not ok and not any(
            r.get("success") and str(r.get("clone_id", "")).startswith(f"{acc_id}_vc{i}_")
            for r in results
        ):
            results.append({
                "success": False,
                "message": f"{first_name} VC#{i}: {last_err}",
            })

        # Batch rest
        joined_since_rest += 1
        if joined_since_rest >= REST_EVERY:
            logger.info(f"😴 Batch rest {REST_TIME}s after {REST_EVERY} joins")
            await asyncio.sleep(REST_TIME)
            joined_since_rest = 0
            gap = 1.0
        else:
            await asyncio.sleep(gap)

    return results


# ==================== LEAVE ALL (ONLY manual) ====================
async def leave_all_for_chat(chat_id: int):
    """
    Leave all CLONES in this chat + base account VC for this chat.
    Base session stays connected — only VC call is left.
    """
    chat_id = int(chat_id)

    for c in list(VC_CLONES):
        if int(c.get("chat_id", 0)) == chat_id:
            await safe_stop_vc(c.get("vc"), chat_id, f"clone_{c.get('clone_id')}")
            await safe_stop_client(c.get("client"), f"clone_{c.get('clone_id')}")
            try:
                VC_CLONES.remove(c)
            except Exception:
                pass

    for acc_id, acc in list(MULTI_ACCOUNTS.items()):
        vc = acc.get("vc")
        if vc:
            try:
                vc_chats = acc.get("vc_chats", set())
                if chat_id in vc_chats:
                    await safe_stop_vc(vc, chat_id, f"base_{acc_id}")
                    vc_chats.discard(chat_id)
                    acc["in_vc"] = bool(vc_chats)
            except Exception:
                pass


async def leave_all_everything():
    """
    Leave all clones + base VCs. Base session stays connected.
    """
    for c in list(VC_CLONES):
        await safe_stop_vc(c.get("vc"), c.get("chat_id"), f"clone_{c.get('clone_id')}")
        await safe_stop_client(c.get("client"), f"clone_{c.get('clone_id')}")
    VC_CLONES.clear()

    for acc_id, acc in list(MULTI_ACCOUNTS.items()):
        vc = acc.get("vc")
        if vc:
            try:
                if hasattr(vc, 'leave_all_calls'):
                    await vc.leave_all_calls()
                elif hasattr(vc, '_calls'):
                    for cid in list(vc._calls.keys()):
                        try:
                            await vc.leave_call(cid)
                        except Exception:
                            pass
                acc["in_vc"] = False
                acc["vc_chats"] = set()
            except Exception:
                pass


async def cleanup_all_vc_clones():
    for c in list(VC_CLONES):
        await safe_stop_vc(c.get("vc"), c.get("chat_id"), f"cleanup_{c.get('clone_id')}")
        await safe_stop_client(c.get("client"), f"cleanup_{c.get('clone_id')}")
    VC_CLONES.clear()


# ==================== VC WATCHER ====================
async def check_vc_active(acc_id: str, chat_id: int) -> bool:
    acc = MULTI_ACCOUNTS.get(acc_id)
    if not acc:
        return False
    client = acc["client"]
    try:
        await ensure_base_connected(acc_id)

        try:
            from pyrogram.raw.functions.channels import GetFullChannel
            from pyrogram.raw.functions.messages import GetFullChat

            chat_id = int(chat_id)
            peer = await client.resolve_peer(chat_id)

            full = None
            try:
                full = await client.invoke(GetFullChat(chat_id=abs(chat_id)))
            except Exception:
                try:
                    full = await client.invoke(GetFullChannel(channel=peer))
                except Exception:
                    full = None

            if full:
                call = getattr(full, "call", None) or getattr(getattr(full, "full_chat", None), "call", None)
                if call:
                    return True
            return False
        except Exception:
            vc = acc.get("vc")
            if vc is None:
                try:
                    vc = PyTgCalls(client, no_sync=True)
                except TypeError:
                    vc = PyTgCalls(client)
                try:
                    await vc.start()
                except Exception:
                    pass
                acc["vc"] = vc
            try:
                if hasattr(vc, "get_call"):
                    call = await vc.get_call(chat_id)
                    return bool(call)
            except Exception:
                pass
            return False
    except Exception as e:
        logger.debug(f"check_vc_active {chat_id}: {e}")
        return False


async def watcher_loop(chat_id: int):
    chat_id = int(chat_id)
    w = WATCHERS.get(chat_id)
    if not w:
        return
    logger.info(f"👁 Watcher started for {chat_id} (count={w['count']})")

    while w.get("active", True):
        try:
            acc_id = w.get("acc_id") or pick_account()
            if not acc_id or acc_id not in MULTI_ACCOUNTS:
                w["last_event"] = "No account available"
                w["last_event_time"] = time.time()
                await push_status_update(chat_id)
                await asyncio.sleep(5)
                continue

            w["acc_id"] = acc_id
            w["last_check"] = time.time()

            # Ensure base account connected
            await ensure_base_connected(acc_id)

            active = await check_vc_active(acc_id, chat_id)
            w["vc_online"] = bool(active)

            if active:
                w["vc_on_count"] = w.get("vc_on_count", 0) + 1
                if not w.get("joined"):
                    set_event(chat_id, f"🟢 VC ONLINE — joining x{w['count']} (alternate mic)")
                    await push_status_update(chat_id)
                    logger.info(f"🎙️ VC active in {chat_id}, joining x{w['count']}")

                    results = await join_same_session_multiple_vc(acc_id, chat_id, w["count"])
                    ok = sum(1 for r in results if r.get("success"))
                    fails = len(results) - ok
                    w["total_joins"] = w.get("total_joins", 0) + ok
                    w["total_fails"] = w.get("total_fails", 0) + fails

                    if ok > 0:
                        w["joined"] = True
                        unmuted = sum(1 for r in results if r.get("success") and not r.get("muted"))
                        muted = sum(1 for r in results if r.get("success") and r.get("muted"))
                        set_event(chat_id, f"✅ JOINED {ok}/{w['count']} (🟢 {unmuted} unmute, 🔴 {muted} mute)")
                        logger.info(f"✅ Watcher joined {ok}/{w['count']} in {chat_id}")
                    else:
                        set_event(chat_id, f"⚠️ Join failed ({fails} fails)")
                        logger.warning(f"⚠️ Watcher join failed for {chat_id}")
                    await push_status_update(chat_id)
                else:
                    if w.get("last_event", "").startswith("✅") is False:
                        set_event(chat_id, "🟢 VC still online (joined)")
                        await push_status_update(chat_id)
            else:
                w["vc_off_count"] = w.get("vc_off_count", 0) + 1
                if w.get("joined"):
                    set_event(chat_id, "🔴 VC OFFLINE — still in call (not leaving)")
                else:
                    set_event(chat_id, "🔴 VC OFFLINE — waiting")
                await push_status_update(chat_id)

            save_watchers()
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Watcher loop error {chat_id}: {e}")
            set_event(chat_id, f"⚠️ Error: {str(e)[:60]}")
            await push_status_update(chat_id)
            await asyncio.sleep(5)


def start_watcher(chat_id: int, count: int, acc_id: str):
    chat_id = int(chat_id)
    old = WATCHERS.get(chat_id)
    if old and old.get("task"):
        try:
            old["task"].cancel()
        except Exception:
            pass

    WATCHERS[chat_id] = {
        "chat_id": chat_id,
        "count": max(1, min(MAX_COUNT, int(count))),
        "acc_id": acc_id,
        "active": True,
        "task": None,
        "joined": False,
        "created_at": time.time(),
        "vc_online": False,
        "last_check": 0,
        "last_event": "Watcher started",
        "last_event_time": time.time(),
        "total_joins": 0,
        "total_fails": 0,
        "vc_on_count": 0,
        "vc_off_count": 0,
        "status_msg_id": None,
    }
    WATCHERS[chat_id]["task"] = asyncio.create_task(watcher_loop(chat_id))
    save_watchers()


def stop_watcher(chat_id: int):
    chat_id = int(chat_id)
    w = WATCHERS.get(chat_id)
    if not w:
        return False
    w["active"] = False
    if w.get("task"):
        try:
            w["task"].cancel()
        except Exception:
            pass
    WATCHERS.pop(chat_id, None)
    save_watchers()
    return True


# ==================== COMMANDS ====================
@bot_app.on_message(pyro_filters.command("addsession") & owner_only())
async def cmd_addsession(client, message):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await safe_send(message, "Usage: /addsession <session_string>")
        return

    sess = parts[1].strip()
    if len(sess) < 10:
        await safe_send(message, "❌ Invalid session string.")
        return

    status = await message.reply("⏳ Validating...", parse_mode=None)

    try:
        temp = Client(
            f"temp_{int(time.time())}",
            api_id=API_ID, api_hash=API_HASH,
            session_string=sess,
            in_memory=True,
        )
        await temp.start()
        me = await temp.get_me()
        first_name = me.first_name or "Unknown"
        user_id = me.id
        username = me.username or ""

        for acc_id, acc in MULTI_ACCOUNTS.items():
            if acc.get("user_id") == user_id:
                await status.edit_text(f"⚠️ Already added: {first_name}", parse_mode=None)
                await temp.stop()
                return

        acc_id = f"acc_{user_id}"
        MULTI_ACCOUNTS[acc_id] = {
            "client": temp,
            "session_string": sess,
            "first_name": first_name,
            "user_id": user_id,
            "username": username,
            "added_at": time.time(),
            "vc": None,
            "in_vc": False,
            "vc_chats": set(),
        }
        save_accounts()

        global ACTIVE_ACCOUNT
        if not ACTIVE_ACCOUNT:
            ACTIVE_ACCOUNT = acc_id
            save_state()

        await status.edit_text(
            f"✅ Account added!\n\n"
            f"👤 {first_name}\n"
            f"🆔 {user_id}\n"
            f"🔗 @{username or 'None'}\n"
            f"📊 Total: {len(MULTI_ACCOUNTS)}",
            parse_mode=None,
        )
    except AuthKeyUnregistered:
        await status.edit_text("❌ Invalid session (unregistered).", parse_mode=None)
    except UserDeactivated:
        await status.edit_text("❌ Account deactivated.", parse_mode=None)
    except SessionPasswordNeeded:
        await status.edit_text("❌ 2FA enabled. Session string must already be authorized.", parse_mode=None)
    except Exception as e:
        await status.edit_text(f"❌ Error:\n{str(e)[:200]}", parse_mode=None)


@bot_app.on_message(pyro_filters.command("removesession") & owner_only())
async def cmd_removesession(client, message):
    parts = message.text.split()
    if len(parts) < 2:
        await safe_send(message, "Usage: /removesession <account_id>")
        return
    acc_id = parts[1].strip()
    if acc_id not in MULTI_ACCOUNTS:
        await safe_send(message, f"❌ Not found: {acc_id}")
        return

    acc = MULTI_ACCOUNTS[acc_id]
    name = acc.get("first_name", "Unknown")

    try:
        if acc.get("vc"):
            vc = acc["vc"]
            if hasattr(vc, '_calls'):
                for cid in list(vc._calls.keys()):
                    await safe_stop_vc(vc, cid, f"remove_{acc_id}")
        await safe_stop_client(acc["client"], f"remove_{acc_id}")
    except Exception:
        pass

    del MULTI_ACCOUNTS[acc_id]

    global ACTIVE_ACCOUNT
    if ACTIVE_ACCOUNT == acc_id:
        ACTIVE_ACCOUNT = None
        save_state()
    save_accounts()

    await safe_send(message, f"✅ Removed: {name}\n{acc_id}")


@bot_app.on_message(pyro_filters.command("listsessions") & owner_only())
async def cmd_listsessions(client, message):
    if not MULTI_ACCOUNTS:
        await safe_send(message, "No accounts added yet.\n\nUse /addsession <session_string>")
        return
    resp = f"📋 Accounts ({len(MULTI_ACCOUNTS)}):\n\n"
    for acc_id, acc in MULTI_ACCOUNTS.items():
        active = "✅" if acc_id == ACTIVE_ACCOUNT else "⬜"
        vc = "🎙️" if acc.get("in_vc") else ""
        resp += f"{active} {vc} {acc.get('first_name')}\n"
        resp += f"   🆔 {acc.get('user_id')}\n"
        resp += f"   🔗 @{acc.get('username') or 'None'}\n"
        resp += f"   📁 {acc_id}\n\n"
    await safe_send(message, resp[:4000])


# ==================== /join ====================
@bot_app.on_message(pyro_filters.command("join") & owner_only())
async def cmd_join(client, message):
    parts = message.text.split()
    if len(parts) < 2:
        await safe_send(message,
            "Usage: /join <chat_id> [count]\n\n"
            "Example: /join -1001234567890 3\n"
            "→ #1 🟢 unmute, #2 🔴 mute, #3 🟢 unmute\n"
            f"Max count: {MAX_COUNT}"
        )
        return

    try:
        chat_id = int(parts[1])
    except Exception:
        await safe_send(message, "❌ Invalid chat ID.")
        return

    count = 1
    if len(parts) >= 3:
        try:
            count = max(1, min(MAX_COUNT, int(parts[2])))
        except Exception:
            count = 1

    if not MULTI_ACCOUNTS:
        await safe_send(message, "❌ No accounts. Add one first.")
        return

    target_acc_id = pick_account()
    status = await message.reply("⏳ Initializing...", parse_mode=None)

    join_task = asyncio.create_task(
        join_same_session_multiple_vc(target_acc_id, chat_id, count)
    )

    i = 0
    while not join_task.done() and i < 3600:
        frame = PROGRESS_FRAMES[i % len(PROGRESS_FRAMES)]
        step = HIDDEN_STEPS[i % len(HIDDEN_STEPS)]
        bar_len = 12
        filled = i % (bar_len + 1)
        bar = "█" * filled + "░" * (bar_len - filled)
        try:
            await status.edit_text(
                f"{frame} Loading...\n\n{bar}\n{step}",
                parse_mode=None,
            )
        except Exception:
            pass
        await asyncio.sleep(1.5)
        i += 1

    results = await join_task
    success = sum(1 for r in results if r.get("success"))

    if success == 0:
        await status.edit_text("⚠️ Could not complete.", parse_mode=None)
        return

    unmuted = sum(1 for r in results if r.get("success") and not r.get("muted"))
    muted = sum(1 for r in results if r.get("success") and r.get("muted"))

    await status.edit_text(
        f"✅ Connected.\n\n{'█' * 12}\n🟢 {unmuted} unmute | 🔴 {muted} mute\nTotal: {success}/{count}",
        parse_mode=None,
    )


# ==================== /protact ====================
@bot_app.on_message(pyro_filters.command("protact") & owner_only())
async def cmd_protact(client, message):
    parts = message.text.split()
    if len(parts) < 3:
        await safe_send(message,
            "Usage: /protact <chat_id> <count>\n\n"
            "Example: /protact -1001234567890 3\n"
            "→ #1 🟢 unmute, #2 🔴 mute, #3 🟢 unmute\n"
            "Stop: /leaveall\n"
            f"Max count: {MAX_COUNT}"
        )
        return

    try:
        chat_id = int(parts[1])
    except Exception:
        await safe_send(message, "❌ Invalid chat ID.")
        return

    try:
        count = max(1, min(MAX_COUNT, int(parts[2])))
    except Exception:
        await safe_send(message, f"❌ Invalid count (1-{MAX_COUNT}).")
        return

    if not MULTI_ACCOUNTS:
        await safe_send(message, "❌ No accounts. Add one first with /addsession.")
        return

    acc_id = pick_account()
    start_watcher(chat_id, count, acc_id)

    try:
        st_msg = await message.reply(build_status_text(chat_id), parse_mode=None)
        w = WATCHERS.get(chat_id)
        if w:
            w["status_msg_id"] = st_msg.id
            save_watchers()
    except Exception:
        pass


# ==================== /status ====================
@bot_app.on_message(pyro_filters.command("status") & owner_only())
async def cmd_status(client, message):
    parts = message.text.split()
    if len(parts) >= 2:
        try:
            chat_id = int(parts[1])
        except Exception:
            await safe_send(message, "❌ Invalid chat ID.")
            return
        if chat_id not in WATCHERS:
            await safe_send(message, f"❌ No watcher for {chat_id}")
            return
        await safe_send(message, build_status_text(chat_id))
        return

    await safe_send(message, build_all_status_text())


# ==================== /leaveall ====================
@bot_app.on_message(pyro_filters.command("leaveall") & owner_only())
async def cmd_leaveall(client, message):
    parts = message.text.split()

    if len(parts) >= 2:
        try:
            chat_id = int(parts[1])
        except Exception:
            await safe_send(message, "❌ Invalid chat ID.")
            return

        stopped = stop_watcher(chat_id)
        await leave_all_for_chat(chat_id)
        await safe_send(message,
            f"✅ Left all from {chat_id}\n"
            f"Watcher: {'stopped' if stopped else 'not found'}"
        )
        return

    for cid in list(WATCHERS.keys()):
        stop_watcher(cid)
    await leave_all_everything()
    await safe_send(message, "✅ Left all VCs everywhere.\nAll watchers stopped.\nBase sessions still connected.")


# ==================== /stopall ====================
@bot_app.on_message(pyro_filters.command("stopall") & owner_only())
async def cmd_stopall(client, message):
    for cid in list(WATCHERS.keys()):
        stop_watcher(cid)
    await leave_all_everything()
    await safe_send(message, "🛑 All VCs left. All watchers stopped.\nBase sessions still connected.")


# ==================== STARTUP ====================
async def startup_notify():
    await asyncio.sleep(3)
    try:
        await bot_app.send_message(
            OWNER_ID,
            f"🟢 Bot online\n\n"
            f"📊 Accounts: {len(MULTI_ACCOUNTS)}\n"
            f"👁 Watchers: {len(WATCHERS)}\n"
            f"⏱ Uptime: {get_uptime()}\n\n"
            f"Status: /status\n"
            f"Mic mode: alternate (1=unmute, 2=mute)\n"
            f"Max count: {MAX_COUNT}",
            parse_mode=None,
        )
    except Exception:
        pass


async def restart_watchers():
    await asyncio.sleep(5)
    for cid, w in list(WATCHERS.items()):
        if w.get("active", True):
            try:
                start_watcher(cid, w.get("count", 1), w.get("acc_id") or pick_account())
                logger.info(f"👁 Restarted watcher {cid}")
            except Exception as e:
                logger.error(f"Watcher restart {cid}: {e}")


# ==================== MAIN ====================
if __name__ == "__main__":
    print("🎙️ VC BOT v19 — alternate mic, safe clones, base sessions protected")

    if not API_ID or not API_HASH or not BOT_TOKEN or not OWNER_ID:
        print("❌ Missing env vars")
        raise SystemExit(1)

    load_state()
    load_accounts()
    load_watchers()

    print(f"📊 Accounts loaded: {len(MULTI_ACCOUNTS)}")
    for aid, a in MULTI_ACCOUNTS.items():
        print(f"   - {a.get('first_name')} ({aid})")

    PID_FILE = "bot.pid"
    try:
        if os.path.exists(PID_FILE):
            try:
                old = int(open(PID_FILE).read().strip())
                os.kill(old, 0)
                print(f"⚠️ Already running PID {old}")
            except OSError:
                pass
        with open(PID_FILE, "w") as f:
            f.write(str(os.getpid()))

        bot_app.start()
        print("✅ Bot started!")

        loop = asyncio.get_event_loop()
        loop.create_task(startup_notify())
        loop.create_task(restart_watchers())

        idle()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        # Only cleanup clones — NEVER touch base accounts
        try:
            for c in list(VC_CLONES):
                try:
                    vc = c.get("vc")
                    cid = c.get("chat_id")
                    if vc and cid:
                        try:
                            asyncio.run_coroutine_threadsafe(
                                vc.leave_call(cid),
                                asyncio.get_event_loop()
                            ).result(timeout=3)
                        except Exception:
                            pass
                except Exception:
                    pass
        except Exception:
            pass

        # Do NOT stop base accounts — they're kept in memory only
        # Base sessions persist via session_string in accounts file
        try:
            bot_app.stop()
        except Exception:
            pass
        try:
            os.remove(PID_FILE)
        except Exception:
            pass
        print("✅ Cleanup done (base sessions safe)")