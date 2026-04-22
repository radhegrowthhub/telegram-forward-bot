"""
╔══════════════════════════════════════════════════════════════╗
║       🚀 FORWARD PRO BOT — FINAL EDITION v7.0              ║
║   Fast | QR Timer | Admin No-Login | Pro UI | Selling      ║
╚══════════════════════════════════════════════════════════════╝
SETUP:
  1. CONFIG mein values daalo
  2. py -m pip install python-telegram-bot==22.7 telethon qrcode[pil] tgcrypto httpx
  3. py bot.py
"""

# ═══════════════════════════════════════════════════════════════
#   ⚙️  CONFIG
# ═══════════════════════════════════════════════════════════════
API_ID        = 39216193
API_HASH      = "ab8292e51e2f837d8127bfd18648b8b6"
BOT_TOKEN     = "7742398158:AAEhtEbqxu5eecOwGT3YYW7_XiLp9YONjzw"
ADMIN_IDS     = [8286417034]           # ← SIRF aapka Telegram ID — bot owner only
ADMIN_USERNAME = "@Shaan_Malik_Dubai"  # ← admin contact
CLAUDE_KEY    = "your-anthropic-key"  # optional AI key
BOT_NAME      = "Forward Pro"
BOT_VERSION   = "v7.0"
TRIAL_DAYS    = 7
QR_TIMEOUT    = 120   # 2 minutes QR validity
PLANS = {
    "w": ("⚡ Weekly",   7,   19),
    "m": ("🌟 Monthly",  30,  49),
    "y": ("👑 Yearly",   365, 399),
}
CHATS_PER_PAGE = 14
# ═══════════════════════════════════════════════════════════════

import asyncio, os, time, json, re, logging, sqlite3, qrcode
import sys

# Windows pe local, Railway/Linux pe current directory
DB_PATH = "fpro.db"

# Windows pe SelectorEventLoop chahiye
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from telegram import Update, InlineKeyboardButton as IB, InlineKeyboardMarkup as IM, BotCommand
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, ContextTypes, filters
from telethon import TelegramClient, events
from telethon.tl.types import Channel, Chat, MessageMediaWebPage
from telethon.sessions import StringSession

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
LOG = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
#   DATABASE
# ═══════════════════════════════════════════════════════════════
def DB():
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c

def db_init():
    c = DB()
    c.executescript("""
    PRAGMA journal_mode=WAL;
    CREATE TABLE IF NOT EXISTS users(
        uid INTEGER PRIMARY KEY, uname TEXT DEFAULT '',
        name TEXT DEFAULT '', sub_end REAL DEFAULT 0,
        bot_on INTEGER DEFAULT 1, joined REAL DEFAULT 0,
        total_fwd INTEGER DEFAULT 0, is_banned INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS sessions(uid INTEGER PRIMARY KEY, sess TEXT);
    CREATE TABLE IF NOT EXISTS channels(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid INTEGER, ch_name TEXT DEFAULT 'Channel',
        src_id TEXT DEFAULT '', src_name TEXT DEFAULT '',
        dests TEXT DEFAULT '[]', enabled INTEGER DEFAULT 1,
        copy_mode INTEGER DEFAULT 1, silent INTEGER DEFAULT 0,
        pin_msg INTEGER DEFAULT 0, remove_cap INTEGER DEFAULT 0,
        media_only INTEGER DEFAULT 0, text_only INTEGER DEFAULT 0,
        block_links INTEGER DEFAULT 0, dup_check INTEGER DEFAULT 1,
        delay_sec INTEGER DEFAULT 0, header TEXT DEFAULT '',
        footer TEXT DEFAULT '', ai_on INTEGER DEFAULT 0,
        ai_style TEXT DEFAULT 'none', ai_prompt TEXT DEFAULT '',
        fmt_bold INTEGER DEFAULT 0, fmt_clean INTEGER DEFAULT 0,
        block_at INTEGER DEFAULT 0,
        block_www INTEGER DEFAULT 0,
        block_tme INTEGER DEFAULT 0,
        block_all_links INTEGER DEFAULT 0,
        created REAL DEFAULT 0);
    CREATE TABLE IF NOT EXISTS flt(id INTEGER PRIMARY KEY AUTOINCREMENT,
        ch_id INTEGER, uid INTEGER, ftype TEXT, val TEXT);
    CREATE TABLE IF NOT EXISTS repl(id INTEGER PRIMARY KEY AUTOINCREMENT,
        ch_id INTEGER, uid INTEGER, rtype TEXT, old_val TEXT, new_val TEXT);
    CREATE TABLE IF NOT EXISTS fwd_log(id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid INTEGER, ch_id INTEGER, msg_id INTEGER, src TEXT, ts REAL DEFAULT 0);
    CREATE INDEX IF NOT EXISTS idx_fwd_uid ON fwd_log(uid);
    CREATE INDEX IF NOT EXISTS idx_fwd_ts  ON fwd_log(ts);
    CREATE TABLE IF NOT EXISTS plans(
        key TEXT PRIMARY KEY,
        label TEXT, days INTEGER, price INTEGER, enabled INTEGER DEFAULT 1);
    CREATE TABLE IF NOT EXISTS settings(
        key TEXT PRIMARY KEY, value TEXT);
    """)
    # Migration — add new columns if not exist
    for col, default in [
        ("block_at","0"), ("block_www","0"),
        ("block_tme","0"), ("block_all_links","0")
    ]:
        try:
            c.execute(f"ALTER TABLE channels ADD COLUMN {col} INTEGER DEFAULT {default}")
        except: pass
    # Insert default plans if not exist
    defaults = [
        ("w", "⚡ Weekly",  7,   19,  1),
        ("m", "🌟 Monthly", 30,  49,  1),
        ("y", "👑 Yearly",  365, 399, 1),
    ]
    for row in defaults:
        c.execute("INSERT OR IGNORE INTO plans(key,label,days,price,enabled) VALUES(?,?,?,?,?)", row)
    # Default settings
    c.execute("INSERT OR IGNORE INTO settings(key,value) VALUES('trial_days','7')")
    c.commit(); c.close()

# ── Plans (Dynamic) ────────────────────────────────────────────
def plan_all():
    c=DB(); r=c.execute("SELECT * FROM plans ORDER BY days").fetchall(); c.close()
    return [dict(i) for i in r]

def plan_get(key):
    c=DB(); r=c.execute("SELECT * FROM plans WHERE key=?",(key,)).fetchone(); c.close()
    return dict(r) if r else None

def plan_upd(key, **kw):
    c=DB()
    for k,v in kw.items(): c.execute(f"UPDATE plans SET {k}=? WHERE key=?",(v,key))
    c.commit(); c.close()

def plan_add(key, label, days, price):
    c=DB(); c.execute("INSERT OR REPLACE INTO plans(key,label,days,price,enabled) VALUES(?,?,?,?,1)",(key,label,days,price)); c.commit(); c.close()

def plan_del(key):
    c=DB(); c.execute("DELETE FROM plans WHERE key=?",(key,)); c.commit(); c.close()

def setting_get(key, default=""):
    c=DB(); r=c.execute("SELECT value FROM settings WHERE key=?",(key,)).fetchone(); c.close()
    return r['value'] if r else default

def setting_set(key, value):
    c=DB(); c.execute("INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)",(key,str(value))); c.commit(); c.close()

def get_trial_days():
    return int(setting_get('trial_days', '7'))

# ── Users ──────────────────────────────────────────────────────
def u_upsert(uid, un="", nm=""):
    c=DB(); r=c.execute("SELECT uid FROM users WHERE uid=?",(uid,)).fetchone()
    new=r is None; now=time.time()
    if new: c.execute("INSERT INTO users(uid,uname,name,sub_end,joined) VALUES(?,?,?,?,?)",(uid,un,nm,now+get_trial_days()*86400,now))
    else:   c.execute("UPDATE users SET uname=?,name=? WHERE uid=?",(un,nm,uid))
    c.commit(); c.close(); return new

def u_get(uid):
    c=DB(); r=c.execute("SELECT * FROM users WHERE uid=?",(uid,)).fetchone(); c.close()
    return dict(r) if r else None

def u_all():
    c=DB(); r=c.execute("SELECT * FROM users ORDER BY joined DESC").fetchall(); c.close()
    return [dict(i) for i in r]

def u_ok(uid):
    u=u_get(uid); return bool(u and not u['is_banned'] and time.time()<u['sub_end'])

def u_sub_str(uid):
    u=u_get(uid)
    if not u: return "❌ No Account"
    if u['is_banned']: return "🚫 Banned"
    rem=u['sub_end']-time.time()
    if rem<=0: return "❌ Expired"
    d=int(rem//86400); h=int((rem%86400)//3600); m=int((rem%3600)//60)
    return f"✅ {d}d {h}h {m}m"

def u_sub_end_str(uid):
    import datetime
    u=u_get(uid)
    if not u or u['sub_end']<=time.time(): return "Expired"
    return datetime.datetime.fromtimestamp(u['sub_end']).strftime("%d %b %Y, %I:%M %p")

def u_give(uid, days):
    c=DB(); u=c.execute("SELECT sub_end FROM users WHERE uid=?",(uid,)).fetchone()
    now=time.time(); base=max(u['sub_end'] if u else now, now)
    c.execute("UPDATE users SET sub_end=? WHERE uid=?",(base+days*86400,uid)); c.commit(); c.close()

def u_revoke(uid): c=DB(); c.execute("UPDATE users SET sub_end=? WHERE uid=?",(time.time()-1,uid)); c.commit(); c.close()
def u_ban(uid,v=True): c=DB(); c.execute("UPDATE users SET is_banned=? WHERE uid=?",(int(v),uid)); c.commit(); c.close()
def u_toggle_bot(uid,v): c=DB(); c.execute("UPDATE users SET bot_on=? WHERE uid=?",(int(v),uid)); c.commit(); c.close()
def u_bot_on(uid): u=u_get(uid); return bool(u and u['bot_on'])
def u_inc(uid,n=1): c=DB(); c.execute("UPDATE users SET total_fwd=total_fwd+? WHERE uid=?",(n,uid)); c.commit(); c.close()

def s_save(uid,s): c=DB(); c.execute("INSERT OR REPLACE INTO sessions VALUES(?,?)",(uid,s)); c.commit(); c.close()
def s_get(uid):
    c=DB(); r=c.execute("SELECT sess FROM sessions WHERE uid=?",(uid,)).fetchone(); c.close()
    return r['sess'] if r else None
def s_del(uid): c=DB(); c.execute("DELETE FROM sessions WHERE uid=?",(uid,)); c.commit(); c.close()

# ── Channels ───────────────────────────────────────────────────
def ch_add(uid,src_id,src_name,ch_name):
    c=DB(); cur=c.execute("INSERT INTO channels(uid,ch_name,src_id,src_name,created) VALUES(?,?,?,?,?)",(uid,ch_name,src_id,src_name,time.time()))
    cid=cur.lastrowid; c.commit(); c.close(); return cid

def ch_all(uid):
    c=DB(); r=c.execute("SELECT * FROM channels WHERE uid=? ORDER BY id",(uid,)).fetchall(); c.close()
    return [dict(i) for i in r]

def ch_get(cid):
    c=DB(); r=c.execute("SELECT * FROM channels WHERE id=?",(cid,)).fetchone(); c.close()
    return dict(r) if r else None

def ch_upd(cid,**kw):
    c=DB()
    for k,v in kw.items(): c.execute(f"UPDATE channels SET {k}=? WHERE id=?",(v,cid))
    c.commit(); c.close()

def ch_del(cid,uid):
    c=DB()
    c.execute("DELETE FROM channels WHERE id=? AND uid=?",(cid,uid))
    c.execute("DELETE FROM flt WHERE ch_id=?",(cid,))
    c.execute("DELETE FROM repl WHERE ch_id=?",(cid,))
    c.commit(); c.close()

def ch_toggle(cid):
    r=ch_get(cid); new=0 if r['enabled'] else 1; ch_upd(cid,enabled=new); return bool(new)

def ch_add_dest(cid,did,dname):
    r=ch_get(cid); dests=json.loads(r['dests'])
    if not any(d['id']==str(did) for d in dests):
        dests.append({'id':str(did),'name':dname}); ch_upd(cid,dests=json.dumps(dests)); return True
    return False

def ch_del_dest(cid,did):
    r=ch_get(cid); dests=[d for d in json.loads(r['dests']) if d['id']!=str(did)]
    ch_upd(cid,dests=json.dumps(dests))

def f_add(cid,uid,ft,val): c=DB(); c.execute("INSERT INTO flt(ch_id,uid,ftype,val) VALUES(?,?,?,?)",(cid,uid,ft,val)); c.commit(); c.close()
def f_get(cid):
    c=DB(); r=c.execute("SELECT * FROM flt WHERE ch_id=?",(cid,)).fetchall(); c.close(); return [dict(i) for i in r]
def f_del(fid): c=DB(); c.execute("DELETE FROM flt WHERE id=?",(fid,)); c.commit(); c.close()

def rp_add(cid,uid,rt,o,n): c=DB(); c.execute("INSERT INTO repl(ch_id,uid,rtype,old_val,new_val) VALUES(?,?,?,?,?)",(cid,uid,rt,o,n)); c.commit(); c.close()
def rp_get(cid,rt=None):
    c=DB()
    if rt: r=c.execute("SELECT * FROM repl WHERE ch_id=? AND rtype=?",(cid,rt)).fetchall()
    else:  r=c.execute("SELECT * FROM repl WHERE ch_id=?",(cid,)).fetchall()
    c.close(); return [dict(i) for i in r]
def rp_del(rpid): c=DB(); c.execute("DELETE FROM repl WHERE id=?",(rpid,)); c.commit(); c.close()

def l_add(uid,cid,mid,src): c=DB(); c.execute("INSERT INTO fwd_log(uid,ch_id,msg_id,src,ts) VALUES(?,?,?,?,?)",(uid,cid,mid,str(src),time.time())); c.commit(); c.close()
def l_dup(cid,mid,src):
    c=DB(); r=c.execute("SELECT id FROM fwd_log WHERE ch_id=? AND msg_id=? AND src=?",(cid,mid,str(src))).fetchone(); c.close(); return r is not None

def l_stats(uid):
    c=DB()
    tot=c.execute("SELECT COUNT(*) as n FROM fwd_log WHERE uid=?",(uid,)).fetchone()['n']
    tod=c.execute("SELECT COUNT(*) as n FROM fwd_log WHERE uid=? AND ts>?",(uid,time.time()-86400)).fetchone()['n']
    wk =c.execute("SELECT COUNT(*) as n FROM fwd_log WHERE uid=? AND ts>?",(uid,time.time()-604800)).fetchone()['n']
    c.close(); return tot,tod,wk

def adm_stats():
    c=DB(); now=time.time()
    tu=c.execute("SELECT COUNT(*) as n FROM users").fetchone()['n']
    au=c.execute("SELECT COUNT(*) as n FROM users WHERE sub_end>? AND is_banned=0",(now,)).fetchone()['n']
    tf=c.execute("SELECT COUNT(*) as n FROM fwd_log").fetchone()['n']
    tr=c.execute("SELECT COUNT(*) as n FROM channels").fetchone()['n']
    tb=c.execute("SELECT COUNT(*) as n FROM users WHERE is_banned=1").fetchone()['n']
    td=c.execute("SELECT COUNT(*) as n FROM fwd_log WHERE ts>?",(now-86400,)).fetchone()['n']
    nw=c.execute("SELECT COUNT(*) as n FROM users WHERE joined>?",(now-604800,)).fetchone()['n']
    c.close(); return tu,au,tf,tr,tb,td,nw

# ═══════════════════════════════════════════════════════════════
#   CHAT CACHE
# ═══════════════════════════════════════════════════════════════
CHAT_CACHE: dict = {}

async def fetch_chats(uid: int) -> list:
    client = ACL.get(uid)
    sess = s_get(uid)
    if not client and not sess: return []
    own = False
    if not client:
        client = TelegramClient(StringSession(sess), API_ID, API_HASH)
        await client.connect(); own = True
    chats = []
    try:
        dialogs = await client.get_dialogs(limit=300)
        for d in dialogs:
            e = d.entity
            if isinstance(e, (Channel, Chat)):
                icon = "📢" if getattr(e,'broadcast',False) else "👥"
                sid = str(e.id)
                if not sid.startswith('-'): sid = f"-100{sid}"
                chats.append({'id':sid,'name':e.title or "Unknown",'icon':icon})
        CHAT_CACHE[uid] = chats
    except Exception as ex:
        LOG.error(f"fetch_chats {uid}: {ex}")
    finally:
        if own:
            try: await client.disconnect()
            except: pass
    return chats

def chat_list_text(chats, page=0):
    start = page * CHATS_PER_PAGE
    end   = start + CHATS_PER_PAGE
    pc    = chats[start:end]
    lines = [f"{start+i+1}. {c['icon']} {c['name']}" for i,c in enumerate(pc)]
    return "\n".join(lines), pc

def num_kb(chats, page, prefix, total):
    start = page * CHATS_PER_PAGE
    end   = min(start + CHATS_PER_PAGE, total)
    nums  = list(range(start+1, end+1))
    rows  = []
    r1    = [IB(str(n), callback_data=f"{prefix}_{n-1}") for n in nums[:7]]
    r2    = [IB(str(n), callback_data=f"{prefix}_{n-1}") for n in nums[7:14]]
    if r1: rows.append(r1)
    if r2: rows.append(r2)
    nav = []
    if page > 0:          nav.append(IB("◀ Prev", callback_data=f"pg_{prefix}_{page-1}"))
    if end < total:       nav.append(IB("Next ▶", callback_data=f"pg_{prefix}_{page+1}"))
    if nav: rows.append(nav)
    rows.append([IB("🔄 Refresh", callback_data=f"refr_{prefix}"),
                 IB("🏠 Home",   callback_data="main")])
    return IM(rows)

# ═══════════════════════════════════════════════════════════════
#   AI
# ═══════════════════════════════════════════════════════════════
AI_STYLES = {
    "none":      ("❌ Off",       "No AI"),
    "news":      ("📰 News",      "Professional news bulletin"),
    "casual":    ("💬 Casual",    "Friendly casual tone"),
    "formal":    ("🎩 Formal",    "Formal professional tone"),
    "short":     ("✂️ Short",     "Summarize in 2-3 lines"),
    "bullet":    ("• Bullets",    "Convert to bullet points"),
    "emoji":     ("😊 Emoji",     "Add emojis make engaging"),
    "clickbait": ("🔥 Clickbait", "Catchy attention-grabbing"),
    "clean":     ("🧹 Clean",     "Remove promos and links"),
    "custom":    ("✏️ Custom",    "My custom AI prompt"),
}

async def ai_rewrite(text, ch):
    if not text or ch.get('ai_style','none')=='none': return text
    style = ch.get('ai_style','none'); prompt = ch.get('ai_prompt','')
    system = prompt if (style=='custom' and prompt) else (
        f"Rewrite as: {AI_STYLES.get(style,('',''))[1]}. "
        "Keep core info. Match original language. Return ONLY rewritten text."
    )
    try:
        import httpx
        async with httpx.AsyncClient(timeout=10) as cl:
            r = await cl.post("https://api.anthropic.com/v1/messages",
                headers={"x-api-key":CLAUDE_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
                json={"model":"claude-haiku-4-5-20251001","max_tokens":800,
                      "system":system,"messages":[{"role":"user","content":f"Rewrite:\n{text}"}]})
            d = r.json()
            if "content" in d: return d["content"][0]["text"].strip()
    except: pass
    return text

# ═══════════════════════════════════════════════════════════════
#   FORWARDING ENGINE
# ═══════════════════════════════════════════════════════════════
ACL: dict = {}
TSK: dict = {}
# reply_map[uid][ch_id][src_msg_id] = dest_msg_id
# Tracks which source message ID maps to which destination message ID
# So reply chains are preserved in forwarded messages
REPLY_MAP: dict = {}

async def _fwd(cl, msg, did, ch, repls, reply_to_msg_id=None):
    try:
        try: did=int(did)
        except: pass
        raw = msg.text or msg.caption or ""
        for r in repls: raw=raw.replace(r['old_val'],r['new_val'])

        # ── COMPREHENSIVE LINK BLOCKING ──────────────────────────
        # Common TLDs for bare domain detection
        _TLDS = (
            r'com|net|org|in|co|io|info|biz|tv|me|online|live|site|web|app|pro'
            r'|club|shop|store|tech|top|xyz|bet|win|casino|games|vip|to|cc|gg'
            r'|uk|us|eu|au|pk|bd|lk|np'
        )

        def _strip(text, b_all, b_www, b_tme, b_at):
            if not text: return text

            if b_all or b_www:
                # 1. https:// or http:// links (any domain, encoded URLs, emoji domains)
                text = re.sub(r'https?://\S+', '', text, flags=re.IGNORECASE)
                # 2. www. links
                text = re.sub(r'www\.\S+', '', text, flags=re.IGNORECASE)
                # 3. Bare domains like bharatwins.com, site.net, example.co.in
                text = re.sub(
                    r'\b[A-Za-z0-9](?:[A-Za-z0-9\-]{0,61}[A-Za-z0-9])?'
                    r'\.(?:' + _TLDS + r')(?:/\S*)?\b',
                    '', text, flags=re.IGNORECASE
                )

            if b_all or b_tme:
                # t.me links including invite +links
                text = re.sub(r't\.me/\S+', '', text, flags=re.IGNORECASE)

            if b_all or b_at:
                # @username mentions
                text = re.sub(r'@[A-Za-z0-9_]+', '', text)

            # Clean leftover spaces/lines
            text = re.sub(r'[ \t]{2,}', ' ', text)
            text = re.sub(r'\n{3,}', '\n\n', text)
            return text.strip()

        b_all = bool(ch.get('block_all_links') or ch.get('block_links'))
        b_www = bool(ch.get('block_www'))
        b_tme = bool(ch.get('block_tme'))
        b_at  = bool(ch.get('block_at'))
        doing_block = b_all or b_www or b_tme or b_at

        if doing_block:
            raw = _strip(raw, b_all, b_www, b_tme, b_at)

        if ch.get('ai_on'): raw = await ai_rewrite(raw, ch)
        if ch.get('fmt_clean'): raw = re.sub(r'\n{3,}','\n\n',raw).strip()
        if ch.get('fmt_bold'):
            ls=raw.split('\n')
            if ls and ls[0].strip(): ls[0]=f"**{ls[0].strip()}**"
            raw='\n'.join(ls)

        hdr=(ch.get('header') or '').strip()
        ftr=(ch.get('footer') or '').strip()
        if hdr: raw=f"{hdr}\n\n{raw}" if raw else hdr
        if ftr: raw=f"{raw}\n\n{ftr}" if raw else ftr
        if ch.get('remove_cap'): raw=""

        sil = bool(ch.get('silent'))

        # Detect WebPage media (link preview card) — never forward these
        is_webpage = isinstance(getattr(msg, 'media', None), MessageMediaWebPage)
        real_media = bool(msg.media and not is_webpage)

        if ch.get('copy_mode'):
            if real_media:
                # send_file — photos, videos, documents, stickers etc.
                # NOTE: link_preview is NOT valid for send_file, only send_message
                s = await cl.send_file(
                    did, msg.media,
                    caption=raw,
                    silent=sil,
                    buttons=None,
                    reply_to=reply_to_msg_id,
                )
            elif raw:
                s = await cl.send_message(
                    did, raw,
                    silent=sil,
                    link_preview=False,
                    reply_to=reply_to_msg_id,
                )
            else:
                return None
        else:
            # Forward mode — preserves original format with source channel name
            s = await cl.forward_messages(did, msg)

        if ch.get('pin_msg') and s:
            try: await cl.pin_message(did, s, notify=not sil)
            except: pass
        return s
    except Exception as e:
        LOG.error(f"FWD→{did}: {e}"); return None

async def eng_start(uid):
    sess=s_get(uid)
    if not sess: return False
    await eng_stop(uid)
    try:
        cl=TelegramClient(StringSession(sess),API_ID,API_HASH)
        await cl.connect()
        if not await cl.is_user_authorized(): return False
        ACL[uid]=cl
        chs=ch_all(uid)
        if not chs: return True
        sm={}
        for ch in chs:
            if not ch['enabled']: continue
            try: k=int(ch['src_id'])
            except: k=ch['src_id']
            sm.setdefault(k,[]).append(ch)
        if not sm: return True

        @cl.on(events.NewMessage(chats=list(sm.keys())))
        async def handler(ev):
            if not u_bot_on(uid) or not u_ok(uid): return
            matched = sm.get(ev.chat_id,[]) or sm.get(str(ev.chat_id),[])
            for _ch_stale in matched:
                # Fresh settings from DB on every message
                ch = ch_get(_ch_stale['id'])
                if not ch or not ch['enabled']: continue
                if ch.get('dup_check') and l_dup(ch['id'],ev.message.id,str(ev.chat_id)): continue
                txt=(ev.message.text or ev.message.caption or "").lower()
                fils=f_get(ch['id'])
                if any(f['val'].lower() in txt for f in fils if f['ftype']=='blacklist'): continue
                wl=[f for f in fils if f['ftype']=='whitelist']
                if wl and not any(f['val'].lower() in txt for f in wl): continue
                if ch.get('media_only') and not ev.message.media: continue
                if ch.get('text_only') and ev.message.media: continue
                repls=rp_get(ch['id']); dests=json.loads(ch['dests']); cnt=0

                # ── REPLY CHAIN: check if source msg is replying to another msg ──
                src_reply_id = None
                if ev.message.reply_to and hasattr(ev.message.reply_to, 'reply_to_msg_id'):
                    src_reply_id = ev.message.reply_to.reply_to_msg_id

                for d in dests:
                    # Find mapped dest msg_id for the reply
                    reply_to_in_dest = None
                    if src_reply_id:
                        rmap = REPLY_MAP.get(uid, {}).get(ch['id'], {}).get(d['id'], {})
                        reply_to_in_dest = rmap.get(src_reply_id)

                    r = await _fwd(cl, ev.message, d['id'], ch, repls,
                                   reply_to_msg_id=reply_to_in_dest)
                    if r:
                        cnt += 1
                        # Save mapping: src_msg_id → dest_msg_id for future replies
                        dest_id_str = str(d['id'])
                        REPLY_MAP.setdefault(uid, {}).setdefault(ch['id'], {}).setdefault(dest_id_str, {})
                        REPLY_MAP[uid][ch['id']][dest_id_str][ev.message.id] = r.id
                        # Keep map small — only last 500 per channel-dest pair
                        rmap = REPLY_MAP[uid][ch['id']][dest_id_str]
                        if len(rmap) > 500:
                            oldest = list(rmap.keys())[:100]
                            for k in oldest: del rmap[k]

                    if ch.get('delay_sec',0)>0: await asyncio.sleep(ch['delay_sec'])
                if cnt: l_add(uid,ch['id'],ev.message.id,str(ev.chat_id)); u_inc(uid,cnt)

        async def runner():
            try: await cl.run_until_disconnected()
            except Exception as e:
                LOG.warning(f"Engine uid={uid}: {e}")
                await asyncio.sleep(8); await eng_start(uid)

        TSK[uid]=asyncio.create_task(runner())
        LOG.info(f"✅ Engine uid={uid}")
        return True
    except Exception as e: LOG.error(f"eng_start {uid}: {e}"); return False

async def eng_stop(uid):
    t=TSK.pop(uid,None)
    if t: t.cancel()
    c=ACL.pop(uid,None)
    if c:
        try: await c.disconnect()
        except: pass

async def eng_restart_all():
    for u in u_all():
        uid=u['uid']
        if u_ok(uid) and u_bot_on(uid) and s_get(uid):
            await eng_start(uid); await asyncio.sleep(0.2)

# ═══════════════════════════════════════════════════════════════
#   UI HELPERS
# ═══════════════════════════════════════════════════════════════
def B(t,c): return IB(t, callback_data=c)
def KB(*rows): return IM(list(rows))
def OO(v): return "🟢" if v else "🔴"
def EE(v): return "✅" if v else "☐"

# ── MAIN MENU — compact ────────────────────────────────────────
def KB_MAIN(uid):
    on=u_bot_on(uid); eng=uid in ACL
    return IM([
        [B(f"{'🟢 Running' if on and eng else '🔴 Paused'}  •  {BOT_NAME}","noop")],
        [B("📡 Channels","chs"),         B("⚙️ Settings","ctrl_g")],
        [B("🤖 AI Rewrite","ai_g"),      B("🔄 Replace","repl_g")],
        [B("📊 Stats","stats"),           B("💳 Subscription","sub")],
        [B(f"{OO(on)} Bot","bot_tog"),   B(f"{'⚡ Engine' if eng else '⛔ Engine'}","eng_r")],
        [B("🆘 Support","support"),       B("🚪 Logout","logout")],
    ])

# ── ADMIN — compact ────────────────────────────────────────────
def KB_ADM():
    tu,au,tf,tr,tb,td,nw = adm_stats()
    return IM([
        [B(f"🔧 Admin  •  {tu}👥  {au}✅  {tb}🚫","noop")],
        [B(f"📨 {tf} fwd  •  📅 {td} today  •  🆕 {nw}/7d","noop")],
        [B("👥 Users","a_users"),          B("✅ Active","a_active")],
        [B("✅ Give Sub","a_give"),         B("❌ Revoke","a_revoke")],
        [B("🚫 Ban","a_ban"),               B("✅ Unban","a_unban")],
        [B("🔍 Search","a_search")],
        [B("📢 Broadcast All","a_bc_all"),  B("📢 Active Only","a_bc_act")],
        [B("📊 Stats","a_stats"),           B("📡 Channels","a_chs")],
        [B("💰 Revenue","a_revenue"),       B("🔄 Restart All","a_restart")],
        [B("💳 Plan Management","a_plans")],
    ])

# ── PLAN MANAGEMENT KEYBOARD ────────────────────────────────────
def KB_PLANS():
    plans = plan_all()
    trial = get_trial_days()
    rows = [
        [B("💳 Plan Management","noop")],
        [B(f"🎁 Trial Days: {trial}  (tap to change)","a_trial")],
        [B("─────────────────────","noop")],
    ]
    for p in plans:
        status = "🟢" if p['enabled'] else "🔴"
        rows.append([
            B(f"{status} {p['label']}  ₹{p['price']}  {p['days']}d", f"a_plan_edit_{p['key']}"),
            B("🔴" if p['enabled'] else "🟢", f"a_plan_tog_{p['key']}"),
            B("🗑", f"a_plan_del_{p['key']}"),
        ])
    rows.append([B("➕ Add New Plan","a_plan_add")])
    rows.append([B("‹ Back","adm")])
    return IM(rows)

# ── CHANNELS ───────────────────────────────────────────────────
def KB_CHS(uid):
    chs=ch_all(uid); rows=[]
    for ch in chs:
        on="🟢" if ch['enabled'] else "🔴"
        ai="🤖" if ch['ai_on'] else ""
        dests=json.loads(ch['dests'])
        rows.append([
            B(f"{on}{ai} {ch['ch_name'][:20]} ({len(dests)}▶)",f"ch_{ch['id']}"),
            B("⚙",f"ctrl_{ch['id']}"), B("🗑",f"ch_del_{ch['id']}"),
        ])
    if not chs: rows.append([B("No channels yet","noop")])
    rows.append([B("➕ Add Channel","ch_new"), B("‹ Back","main")])
    return IM(rows)

# ── CHANNEL ────────────────────────────────────────────────────
def KB_CH(cid):
    if not ch_get(cid): return KB([B("❌ Not found","noop"),B("‹ Back","chs")])
    ch=ch_get(cid); dests=json.loads(ch['dests'])
    ball = bool(ch.get('block_all_links') or ch.get('block_links'))
    bat  = bool(ch.get('block_at'))
    return IM([
        [B(f"{'🟢' if ch['enabled'] else '🔴'} {ch['ch_name'][:28]}",f"ch_tog_{cid}")],
        [B(f"📤 {len(dests)} dest",f"dests_{cid}"),  B("⚙️ Control",f"ctrl_{cid}")],
        [B("🤖 AI",f"ai_{cid}"),                     B("🔄 Replace",f"repls_{cid}")],
        [B("✂️ Filters",f"flt_{cid}"),                B("📐 Format",f"fmt_{cid}")],
        [B(f"{'🔴 Links Blocked' if ball else '⚪ Block Links'}",f"t_ball_{cid}"),
         B(f"{'🔴 @User Blocked' if bat  else '⚪ Block @User'}",f"t_bat_{cid}")],
        [B("✏️ Rename",f"ch_ren_{cid}"),               B("📊 Stats",f"ch_stat_{cid}")],
        [B("‹ Channels","chs"),                        B("🏠 Home","main")],
    ])

# ── CONTROL PANEL ──────────────────────────────────────────────
def KB_CTRL(cid):
    ch=ch_get(cid)
    if not ch: return KB([B("❌ Not found","noop"),B("‹ Back","chs")])
    ball = bool(ch.get('block_all_links') or ch.get('block_links'))
    bat  = bool(ch.get('block_at'))
    return IM([
        [B(f"⚙️ {ch['ch_name'][:28]}","noop")],
        [B(f"{OO(ch['copy_mode'])} Copy Mode",   f"t_copy_{cid}"),
         B(f"{OO(ch['silent'])} Silent",          f"t_sil_{cid}")],
        [B(f"{OO(ch['pin_msg'])} Pin",            f"t_pin_{cid}"),
         B(f"{OO(ch['remove_cap'])} No Caption",  f"t_cap_{cid}")],
        [B(f"{OO(ch['media_only'])} Media Only",  f"t_med_{cid}"),
         B(f"{OO(ch['text_only'])} Text Only",    f"t_txt_{cid}")],
        [B(f"{OO(ch['dup_check'])} Dup Check",    f"t_dup_{cid}"),
         B(f"{OO(ch['enabled'])} Enable",          f"ch_tog_{cid}")],
        [B(f"⏱ Delay: {ch['delay_sec']}s",        f"s_dly_{cid}"),
         B(f"📝 Header {'✅' if ch['header'] else '—'}",f"s_hdr_{cid}")],
        [B(f"📝 Footer {'✅' if ch['footer'] else '—'}",f"s_ftr_{cid}")],
        [B("── QUICK BLOCK ──","noop")],
        [B(f"{'🔴 Block ALL Links  ON' if ball else '⚪ Block ALL Links  OFF'}",f"t_ball_{cid}")],
        [B(f"{'🔴 Block @Username  ON' if bat  else '⚪ Block @Username  OFF'}",f"t_bat_{cid}")],
        [B("🤖 AI",f"ai_{cid}"),  B("🔄 Replace",f"repls_{cid}"),  B("✂️ Filters",f"flt_{cid}")],
        [B("‹ Back",f"ch_{cid}"), B("🏠 Home","main")],
    ])

def KB_CTRL_G(uid):
    on=u_bot_on(uid); eng=uid in ACL; chs=ch_all(uid)
    on_c=sum(1 for c in chs if c['enabled'])
    return IM([
        [B(f"{OO(on)} Bot","bot_tog"), B(f"{'⚡ ON' if eng else '⛔ OFF'} Engine","eng_r")],
        [B(f"📡 {len(chs)} channels  •  🟢 {on_c} active","noop")],
        [B("📡 Channels","chs"), B("‹ Back","main")],
    ])

# ── AI ─────────────────────────────────────────────────────────
def KB_AI(cid):
    ch=ch_get(cid); cur=ch.get('ai_style','none')
    rows=[[B(f"🤖 AI  •  {ch['ch_name'][:22]}","noop")],
          [B(f"Power: {OO(ch['ai_on'])} {'ON — '+AI_STYLES.get(cur,('?',))[0] if ch['ai_on'] else 'OFF'}",f"t_ai_{cid}")]]
    for k,(label,_) in AI_STYLES.items():
        mark="✅ " if cur==k else ""
        rows.append([B(f"{mark}{label}",f"ai_s_{cid}_{k}")])
    rows.append([B(f"✏️ Custom {'✅' if ch.get('ai_prompt') else '—'}",f"ai_cp_{cid}"),
                 B("🧪 Test",f"ai_t_{cid}")])
    rows.append([B("‹ Back",f"ch_{cid}"), B("🏠 Home","main")])
    return IM(rows)

def KB_AI_G(uid):
    chs=ch_all(uid); rows=[[B("🤖 AI — Select Channel","noop")]]
    for ch in chs:
        st=AI_STYLES.get(ch.get('ai_style','none'),('❌',))[0]
        rows.append([B(f"{'🤖' if ch['ai_on'] else '○'} {ch['ch_name'][:22]} — {st}",f"ai_{ch['id']}")])
    if not chs: rows.append([B("No channels!","ch_new")])
    rows.append([B("‹ Back","main")])
    return IM(rows)

# ── REPLACEMENTS ───────────────────────────────────────────────
def KB_REPLS(cid):
    ch=ch_get(cid); lrp=rp_get(cid,'link'); wrp=rp_get(cid,'word')
    rows=[[B(f"🔄 {ch['ch_name'][:26]}","noop")]]
    rows.append([B(f"🔗 Link ({len(lrp)})","noop")])
    for r in lrp: rows.append([B(f"🔗 {r['old_val'][:18]}→{r['new_val'][:12]}","noop"),B("🗑",f"d_rp_{r['id']}")])
    rows.append([B("➕ Add Link Replace",f"add_lrp_{cid}")])
    rows.append([B(f"📝 Word ({len(wrp)})","noop")])
    for r in wrp: rows.append([B(f"📝 {r['old_val'][:18]}→{r['new_val'][:12]}","noop"),B("🗑",f"d_rp_{r['id']}")])
    rows.append([B("➕ Add Word Replace",f"add_wrp_{cid}")])
    rows.append([B("‹ Back",f"ch_{cid}"), B("🏠 Home","main")])
    return IM(rows)

def KB_REPL_G(uid):
    chs=ch_all(uid); rows=[[B("🔄 Replacements","noop")]]
    for ch in chs:
        lc=len(rp_get(ch['id'],'link')); wc=len(rp_get(ch['id'],'word'))
        rows.append([B(f"📡 {ch['ch_name'][:22]}  🔗{lc} 📝{wc}",f"repls_{ch['id']}")])
    if not chs: rows.append([B("No channels!","ch_new")])
    rows.append([B("‹ Back","main")])
    return IM(rows)

# ── FILTERS ────────────────────────────────────────────────────
def KB_FLT(cid):
    ch = ch_get(cid)
    if not ch: return KB([B("❌ Channel not found","noop"), B("‹ Back","chs")])
    fils=f_get(cid)
    wl=[f for f in fils if f['ftype']=='whitelist']
    bl=[f for f in fils if f['ftype']=='blacklist']
    all_on  = bool(ch.get('block_all_links') or ch.get('block_links'))
    www_on  = bool(ch.get('block_www'))
    tme_on  = bool(ch.get('block_tme'))
    at_on   = bool(ch.get('block_at'))
    return IM([
        [B(f"✂️ Filters  •  {ch['ch_name'][:20]}","noop")],

        [B("─── 🔗 LINK BLOCKING ───","noop")],
        # Master toggle
        [B(f"{'🔴 BLOCK ALL LINKS  ✅ ON' if all_on else '⚪ BLOCK ALL LINKS  ❌ OFF'}",
           f"t_ball_{cid}")],
        [B("─ Individual Link Controls ─","noop")],
        # http/www/bare domains — ONE button
        [B(f"{OO(www_on)} http • https • www • .com/.net etc",f"t_bwww_{cid}")],
        # t.me links
        [B(f"{OO(tme_on)} t.me Links  (invite/channel/post)",f"t_btme_{cid}")],

        [B("─── 👤 USERNAME BLOCKING ───","noop")],
        # @username — completely separate
        [B(f"{OO(at_on)} @username Mentions  (@abc, @xyz)",f"t_bat_{cid}")],

        [B("─── ✅ WHITELIST (Forward IF contains) ───","noop")],
        *[[B(f"✅ {f['val'][:30]}","noop"),B("🗑",f"d_flt_{f['id']}")] for f in wl],
        [B("➕ Add Whitelist",f"add_wl_{cid}")],

        [B("─── 🚫 BLACKLIST (Skip IF contains) ───","noop")],
        *[[B(f"🚫 {f['val'][:30]}","noop"),B("🗑",f"d_flt_{f['id']}")] for f in bl],
        [B("➕ Add Blacklist",f"add_bl_{cid}")],

        [B("‹ Back",f"ch_{cid}"), B("🏠 Home","main")],
    ])

# ── FORMAT ─────────────────────────────────────────────────────
def KB_FMT(cid):
    ch=ch_get(cid)
    return IM([
        [B(f"📐 Format  •  {ch['ch_name'][:20]}","noop")],
        [B(f"{OO(ch['fmt_bold'])} Bold Title",   f"t_bold_{cid}"),
         B(f"{OO(ch['fmt_clean'])} Clean Text",  f"t_clean_{cid}")],
        [B(f"{OO(ch['block_links'])} No Links",  f"t_blk_{cid}"),
         B(f"{OO(ch['remove_cap'])} No Caption", f"t_cap_{cid}")],
        [B(f"📝 Header {'✅' if ch['header'] else '—'}",f"s_hdr_{cid}"),
         B(f"📝 Footer {'✅' if ch['footer'] else '—'}",f"s_ftr_{cid}")],
        [B("🤖 AI Style",f"ai_{cid}")],
        [B("‹ Back",f"ch_{cid}"), B("🏠 Home","main")],
    ])

# ── DESTINATIONS ───────────────────────────────────────────────
def KB_DESTS(cid):
    ch=ch_get(cid); dests=json.loads(ch['dests'])
    rows=[[B(f"📤 Destinations  •  {ch['ch_name'][:18]}","noop")]]
    for d in dests: rows.append([B(f"▸ {d['name'][:30]}","noop"),B("🗑",f"d_dest_{cid}_{d['id']}")])
    if not dests: rows.append([B("No destinations yet","noop")])
    rows.append([B("➕ Add Destination",f"dest_pick_{cid}")])
    rows.append([B("‹ Back",f"ch_{cid}"), B("🏠 Home","main")])
    return IM(rows)

# ── SUBSCRIPTION ───────────────────────────────────────────────
def KB_SUB(uid):
    plans = [p for p in plan_all() if p['enabled']]
    rows = [
        [B(f"💳  {u_sub_str(uid)}","noop")],
        [B(f"Expires: {u_sub_end_str(uid)}","noop")],
    ]
    for p in plans:
        rows.append([B(f"{p['label']}  •  ₹{p['price']}  •  {p['days']} Days", f"buy_{p['key']}")])
    rows.append([B("🔄 Renew","sub_renew"),  B(f"💬 {ADMIN_USERNAME}","support")])
    rows.append([B("‹ Back","main")])
    return IM(rows)

# ═══════════════════════════════════════════════════════════════
#   QR LOGIN WITH COUNTDOWN TIMER
# ═══════════════════════════════════════════════════════════════
QR_CL: dict = {}

async def do_qr(update: Update, ctx):
    uid = update.effective_user.id
    msg = update.callback_query.message if update.callback_query else update.message

    # Clean old
    old = QR_CL.pop(uid, None)
    if old:
        try: await old.disconnect()
        except: pass

    cl = TelegramClient(StringSession(), API_ID, API_HASH)
    await cl.connect(); QR_CL[uid] = cl; fp = f"qr_{uid}.png"

    prog = await msg.reply_text("⏳ Generating QR code...")

    try:
        qr = await cl.qr_login()
        qrcode.make(qr.url).save(fp)
        await prog.delete()

        # Send QR photo
        qrmsg = await msg.reply_photo(
            open(fp, "rb"),
            caption=(
                f"📱 *{BOT_NAME} Login*\n\n"
                f"1️⃣ Open Telegram\n"
                f"2️⃣ Settings → Devices\n"
                f"3️⃣ Link Device → Scan\n\n"
                f"⏳ Expires in: 2:00 min"
            ),
            parse_mode="Markdown"
        )
        if os.path.exists(fp): os.remove(fp)

        # Live countdown timer
        start_time = time.time()
        timer_msg  = await msg.reply_text("⏳ QR Timer: 2:00 remaining...")
        timer_task = asyncio.create_task(
            _qr_countdown(timer_msg, start_time, QR_TIMEOUT)
        )

        try:
            await asyncio.wait_for(qr.wait(), timeout=QR_TIMEOUT)
        except asyncio.TimeoutError:
            timer_task.cancel()
            try: await timer_msg.delete()
            except: pass
            try: await qrmsg.delete()
            except: pass
            await msg.reply_text(
                "⏰ QR Code Expired!\n\nTap below to try again.",
                reply_markup=KB([B("🔄  Try Again","qr_login")])
            )
            try: await cl.disconnect()
            except: pass
            return

        timer_task.cancel()
        try: await timer_msg.delete()
        except: pass

        # Login success
        sess = cl.session.save(); s_save(uid, sess)
        is_new = u_upsert(uid, update.effective_user.username or "", update.effective_user.first_name or "")
        me = await cl.get_me(); nm = me.first_name or ""; un = f"@{me.username}" if me.username else str(me.id)
        ACL[uid] = cl  # keep client alive

        # Start engine
        asyncio.create_task(eng_start(uid))

        # Pre-fetch chats
        fetch_msg = await msg.reply_text("📡 Fetching your channels...")
        asyncio.create_task(_fetch_and_delete(uid, fetch_msg))

        try: await qrmsg.delete()
        except: pass

        await msg.reply_text(
            f"✅ *Login Successful!*\n\n"
            f"👤 {nm}  ({un})\n"
            f"{'🎁 Free Trial: '+str(get_trial_days())+' days!' if is_new else '💳 '+u_sub_str(uid)}",
            parse_mode="Markdown",
            reply_markup=KB_MAIN(uid)
        )

    except Exception as e:
        LOG.error(f"QR:{e}"); await msg.reply_text(f"❌ Error: {e}\n/start karo")
        try: await cl.disconnect()
        except: pass
    finally:
        if os.path.exists(fp): os.remove(fp)
        QR_CL.pop(uid, None)

async def _qr_countdown(timer_msg, start_time, total):
    """Live countdown updates every 15 seconds."""
    try:
        intervals = [105, 90, 75, 60, 45, 30, 20, 10, 5]
        for sec in intervals:
            await asyncio.sleep(total - sec - (time.time() - start_time))
            if sec <= 0: break
            m = sec // 60; s = sec % 60
            try:
                await timer_msg.edit_text(f"⏳ QR Timer: {m}:{s:02d} remaining...")
            except: pass
    except asyncio.CancelledError:
        pass

async def _fetch_and_delete(uid, msg):
    await fetch_chats(uid)
    try: await msg.delete()
    except: pass

# ═══════════════════════════════════════════════════════════════
#   CHANNEL PICKER (numbered)
# ═══════════════════════════════════════════════════════════════
async def show_picker(uid, mode, page, edit_target, cid_dest=None):
    chats = CHAT_CACHE.get(uid, [])
    if not chats: chats = await fetch_chats(uid)
    if not chats:
        try: await edit_target.edit_text("❌ No channels found! Join some channels first.")
        except: await edit_target.reply_text("❌ No channels found!")
        return

    total = len(chats)
    list_txt, _ = chat_list_text(chats, page)

    if mode == 'src':
        hdr = (f"📡  Select SOURCE Channel\n"
               f"━━━━━━━━━━━━━━━━━━━━\n"
               f"Kaunsa channel se forward karna hai?\n\n")
        prefix = "ps"
    else:
        hdr = (f"📤  Select DESTINATION Channel\n"
               f"━━━━━━━━━━━━━━━━━━━━\n"
               f"Kahan forward karna hai?\n\n")
        prefix = f"pd_{cid_dest}"

    txt = hdr + list_txt + f"\n━━━━━━━━━━━━━━━━━━━━\n📋 Total: {total} channels"
    kb  = num_kb(chats, page, prefix, total)

    try: await edit_target.edit_text(txt, reply_markup=kb)
    except: await edit_target.reply_text(txt, reply_markup=kb)

# ═══════════════════════════════════════════════════════════════
#   STATE
# ═══════════════════════════════════════════════════════════════
TEMP: dict = {}
(ST_LO,ST_LN,ST_WO,ST_WN,ST_WL,ST_BL,
 ST_HDR,ST_FTR,ST_DLY,ST_AIP,ST_AIT,
 ST_REN,ST_BC,ST_GID,ST_GDY,ST_SRCH,
 ST_PLAN_PRICE,ST_PLAN_DAYS,ST_PLAN_NEW,ST_TRIAL) = range(20)

# ═══════════════════════════════════════════════════════════════
#   COMMANDS
# ═══════════════════════════════════════════════════════════════
async def cmd_start(u: Update, ctx):
    uid = u.effective_user.id
    # Admin bypass — no login required
    if uid in ADMIN_IDS:
        await u.message.reply_text("🔧 Admin Panel", reply_markup=KB_ADM()); return
    if s_get(uid):
        await u.message.reply_text(
            f"⚡ {BOT_NAME}  •  {u_sub_str(uid)}",
            reply_markup=KB_MAIN(uid)); return
    await u.message.reply_text(
        f"⚡ *{BOT_NAME}* — Pro Forwarder\n\n"
        f"📡 Auto channel detect\n"
        f"🤖 AI post rewriter\n"
        f"📋 Copy mode • 🔗 Replace\n"
        f"✂️ Filters • 📐 Format\n"
        f"🎁 {get_trial_days()} days free trial\n\n"
        f"Admin: {ADMIN_USERNAME}\n\n"
        f"👇 Tap to login — No OTP!",
        parse_mode="Markdown",
        reply_markup=KB([B("📱 Login with QR Code","qr_login")])
    )

async def cmd_menu(u: Update, ctx):
    uid = u.effective_user.id
    if uid in ADMIN_IDS:
        await u.message.reply_text("🔧 Admin Panel", reply_markup=KB_ADM()); return
    if not s_get(uid): await u.message.reply_text("Pehle /start karo!"); return
    await u.message.reply_text(f"⚡ {BOT_NAME}  •  {u_sub_str(uid)}", reply_markup=KB_MAIN(uid))

async def cmd_admin(u: Update, ctx):
    if u.effective_user.id not in ADMIN_IDS: return
    await u.message.reply_text("🔧 Admin Panel", reply_markup=KB_ADM())

async def cmd_cancel(u: Update, ctx):
    uid = u.effective_user.id; ctx.user_data.clear(); TEMP.pop(uid,None)
    await u.message.reply_text("❌ Cancelled.",
        reply_markup=KB_ADM() if uid in ADMIN_IDS else (KB_MAIN(uid) if s_get(uid) else None))

# ═══════════════════════════════════════════════════════════════
#   CALLBACK
# ═══════════════════════════════════════════════════════════════
async def cbk(update: Update, ctx):
    q = update.callback_query; await q.answer()
    d = q.data; uid = update.effective_user.id

    async def ED(txt, kb=None):
        try: await q.edit_message_text(txt, reply_markup=kb)
        except: await q.message.reply_text(txt, reply_markup=kb)

    async def ANS(m, alert=False): await q.answer(m, show_alert=alert)

    if d == "noop": return

    # ── QR ──
    if d == "qr_login": await do_qr(update, ctx); return

    # ── ADMIN (no session needed) ──
    if uid in ADMIN_IDS and d in ("main","adm") or (uid in ADMIN_IDS and d.startswith("a_")):
        await _admin_cbk(d, uid, q, ctx, ED, ANS); return

    # ── SESSION CHECK ──
    if not s_get(uid) and uid not in ADMIN_IDS:
        await q.message.reply_text("Pehle /start karo!"); return

    # ── MAIN ──
    if d == "main":
        if uid in ADMIN_IDS: await ED("🔧 Admin Panel", KB_ADM()); return
        await ED(f"⚡ {BOT_NAME}  •  {u_sub_str(uid)}", KB_MAIN(uid))

    elif d == "bot_tog":
        cur=u_bot_on(uid); u_toggle_bot(uid,not cur)
        await ANS(f"Bot {'🟢 ON' if not cur else '🔴 OFF'}",True)
        await ED(f"⚡ {BOT_NAME}  •  {u_sub_str(uid)}", KB_MAIN(uid))

    elif d == "eng_r":
        await eng_stop(uid); ok=await eng_start(uid)
        await ANS("✅ Engine Restarted!" if ok else "❌ Error!",True)
        await ED(f"⚡ {BOT_NAME}  •  {u_sub_str(uid)}", KB_MAIN(uid))

    elif d == "logout":
        await eng_stop(uid); s_del(uid); CHAT_CACHE.pop(uid,None)
        await ED("🚪 Logged out!\n/start se login karo.")

    elif d == "support":
        await ED(
            f"🆘  Support & Help\n\n"
            f"Admin: {ADMIN_USERNAME}\n\n"
            f"For:\n"
            f"• Subscription purchase\n"
            f"• Technical issues\n"
            f"• Plan renewal\n"
            f"• Custom features\n\n"
            f"Your ID: `{uid}`",
            KB([B(f"💬  Contact  {ADMIN_USERNAME}","noop"), B("‹ Back","main")])
        )

    elif d == "help":
        await ED(
            f"❓  Help Guide\n\n"
            f"SETUP (3 Steps):\n"
            f"  1. 📡 My Channels → Add New\n"
            f"  2. Select source (number tap)\n"
            f"  3. Select destination\n"
            f"  ✅ Done! Forwarding starts!\n\n"
            f"FEATURES:\n"
            f"  ⚙️ Control Panel = All ON/OFF\n"
            f"  🤖 AI = Auto rewrite posts\n"
            f"  🔗 Link Replace = URL swap\n"
            f"  📝 Word Replace = Text swap\n"
            f"  ✂️ Filters = Keyword rules\n"
            f"  📐 Format = Post styling\n\n"
            f"Support: {ADMIN_USERNAME}",
            KB([B("📡 Add Channel","ch_new"), B("‹ Back","main")])
        )

    # ── CHANNELS ──
    elif d == "chs": await ED("📡  My Channels", KB_CHS(uid))

    elif d == "ch_new":
        if not u_ok(uid): await ANS("❌ Subscription needed!",True); return
        chats=CHAT_CACHE.get(uid,[])
        if not chats:
            p=await q.message.reply_text("📡 Fetching channels...")
            chats=await fetch_chats(uid)
            try: await p.delete()
            except: pass
        await show_picker(uid,'src',0,q.message)

    elif d.startswith("ch_") and d[3:].isdigit():
        cid=int(d[3:]); ch=ch_get(cid)
        if not ch: return
        dests=json.loads(ch['dests'])
        await ED(
            f"📡  {ch['ch_name']}\n"
            f"▸ {ch['src_name']}\n"
            f"📤 {len(dests)} dest  •  {'🟢 ON' if ch['enabled'] else '🔴 OFF'}\n"
            f"🤖 AI: {AI_STYLES.get(ch.get('ai_style','none'),('—',))[0] if ch['ai_on'] else 'Off'}",
            KB_CH(cid)
        )

    elif d.startswith("ch_del_"):
        cid=int(d[7:]); ch_del(cid,uid)
        await eng_stop(uid); asyncio.create_task(eng_start(uid))
        await ANS("🗑 Deleted!",True); await ED("📡 My Channels",KB_CHS(uid))

    elif d.startswith("ch_tog_"):
        cid=int(d[7:]); new=ch_toggle(cid)
        await eng_stop(uid); asyncio.create_task(eng_start(uid))
        await ANS(f"{'🟢 Enabled' if new else '🔴 Disabled'}",True)
        r=ch_get(cid); await ED(f"📡 {r['ch_name']}",KB_CH(cid))

    elif d.startswith("ch_ren_"):
        cid=int(d[7:]); TEMP[uid]={'cid':cid}; ctx.user_data['st']=ST_REN
        await q.message.reply_text("✏️ New name bhejo:\n\n/cancel")

    elif d.startswith("ch_stat_"):
        cid=int(d[8:]); ch=ch_get(cid)
        c=DB()
        tot=c.execute("SELECT COUNT(*) as n FROM fwd_log WHERE ch_id=?",(cid,)).fetchone()['n']
        tod=c.execute("SELECT COUNT(*) as n FROM fwd_log WHERE ch_id=? AND ts>?",(cid,time.time()-86400)).fetchone()['n']
        c.close()
        fils=f_get(cid); lrp=rp_get(cid,'link'); wrp=rp_get(cid,'word')
        dests=json.loads(ch['dests'])
        await ED(
            f"📊  Stats: {ch['ch_name']}\n\n"
            f"📨 Forwarded Total: {tot}\n"
            f"📅 Today          : {tod}\n"
            f"📤 Destinations   : {len(dests)}\n"
            f"✂️  Filters        : {len(fils)}\n"
            f"🔗 Link Replaces  : {len(lrp)}\n"
            f"📝 Word Replaces  : {len(wrp)}\n"
            f"🤖 AI Style       : {AI_STYLES.get(ch.get('ai_style','none'),('—',))[0]}",
            KB([B("‹ Back",f"ch_{cid}"), B("🏠 Home","main")])
        )

    # ── PICKER ──
    elif d.startswith("pg_"):
        parts=d.split("_"); page=int(parts[-1])
        if "ps" in d:
            await show_picker(uid,'src',page,q.message)
        elif "pd_" in d:
            cid=int(parts[2]); await show_picker(uid,'dest',page,q.message,cid)

    elif d.startswith("refr_"):
        p=await q.message.reply_text("🔄 Refreshing...")
        await fetch_chats(uid)
        try: await p.delete()
        except: pass
        if "ps" in d: await show_picker(uid,'src',0,q.message)
        else:
            cid=TEMP.get(uid,{}).get('cid')
            await show_picker(uid,'dest',0,q.message,cid)

    elif d.startswith("ps_"):  # source picked
        idx=int(d[3:]); chats=CHAT_CACHE.get(uid,[])
        if idx>=len(chats): await ANS("❌ Invalid!"); return
        chat=chats[idx]
        cid=ch_add(uid,chat['id'],chat['name'],chat['name'][:25])
        asyncio.create_task(eng_start(uid))
        TEMP[uid]={'cid':cid}
        await show_picker(uid,'dest',0,q.message,cid)

    elif d.startswith("pd_"):  # dest picked
        parts=d.split("_"); cid=int(parts[1]); idx=int(parts[2])
        chats=CHAT_CACHE.get(uid,[])
        if idx>=len(chats): await ANS("❌ Invalid!"); return
        chat=chats[idx]
        ch_add_dest(cid,chat['id'],chat['name'])
        asyncio.create_task(eng_start(uid))
        ch=ch_get(cid)
        await ED(
            f"╔══ ✅ Setup Complete! ══╗\n\n"
            f"📡 Source : {ch['src_name']}\n"
            f"📤 Dest   : {chat['name']}\n\n"
            f"🎉 Forwarding is now active!\n\n"
            f"Customise your channel:\n╚{'═'*24}╝",
            IM([
                [B("➕ Add More Destinations",f"dest_pick_{cid}")],
                [B("⚙️ Control Panel",f"ctrl_{cid}"), B("🤖 AI Setup",f"ai_{cid}")],
                [B("🔗 Link Replace",f"lrp_{cid}"),  B("📝 Word Replace",f"wrp_{cid}")],
                [B("📡 My Channels","chs"),            B("🏠 Home","main")],
            ])
        )

    elif d.startswith("dest_pick_"):
        cid=int(d[10:]); TEMP[uid]={'cid':cid}
        await show_picker(uid,'dest',0,q.message,cid)

    # ── CONTROL ──
    elif d == "ctrl_g": await ED("⚙️ Global Settings", KB_CTRL_G(uid))
    elif d.startswith("ctrl_") and d[5:].isdigit():
        cid=int(d[5:]); await ED("⚙️ Control Panel", KB_CTRL(cid))

    elif d.startswith("t_copy_"):
        cid=int(d[7:]); ch=ch_get(cid)
        if not ch: await ANS("❌",True); return
        ch_upd(cid,copy_mode=0 if ch['copy_mode'] else 1); await ED("⚙️",KB_CTRL(cid))
    elif d.startswith("t_sil_"):
        cid=int(d[6:]); ch=ch_get(cid)
        if not ch: await ANS("❌",True); return
        ch_upd(cid,silent=0 if ch['silent'] else 1); await ED("⚙️",KB_CTRL(cid))
    elif d.startswith("t_pin_"):
        cid=int(d[6:]); ch=ch_get(cid)
        if not ch: await ANS("❌",True); return
        ch_upd(cid,pin_msg=0 if ch['pin_msg'] else 1); await ED("⚙️",KB_CTRL(cid))
    elif d.startswith("t_cap_"):
        cid=int(d[6:]); ch=ch_get(cid)
        if not ch: await ANS("❌",True); return
        ch_upd(cid,remove_cap=0 if ch['remove_cap'] else 1); await ED("⚙️",KB_CTRL(cid))
    elif d.startswith("t_med_"):
        cid=int(d[6:]); ch=ch_get(cid)
        if not ch: await ANS("❌",True); return
        ch_upd(cid,media_only=0 if ch['media_only'] else 1,text_only=0); await ED("⚙️",KB_CTRL(cid))
    elif d.startswith("t_txt_"):
        cid=int(d[6:]); ch=ch_get(cid)
        if not ch: await ANS("❌",True); return
        ch_upd(cid,text_only=0 if ch['text_only'] else 1,media_only=0); await ED("⚙️",KB_CTRL(cid))
    elif d.startswith("t_blk_"):
        cid=int(d[6:]); ch=ch_get(cid)
        if not ch: await ANS("❌ Not found!",True); return
        ch_upd(cid,block_links=0 if ch['block_links'] else 1); await ED("⚙️",KB_CTRL(cid))
    elif d.startswith("t_ball_"):
        cid=int(d[7:]); ch=ch_get(cid)
        if not ch: await ANS("❌ Not found!",True); return
        current = bool(ch.get('block_all_links') or ch.get('block_links'))
        new_val = 0 if current else 1
        # Only links — @username is separate, NOT touched here
        ch_upd(cid,
            block_all_links=new_val,
            block_links=new_val,
            block_www=new_val,
            block_tme=new_val,
        )
        await ANS(f"🔴 ALL Links BLOCKED!" if new_val else "⚪ Link blocking OFF", True)
        await ED("✂️ Filters", KB_FLT(cid))
    elif d.startswith("t_bwww_"):
        cid=int(d[7:]); ch=ch_get(cid)
        if not ch: await ANS("❌ Not found!",True); return
        ch_upd(cid,block_www=0 if ch.get('block_www') else 1); await ED("✂️ Filters",KB_FLT(cid))
    elif d.startswith("t_btme_"):
        cid=int(d[7:]); ch=ch_get(cid)
        if not ch: await ANS("❌ Not found!",True); return
        ch_upd(cid,block_tme=0 if ch.get('block_tme') else 1); await ED("✂️ Filters",KB_FLT(cid))
    elif d.startswith("t_bat_"):
        cid=int(d[6:]); ch=ch_get(cid)
        if not ch: await ANS("❌ Not found!",True); return
        ch_upd(cid,block_at=0 if ch.get('block_at') else 1); await ED("✂️ Filters",KB_FLT(cid))
    elif d.startswith("t_dup_"):
        cid=int(d[6:]); ch=ch_get(cid)
        if not ch: await ANS("❌",True); return
        ch_upd(cid,dup_check=0 if ch['dup_check'] else 1); await ED("⚙️",KB_CTRL(cid))
    elif d.startswith("t_bold_"):
        cid=int(d[7:]); ch=ch_get(cid)
        if not ch: await ANS("❌",True); return
        ch_upd(cid,fmt_bold=0 if ch['fmt_bold'] else 1); await ED("📐",KB_FMT(cid))
    elif d.startswith("t_clean_"):
        cid=int(d[8:]); ch=ch_get(cid)
        if not ch: await ANS("❌",True); return
        ch_upd(cid,fmt_clean=0 if ch['fmt_clean'] else 1); await ED("📐",KB_FMT(cid))

    elif d.startswith("s_dly_"): cid=int(d[6:]); TEMP[uid]={'cid':cid}; ctx.user_data['st']=ST_DLY; await q.message.reply_text("⏱ Delay seconds (0-3600):\n\n/cancel")
    elif d.startswith("s_hdr_"): cid=int(d[6:]); TEMP[uid]={'cid':cid}; ctx.user_data['st']=ST_HDR; await q.message.reply_text("📝 Header text (- for none):\n\n/cancel")
    elif d.startswith("s_ftr_"): cid=int(d[6:]); TEMP[uid]={'cid':cid}; ctx.user_data['st']=ST_FTR; await q.message.reply_text("📝 Footer text (- for none):\n\n/cancel")

    # ── FORMAT ──
    elif d.startswith("fmt_"): cid=int(d[4:]); await ED("📐 Format",KB_FMT(cid))

    # ── AI ──
    elif d == "ai_g": await ED("🤖 AI Settings",KB_AI_G(uid))
    elif d.startswith("ai_") and d[3:].isdigit(): cid=int(d[3:]); await ED("🤖 AI",KB_AI(cid))
    elif d.startswith("t_ai_"):     cid=int(d[5:]); ch=ch_get(cid); ch_upd(cid,ai_on=0 if ch['ai_on'] else 1); await ED("🤖",KB_AI(cid))
    elif d.startswith("ai_s_"):
        parts=d.split("_"); cid=int(parts[2]); style="_".join(parts[3:])
        ch_upd(cid,ai_style=style); (ch_upd(cid,ai_on=1) if style!='none' else None)
        await ANS(f"✅ {AI_STYLES.get(style,('?',))[0]}",True); await ED("🤖",KB_AI(cid))
    elif d.startswith("ai_cp_"):    cid=int(d[6:]); TEMP[uid]={'cid':cid}; ctx.user_data['st']=ST_AIP; await q.message.reply_text("✏️ Custom AI prompt:\n\nExample: 'Rewrite as breaking news in Hindi'\n\n/cancel")
    elif d.startswith("ai_t_"):     cid=int(d[5:]); TEMP[uid]={'cid':cid}; ctx.user_data['st']=ST_AIT; await q.message.reply_text("🧪 Test text bhejo:\n\n/cancel")

    # ── REPLACEMENTS ──
    elif d == "repl_g": await ED("🔄 Replacements",KB_REPL_G(uid))
    elif d.startswith("repls_"):    cid=int(d[6:]); await ED("🔄",KB_REPLS(cid))
    elif d.startswith("lrp_"):
        cid=int(d[4:]); lrp=rp_get(cid,'link')
        rows=[[B(f"🔗 Link Replace ({len(lrp)})","noop")]]
        for r in lrp: rows.append([B(f"🔗 {r['old_val'][:24]}→{r['new_val'][:14]}","noop"),B("🗑",f"d_rp_{r['id']}")])
        rows.append([B("➕ Add",f"add_lrp_{cid}")]); rows.append([B("‹ Back",f"ch_{cid}"),B("🏠","main")])
        await ED("🔗 Link Replace",IM(rows))
    elif d.startswith("wrp_"):
        cid=int(d[4:]); wrp=rp_get(cid,'word')
        rows=[[B(f"📝 Word Replace ({len(wrp)})","noop")]]
        for r in wrp: rows.append([B(f"📝 {r['old_val'][:24]}→{r['new_val'][:14]}","noop"),B("🗑",f"d_rp_{r['id']}")])
        rows.append([B("➕ Add",f"add_wrp_{cid}")]); rows.append([B("‹ Back",f"ch_{cid}"),B("🏠","main")])
        await ED("📝 Word Replace",IM(rows))
    elif d.startswith("add_lrp_"): cid=int(d[8:]); TEMP[uid]={'cid':cid,'rt':'link'}; ctx.user_data['st']=ST_LO; await q.message.reply_text("🔗 Purana link bhejo:\n\n/cancel")
    elif d.startswith("add_wrp_"): cid=int(d[8:]); TEMP[uid]={'cid':cid,'rt':'word'}; ctx.user_data['st']=ST_WO; await q.message.reply_text("📝 Purana word bhejo:\n\n/cancel")
    elif d.startswith("d_rp_"): rp_del(int(d[5:])); await ANS("✅ Removed!")

    # ── FILTERS ──
    elif d.startswith("flt_"):
        cid=int(d[4:]); ch=ch_get(cid)
        if not ch: await ANS("❌ Not found!",True); return
        await ED("✂️ Filters",KB_FLT(cid))
    elif d.startswith("add_wl_"): cid=int(d[7:]); TEMP[uid]={'cid':cid,'ft':'whitelist'}; ctx.user_data['st']=ST_WL; await q.message.reply_text("✅ Whitelist word:\n(Sirf ye forward honge)\n\n/cancel")
    elif d.startswith("add_bl_"): cid=int(d[7:]); TEMP[uid]={'cid':cid,'ft':'blacklist'}; ctx.user_data['st']=ST_BL; await q.message.reply_text("🚫 Blacklist word:\n(Ye skip honge)\n\n/cancel")
    elif d.startswith("d_flt_"): f_del(int(d[6:])); await ANS("✅ Removed!")

    # ── DESTINATIONS ──
    elif d.startswith("dests_"): cid=int(d[6:]); await ED("📤 Destinations",KB_DESTS(cid))
    elif d.startswith("d_dest_"):
        parts=d.split("_"); cid=int(parts[2]); did=parts[3]
        ch_del_dest(cid,did); asyncio.create_task(eng_start(uid))
        await ANS("✅ Removed!"); await ED("📤",KB_DESTS(cid))

    # ── SUBSCRIPTION ──
    elif d == "sub": await ED("💳 Subscription",KB_SUB(uid))
    elif d == "sub_renew":
        plans = [p for p in plan_all() if p['enabled']]
        rows = [B(f"{p['label']}  ₹{p['price']}  {p['days']}d", f"buy_{p['key']}") for p in plans]
        rows_kb = [[r] for r in rows] + [[B("‹ Back","sub")]]
        await ED(
            f"🔄 Plan Renewal\n\nCurrent: {u_sub_str(uid)}\nExpires: {u_sub_end_str(uid)}\n\nPlan choose karo 👇",
            IM(rows_kb)
        )
    elif d.startswith("buy_"):
        pk=d[4:]; pl=plan_get(pk)
        if not pl: await ANS("❌ Plan not found!",True); return
        label=pl['label']; days=pl['days']; price=pl['price']
        await ED(
            f"💳 {label} Plan\n\n"
            f"💰 Price  : ₹{price}\n"
            f"⏳ Period : {days} days\n\n"
            f"Payment Steps:\n"
            f"━━━━━━━━━━━━━━\n"
            f"1️⃣  UPI: (contact admin)\n"
            f"2️⃣  Amount: ₹{price}\n"
            f"3️⃣  Screenshot lo\n"
            f"4️⃣  Admin ko bhejo: {ADMIN_USERNAME}\n"
            f"5️⃣  Write: Plan:{label} | ID:{uid}\n\n"
            f"✅  Admin activates in 5-10 min",
            KB([B(f"💬 Contact {ADMIN_USERNAME}","support"),B("‹ Back","sub")])
        )
    elif d == "contact_admin": await ED(f"💬 Admin: {ADMIN_USERNAME}\n\nYour ID: `{uid}`",KB([B("‹ Back","sub")]))

    # ── STATS ──
    elif d == "stats":
        tf,td,tw=l_stats(uid); chs=ch_all(uid)
        on_c=sum(1 for c in chs if c['enabled']); ai_c=sum(1 for c in chs if c['ai_on'])
        eng="🟢 Running" if uid in ACL else "🔴 Stopped"
        await ED(
            f"📊  My Statistics\n\n"
            f"📨 Total Forwarded : {tf}\n"
            f"📅 Today           : {td}\n"
            f"📆 This Week       : {tw}\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"📡 Channels        : {len(chs)}\n"
            f"🟢 Active          : {on_c}\n"
            f"🤖 AI Enabled      : {ai_c}\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"⚡ Engine          : {eng}\n"
            f"💳 {u_sub_str(uid)}",
            KB([B("‹ Back","main")])
        )

# ─── Admin callbacks ─────────────────────────────────────────────
async def _admin_cbk(d, uid, q, ctx, ED, ANS):
    if d in ("main","adm"): await ED("🔧 Admin Panel", KB_ADM()); return

    elif d == "a_users":
        users=u_all(); lines=[]
        for u in users[:25]:
            st="✅" if u_ok(u['uid']) else "❌"; bn="🚫" if u['is_banned'] else ""
            lines.append(f"{st}{bn} {u.get('name','?')[:12]} | `{u['uid']}` | {u['total_fwd']}fwd")
        await ED(f"👥 All Users ({len(users)})\n\n"+"\n".join(lines)+(f"\n...+{len(users)-25} more" if len(users)>25 else ""),KB([B("‹ Back","adm")]))

    elif d == "a_active":
        users=[u for u in u_all() if u_ok(u['uid'])]
        lines=[f"✅ {u.get('name','?')[:14]} | `{u['uid']}` | {u_sub_str(u['uid'])}" for u in users[:25]]
        await ED(f"✅ Active Subs ({len(users)})\n\n"+"\n".join(lines or ["None"]),KB([B("‹ Back","adm")]))

    elif d == "a_stats":
        tu,au,tf,tr,tb,td,nw=adm_stats()
        import datetime; today=datetime.date.today().strftime("%d %b %Y")
        await ED(
            f"📊 Admin Stats  •  {today}\n\n"
            f"👥 Total Users   : {tu}\n"
            f"✅ Active Subs   : {au}\n"
            f"❌ No Sub        : {tu-au-tb}\n"
            f"🚫 Banned        : {tb}\n"
            f"🆕 New (7d)      : {nw}\n"
            f"⚡ Live Engines  : {len(ACL)}\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"📨 Total Fwd     : {tf}\n"
            f"📅 Today Fwd     : {td}\n"
            f"📡 Total Channels: {tr}",
            KB([B("‹ Back","adm")])
        )

    elif d == "a_revenue":
        users=u_all(); au=sum(1 for u in users if u_ok(u['uid']))
        plans = plan_all()
        lines = "\n".join([f"  {p['label']} ₹{p['price']} × ? users" for p in plans])
        m_plan = next((p['price'] for p in plans if p['key']=='m'), 49)
        await ED(
            f"💰 Revenue Dashboard\n\n"
            f"✅ Active Subscribers: {au}\n"
            f"💵 Est. Revenue (all monthly): ₹{au*m_plan}\n\n"
            f"Plans:\n{lines}",
            KB([B("💳 Manage Plans","a_plans"), B("‹ Back","adm")])
        )

    elif d == "a_chs":
        c=DB()
        rows=c.execute("SELECT ch.id,ch.ch_name,ch.uid,ch.enabled,u.name FROM channels ch LEFT JOIN users u ON ch.uid=u.uid ORDER BY ch.id DESC LIMIT 30").fetchall()
        c.close()
        lines=[f"{'🟢' if r['enabled'] else '🔴'} {r['ch_name'][:16]} | uid:{r['uid']}" for r in rows]
        await ED(f"📡 All Channels ({len(rows)})\n\n"+"\n".join(lines or ["None"]),KB([B("‹ Back","adm")]))

    elif d == "a_restart":
        await ANS("🔄 Restarting all...",True); asyncio.create_task(eng_restart_all())

    elif d == "a_search":
        ctx.user_data['st']=ST_SRCH; await q.message.reply_text("🔍 User ID or username bhejo:\n\n/cancel")

    elif d == "a_give":   ctx.user_data['st']=ST_GID; ctx.user_data['sa']='give';   await q.message.reply_text("✅ User ID (sub deni hai):\n\n/cancel")
    elif d == "a_revoke": ctx.user_data['st']=ST_GID; ctx.user_data['sa']='revoke'; await q.message.reply_text("❌ User ID (sub revoke):\n\n/cancel")
    elif d == "a_ban":    ctx.user_data['st']=ST_GID; ctx.user_data['sa']='ban';    await q.message.reply_text("🚫 User ID (ban karna):\n\n/cancel")
    elif d == "a_unban":  ctx.user_data['st']=ST_GID; ctx.user_data['sa']='unban';  await q.message.reply_text("✅ User ID (unban karna):\n\n/cancel")
    elif d == "a_bc_all": ctx.user_data['st']=ST_BC; ctx.user_data['bt']='all';    await q.message.reply_text("📢 Message (ALL users):\n\n/cancel")
    elif d == "a_bc_act": ctx.user_data['st']=ST_BC; ctx.user_data['bt']='active'; await q.message.reply_text("📢 Message (ACTIVE only):\n\n/cancel")

    # ── PLAN MANAGEMENT ──
    elif d == "a_plans":
        await ED("💳 Plan Management", KB_PLANS())

    elif d == "a_trial":
        ctx.user_data['st'] = ST_TRIAL
        await q.message.reply_text(
            f"🎁 Trial Days change karo\n\n"
            f"Current: {get_trial_days()} days\n\n"
            f"Naya number bhejo (e.g. 7):\n\n/cancel"
        )

    elif d.startswith("a_plan_tog_"):
        pk = d[11:]
        p = plan_get(pk)
        if p:
            plan_upd(pk, enabled=0 if p['enabled'] else 1)
            await ANS(f"{'🟢 Enabled' if not p['enabled'] else '🔴 Disabled'}", True)
        await ED("💳 Plan Management", KB_PLANS())

    elif d.startswith("a_plan_del_"):
        pk = d[11:]
        plan_del(pk)
        await ANS("🗑 Plan deleted!", True)
        await ED("💳 Plan Management", KB_PLANS())

    elif d.startswith("a_plan_edit_"):
        pk = d[12:]
        p = plan_get(pk)
        if not p: return
        TEMP[uid] = {'plan_key': pk, 'step': 'price'}
        ctx.user_data['st'] = ST_PLAN_PRICE
        await q.message.reply_text(
            f"✏️ Edit Plan: {p['label']}\n\n"
            f"Current Price: ₹{p['price']}\n"
            f"Current Days: {p['days']}\n\n"
            f"Naya PRICE bhejo (₹):\n\n/cancel"
        )

    elif d == "a_plan_add":
        TEMP[uid] = {'step': 'new_plan'}
        ctx.user_data['st'] = ST_PLAN_NEW
        await q.message.reply_text(
            "➕ New Plan Add\n\n"
            "Format mein bhejo:\n"
            "LABEL,DAYS,PRICE\n\n"
            "Example:\n"
            "💎 Premium,90,149\n\n/cancel"
        )

# ═══════════════════════════════════════════════════════════════
#   MESSAGE HANDLER
# ═══════════════════════════════════════════════════════════════
async def msg_hdl(update: Update, ctx):
    uid=update.effective_user.id; txt=(update.message.text or "").strip()
    st=ctx.user_data.get('st')

    if txt=="/cancel":
        ctx.user_data.clear(); TEMP.pop(uid,None)
        kb=KB_ADM() if uid in ADMIN_IDS else (KB_MAIN(uid) if s_get(uid) else None)
        await update.message.reply_text("❌ Cancelled.",reply_markup=kb); return

    def _cid(): return TEMP.get(uid,{}).get('cid')

    if st==ST_REN:
        ch_upd(_cid(),ch_name=txt[:30]); ctx.user_data.pop('st',None); TEMP.pop(uid,None)
        await update.message.reply_text(f"✅ Renamed: {txt[:30]}",reply_markup=KB([B("‹ Back",f"ch_{_cid() or 0}")]))

    elif st==ST_LO:
        TEMP.setdefault(uid,{})['old']=txt; ctx.user_data['st']=ST_LN
        await update.message.reply_text(f"✅ Old: `{txt}`\nNaya bhejo:\n\n/cancel",parse_mode="Markdown")
    elif st==ST_LN:
        old=TEMP.get(uid,{}).get('old',''); rp_add(_cid(),uid,'link',old,txt)
        ctx.user_data.pop('st',None); TEMP.pop(uid,None)
        await update.message.reply_text(f"✅ `{old}` → `{txt}`",parse_mode="Markdown",reply_markup=KB([B("➕ More",f"add_lrp_{_cid() or 0}"),B("‹ Back",f"lrp_{_cid() or 0}")]))

    elif st==ST_WO:
        TEMP.setdefault(uid,{})['old']=txt; ctx.user_data['st']=ST_WN
        await update.message.reply_text(f"✅ Old: `{txt}`\nNaya bhejo:\n\n/cancel",parse_mode="Markdown")
    elif st==ST_WN:
        old=TEMP.get(uid,{}).get('old',''); rp_add(_cid(),uid,'word',old,txt)
        ctx.user_data.pop('st',None); TEMP.pop(uid,None)
        await update.message.reply_text(f"✅ `{old}` → `{txt}`",parse_mode="Markdown",reply_markup=KB([B("➕ More",f"add_wrp_{_cid() or 0}"),B("‹ Back",f"wrp_{_cid() or 0}")]))

    elif st in (ST_WL,ST_BL):
        ft=TEMP.get(uid,{}).get('ft','whitelist'); f_add(_cid(),uid,ft,txt)
        ctx.user_data.pop('st',None); TEMP.pop(uid,None)
        icon="✅" if ft=='whitelist' else "🚫"
        await update.message.reply_text(f"{icon} `{txt}`",parse_mode="Markdown",reply_markup=KB([B("➕ More",f"add_{'wl' if ft=='whitelist' else 'bl'}_{_cid() or 0}"),B("‹ Back",f"flt_{_cid() or 0}")]))

    elif st==ST_DLY:
        try: v=max(0,min(3600,int(txt))); ch_upd(_cid(),delay_sec=v); await update.message.reply_text(f"✅ Delay: {v}s",reply_markup=KB([B("‹ Back",f"ctrl_{_cid() or 0}")]))
        except: await update.message.reply_text("❌ Number (0-3600) bhejo!")
        ctx.user_data.pop('st',None); TEMP.pop(uid,None)

    elif st==ST_HDR:
        ch_upd(_cid(),header="" if txt=="-" else txt); ctx.user_data.pop('st',None); TEMP.pop(uid,None)
        await update.message.reply_text(f"✅ Header {'cleared' if txt=='-' else 'set'}!",reply_markup=KB([B("‹ Back",f"ctrl_{_cid() or 0}")]))

    elif st==ST_FTR:
        ch_upd(_cid(),footer="" if txt=="-" else txt); ctx.user_data.pop('st',None); TEMP.pop(uid,None)
        await update.message.reply_text(f"✅ Footer {'cleared' if txt=='-' else 'set'}!",reply_markup=KB([B("‹ Back",f"ctrl_{_cid() or 0}")]))

    elif st==ST_AIP:
        ch_upd(_cid(),ai_prompt=txt,ai_style='custom',ai_on=1)
        ctx.user_data.pop('st',None); TEMP.pop(uid,None)
        await update.message.reply_text("✅ Custom AI saved! 🤖",reply_markup=KB([B("🧪 Test",f"ai_t_{_cid() or 0}"),B("‹ Back",f"ai_{_cid() or 0}")]))

    elif st==ST_AIT:
        cid=_cid(); ch=ch_get(cid) if cid else None
        ctx.user_data.pop('st',None); TEMP.pop(uid,None)
        if not ch: await update.message.reply_text("❌ Channel not found."); return
        await update.message.reply_text("⏳ AI rewriting...")
        result=await ai_rewrite(txt,ch)
        style=AI_STYLES.get(ch.get('ai_style','none'),('?','?'))
        await update.message.reply_text(
            f"🤖 AI Test ({style[0]})\n\n"
            f"📥 Original:\n{txt[:200]}\n\n"
            f"📤 Rewritten:\n{result[:500]}",
            reply_markup=KB([B("⚙️ Change Style",f"ai_{cid}"),B("🏠 Home","main")])
        )

    elif st==ST_BC:
        if uid not in ADMIN_IDS: return
        bt=ctx.user_data.get('bt','all'); all_u=u_all()
        targets=[u for u in all_u if bt=='all' or u_ok(u['uid'])]
        ctx.user_data.pop('st',None); sent=0
        for u in targets:
            try:
                await ctx.bot.send_message(u['uid'],
                    f"📢  {BOT_NAME}\n━━━━━━━━━━━━━━━\n{txt}\n━━━━━━━━━━━━━━━\n{ADMIN_USERNAME}")
                sent+=1; await asyncio.sleep(0.05)
            except: pass
        await update.message.reply_text(f"✅ Broadcast: {sent}/{len(targets)} sent!",reply_markup=KB_ADM())

    elif st==ST_GID:
        if uid not in ADMIN_IDS: return
        try:
            tid=int(txt); act=ctx.user_data.get('sa','give')
            if act in ('revoke','ban','unban'):
                if act=='revoke': u_revoke(tid); msg2="❌ Your subscription has been revoked."; ok=f"✅ Revoked: {tid}"
                elif act=='ban': u_ban(tid,True); await eng_stop(tid); msg2="🚫 You have been banned from the bot."; ok=f"🚫 Banned: {tid}"
                else: u_ban(tid,False); msg2=f"✅ You have been unbanned! /start to login."; ok=f"✅ Unbanned: {tid}"
                try: await ctx.bot.send_message(tid,msg2)
                except: pass
                await update.message.reply_text(ok,reply_markup=KB_ADM())
                ctx.user_data.pop('st',None)
            else:
                TEMP[uid]={'tid':tid}; ctx.user_data['st']=ST_GDY
                await update.message.reply_text(f"User: {tid}\nKitne din ki sub deni hai?")
        except: await update.message.reply_text("❌ Valid User ID bhejo!")

    elif st==ST_GDY:
        if uid not in ADMIN_IDS: return
        try:
            days=int(txt); tid=TEMP.get(uid,{}).get('tid')
            u_give(tid,days)
            try: await ctx.bot.send_message(tid,
                f"🎉  Subscription Activated!\n\n"
                f"✅  {days} Days Plan\n"
                f"⚡  Enjoy {BOT_NAME} Pro!\n\n"
                f"Support: {ADMIN_USERNAME}")
            except: pass
            await update.message.reply_text(f"✅ {days} days → {tid}",reply_markup=KB_ADM())
        except: await update.message.reply_text("❌ Number bhejo!")
        ctx.user_data.pop('st',None); TEMP.pop(uid,None)

    elif st==ST_SRCH:
        if uid not in ADMIN_IDS: return
        ctx.user_data.pop('st',None)
        try:
            tid=int(txt); u=u_get(tid)
            if u:
                chs=ch_all(tid); tf,td,_=l_stats(tid)
                await update.message.reply_text(
                    f"👤 User Found\n\n"
                    f"Name : {u.get('name','?')}\n"
                    f"ID   : `{u['uid']}`\n"
                    f"Sub  : {u_sub_str(tid)}\n"
                    f"Chs  : {len(chs)}\n"
                    f"Fwd  : {tf}  (Today: {td})\n"
                    f"Ban  : {'Yes' if u['is_banned'] else 'No'}",
                    reply_markup=IM([
                        [B("✅ Give Sub",f"a_give"),    B("❌ Revoke",f"a_revoke")],
                        [B("🚫 Ban",f"a_ban"),          B("✅ Unban",f"a_unban")],
                        [B("‹ Back","adm")],
                    ])
                )
            else: await update.message.reply_text("❌ User not found.",reply_markup=KB_ADM())
        except: await update.message.reply_text("❌ Valid User ID bhejo!",reply_markup=KB_ADM())

    # ── PLAN EDITING STATES ──────────────────────────────────────
    elif st==ST_TRIAL:
        if uid not in ADMIN_IDS: return
        ctx.user_data.pop('st',None)
        try:
            days = max(0, int(txt))
            setting_set('trial_days', days)
            await update.message.reply_text(
                f"✅ Trial updated: {days} days\n\nNaye users ko {days} days trial milega.",
                reply_markup=KB([B("💳 Plans","a_plans"), B("‹ Admin","adm")])
            )
        except:
            await update.message.reply_text("❌ Number bhejo! e.g. 7")

    elif st==ST_PLAN_PRICE:
        if uid not in ADMIN_IDS: return
        pk = TEMP.get(uid,{}).get('plan_key')
        try:
            price = max(0, int(txt))
            TEMP.setdefault(uid,{})['new_price'] = price
            TEMP[uid]['step'] = 'days'
            ctx.user_data['st'] = ST_PLAN_DAYS
            p = plan_get(pk)
            await update.message.reply_text(
                f"✅ Price: ₹{price}\n\n"
                f"Ab DAYS bhejo (current: {p['days']}):\n\n/cancel"
            )
        except:
            await update.message.reply_text("❌ Valid price bhejo! e.g. 49")

    elif st==ST_PLAN_DAYS:
        if uid not in ADMIN_IDS: return
        pk = TEMP.get(uid,{}).get('plan_key')
        new_price = TEMP.get(uid,{}).get('new_price')
        ctx.user_data.pop('st',None); TEMP.pop(uid,None)
        try:
            days = max(1, int(txt))
            plan_upd(pk, price=new_price, days=days)
            p = plan_get(pk)
            await update.message.reply_text(
                f"✅ Plan Updated!\n\n"
                f"Plan : {p['label']}\n"
                f"Price: ₹{new_price}\n"
                f"Days : {days}",
                reply_markup=KB([B("💳 Plans","a_plans"), B("‹ Admin","adm")])
            )
        except:
            await update.message.reply_text("❌ Valid days bhejo! e.g. 30")

    elif st==ST_PLAN_NEW:
        if uid not in ADMIN_IDS: return
        ctx.user_data.pop('st',None); TEMP.pop(uid,None)
        try:
            parts = txt.split(',')
            if len(parts) != 3: raise ValueError
            label = parts[0].strip()
            days  = max(1, int(parts[1].strip()))
            price = max(0, int(parts[2].strip()))
            # Generate unique key
            import hashlib
            key = hashlib.md5(label.encode()).hexdigest()[:4]
            plan_add(key, label, days, price)
            await update.message.reply_text(
                f"✅ New Plan Added!\n\n"
                f"Plan : {label}\n"
                f"Days : {days}\n"
                f"Price: ₹{price}",
                reply_markup=KB([B("💳 Plans","a_plans"), B("‹ Admin","adm")])
            )
        except:
            await update.message.reply_text(
                "❌ Format galat hai!\n\n"
                "Sahi format:\nLABEL,DAYS,PRICE\n\n"
                "Example:\n💎 Premium,90,149"
            )

# ═══════════════════════════════════════════════════════════════
#   MAIN
# ═══════════════════════════════════════════════════════════════
async def post_init(app):
    db_init()
    await app.bot.set_my_commands([
        BotCommand("start",  "Open bot"),
        BotCommand("menu",   "Dashboard"),
        BotCommand("admin",  "Admin panel"),
        BotCommand("cancel", "Cancel"),
    ])
    # Delay startup so event loop is fully ready
    async def _delayed_restart():
        await asyncio.sleep(2)
        await eng_restart_all()
    asyncio.create_task(_delayed_restart())
    LOG.info(f"✅ {BOT_NAME} {BOT_VERSION} Ready!")

def main():
    from telegram.request import HTTPXRequest
    request = HTTPXRequest(
        read_timeout=10,
        write_timeout=10,
        connect_timeout=10,
        pool_timeout=3,
    )
    app = Application.builder().token(BOT_TOKEN).request(request).post_init(post_init).concurrent_updates(True).build()
    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("menu",   cmd_menu))
    app.add_handler(CommandHandler("admin",  cmd_admin))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CallbackQueryHandler(cbk))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, msg_hdl))
    print(f"\n{'═'*55}")
    print(f"  🚀  {BOT_NAME}  {BOT_VERSION}  —  Final Edition")
    print(f"  QR Timer | Auto Channels | Admin No-Login")
    print(f"  Admin: {ADMIN_USERNAME}")
    print(f"{'═'*55}\n")
    app.run_polling(
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES,
    )

if __name__ == "__main__":
    main()
