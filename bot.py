"""
╔══════════════════════════════════════════════════════════════╗
║       🚀 FORWARD PRO BOT — FINAL EDITION v9.0              ║
║  Individual Media Toggles | Para Block | Multi-Style       ║
╚══════════════════════════════════════════════════════════════╝
SETUP:
  pip install python-telegram-bot==22.7 telethon qrcode[pil] tgcrypto httpx
  py bot.py
"""

# ═══════════════════════════════════════════════════════════════
#   ⚙️  CONFIG
# ═══════════════════════════════════════════════════════════════
API_ID         = 29770180
API_HASH       = "e4452a45c8d4c9d0d7250f8017033472"
BOT_TOKEN      = "8336442095:AAF6doNdq6Hdr3kUGrZH0hdOge8eDIt0G2U"
ADMIN_IDS      = [8660435467]
ADMIN_USERNAME = "@Shaan_Malik_Official"
CLAUDE_KEY     = "your-anthropic-key"
BOT_NAME       = "Advanced Forward Bot"
BOT_VERSION    = "v9.0"
TRIAL_DAYS     = 7
QR_TIMEOUT     = 30
CHATS_PER_PAGE = 14
# ═══════════════════════════════════════════════════════════════

import asyncio, os, time, json, re, logging, sqlite3, qrcode, sys
DB_PATH = "fpro.db"

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from telegram import Update, InlineKeyboardButton as IB, InlineKeyboardMarkup as IM, BotCommand
from telegram.ext import (Application, CommandHandler, CallbackQueryHandler,
                           MessageHandler, ContextTypes, filters as tg_filters)
from telethon import TelegramClient, events
from telethon.tl.types import (
    Channel, Chat, MessageMediaWebPage, MessageMediaPhoto, MessageMediaDocument,
    DocumentAttributeSticker, DocumentAttributeVideo, DocumentAttributeAnimated,
    DocumentAttributeAudio,
)
from telethon.sessions import StringSession

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
LOG = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
#   MEDIA TYPE DEFINITIONS
#   Each entry: key -> (emoji_label, db_column)
#   db_column is stored in channels table as blk_<key>
# ═══════════════════════════════════════════════════════════════
MEDIA_TYPES = {
    "photo":      ("🖼️  Photo",           "blk_photo"),
    "screenshot": ("📸 Screenshot",        "blk_screenshot"),
    "video":      ("🎬 Video",             "blk_video"),
    "vidmsg":     ("⭕ Video Message",     "blk_vidmsg"),
    "voice":      ("🎤 Voice Message",     "blk_voice"),
    "audio":      ("🎵 Audio / Music",     "blk_audio"),
    "doc":        ("📄 Document / File",   "blk_doc"),
    "sticker":    ("😄 Sticker",           "blk_sticker"),
    "gif":        ("🎞️  GIF / Animation",  "blk_gif"),
}

# Ordered display groups for the UI
MEDIA_GROUPS = [
    ("── 🖼️  Images ──────────────────", ["photo", "screenshot"]),
    ("── 🎬  Video ───────────────────", ["video", "vidmsg"]),
    ("── 🎤  Audio ───────────────────", ["voice", "audio"]),
    ("── 📎  Other ───────────────────", ["doc", "sticker", "gif"]),
]

# ═══════════════════════════════════════════════════════════════
#   DATABASE
# ═══════════════════════════════════════════════════════════════
import threading

def DB():
    c = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=15)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    return c

def db_init():
    c = DB()
    # Base media block columns for CREATE TABLE
    media_cols = "\n".join(
        f"        {v[1]} INTEGER DEFAULT 0," for v in MEDIA_TYPES.values()
    )
    c.executescript(f"""
    PRAGMA journal_mode=WAL;

    CREATE TABLE IF NOT EXISTS users(
        uid INTEGER PRIMARY KEY, uname TEXT DEFAULT '', name TEXT DEFAULT '',
        sub_end REAL DEFAULT 0, bot_on INTEGER DEFAULT 1,
        joined REAL DEFAULT 0, total_fwd INTEGER DEFAULT 0, is_banned INTEGER DEFAULT 0);

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
        block_at INTEGER DEFAULT 0, block_www INTEGER DEFAULT 0,
        block_tme INTEGER DEFAULT 0, block_all_links INTEGER DEFAULT 0,
        free_style TEXT DEFAULT 'none', emoji_str TEXT DEFAULT '',
        emoji_pos TEXT DEFAULT 'off',
        {media_cols}
        created REAL DEFAULT 0);

    CREATE TABLE IF NOT EXISTS flt(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ch_id INTEGER, uid INTEGER, ftype TEXT, val TEXT, extra TEXT DEFAULT '');

    CREATE TABLE IF NOT EXISTS repl(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ch_id INTEGER, uid INTEGER, rtype TEXT, old_val TEXT, new_val TEXT);

    CREATE TABLE IF NOT EXISTS fwd_log(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid INTEGER, ch_id INTEGER, msg_id INTEGER, src TEXT, ts REAL DEFAULT 0);

    CREATE INDEX IF NOT EXISTS idx_fwd_uid ON fwd_log(uid);
    CREATE INDEX IF NOT EXISTS idx_fwd_ts  ON fwd_log(ts);

    CREATE TABLE IF NOT EXISTS plans(
        key TEXT PRIMARY KEY, label TEXT, days INTEGER,
        price INTEGER, max_channels INTEGER DEFAULT 0, enabled INTEGER DEFAULT 1);

    CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);
    """)

    # Migration: add columns if missing
    migrations = [
        ("channels", "block_at",       "INTEGER DEFAULT 0"),
        ("channels", "block_www",      "INTEGER DEFAULT 0"),
        ("channels", "block_all_links","INTEGER DEFAULT 0"),
        ("channels", "block_tme",      "INTEGER DEFAULT 0"),
        ("channels", "free_style",     "TEXT DEFAULT 'none'"),
        ("channels", "emoji_str",      "TEXT DEFAULT ''"),
        ("channels", "emoji_pos",      "TEXT DEFAULT 'off'"),
        ("flt",      "extra",          "TEXT DEFAULT ''"),
        ("plans",    "max_channels",   "INTEGER DEFAULT 0"),
    ]
    # Add all media block columns
    for mkey, (mlabel, mcol) in MEDIA_TYPES.items():
        migrations.append(("channels", mcol, "INTEGER DEFAULT 0"))

    for tbl, col, defn in migrations:
        try: c.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {defn}")
        except: pass

    for row in [
        ("w","⚡ Weekly",  7,  19, 0, 1),
        ("m","🌟 Monthly",30,  49, 0, 1),
        ("y","👑 Yearly",365,399, 0, 1),
    ]:
        c.execute("INSERT OR IGNORE INTO plans VALUES(?,?,?,?,?,?)", row)

    c.execute("INSERT OR IGNORE INTO settings VALUES('trial_days','7')")
    c.commit(); c.close()

# ── Helpers ────────────────────────────────────────────────────
def setting_get(key, default=""):
    c=DB(); r=c.execute("SELECT value FROM settings WHERE key=?",(key,)).fetchone(); c.close()
    return r['value'] if r else default
def setting_set(key, value):
    c=DB(); c.execute("INSERT OR REPLACE INTO settings VALUES(?,?)",(key,str(value))); c.commit(); c.close()
def get_trial_days(): return int(setting_get('trial_days','7'))

def plan_all():
    c=DB(); r=c.execute("SELECT * FROM plans ORDER BY days").fetchall(); c.close()
    return [dict(i) for i in r]
def plan_get(key):
    c=DB(); r=c.execute("SELECT * FROM plans WHERE key=?",(key,)).fetchone(); c.close()
    return dict(r) if r else None
def plan_upd(key,**kw):
    c=DB()
    for k,v in kw.items(): c.execute(f"UPDATE plans SET {k}=? WHERE key=?",(v,key))
    c.commit(); c.close()
def plan_add(key,label,days,price,max_channels=0):
    c=DB(); c.execute("INSERT OR REPLACE INTO plans VALUES(?,?,?,?,?,1)",(key,label,days,price,max_channels)); c.commit(); c.close()
def plan_del(key):
    c=DB(); c.execute("DELETE FROM plans WHERE key=?",(key,)); c.commit(); c.close()

def u_upsert(uid,un="",nm=""):
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
    import datetime; u=u_get(uid)
    if not u or u['sub_end']<=time.time(): return "Expired"
    return datetime.datetime.fromtimestamp(u['sub_end']).strftime("%d %b %Y, %I:%M %p")
def u_give(uid,days):
    c=DB(); u=c.execute("SELECT sub_end FROM users WHERE uid=?",(uid,)).fetchone()
    now=time.time(); base=max(u['sub_end'] if u else now,now)
    c.execute("UPDATE users SET sub_end=? WHERE uid=?",(base+days*86400,uid)); c.commit(); c.close()
def u_revoke(uid): c=DB(); c.execute("UPDATE users SET sub_end=? WHERE uid=?",(time.time()-1,uid)); c.commit(); c.close()
def u_ban(uid,v=True): c=DB(); c.execute("UPDATE users SET is_banned=? WHERE uid=?",(int(v),uid)); c.commit(); c.close()
def u_toggle_bot(uid,v): c=DB(); c.execute("UPDATE users SET bot_on=? WHERE uid=?",(int(v),uid)); c.commit(); c.close()
def u_bot_on(uid): u=u_get(uid); return bool(u and u['bot_on'])
def u_inc(uid,n=1): c=DB(); c.execute("UPDATE users SET total_fwd=total_fwd+? WHERE uid=?",(n,uid)); c.commit(); c.close()
def u_set_plan(uid,pk): setting_set(f"user_plan_{uid}",pk)
def u_channel_limit(uid):
    c=DB(); r=c.execute("SELECT value FROM settings WHERE key=?",(f"user_plan_{uid}",)).fetchone(); c.close()
    if r and r['value']:
        p=plan_get(r['value'])
        if p: return len(ch_all(uid)), int(p.get('max_channels') or 0)
    return len(ch_all(uid)), 0
def u_can_add_channel(uid):
    cur,mx=u_channel_limit(uid)
    if mx==0: return True,""
    if cur>=mx: return False,f"❌ Plan limit!\n\nMax {mx} channel(s) allowed.\nYou have {cur}.\n\nUpgrade plan."
    return True,""

SESSION_CACHE:dict={}
KNOWN_USERS:set=set()

def s_save(uid,s):
    SESSION_CACHE[uid]=s
    c=DB(); c.execute("INSERT OR REPLACE INTO sessions VALUES(?,?)",(uid,s)); c.commit(); c.close()
def s_get(uid):
    if uid in ACL: return SESSION_CACHE.get(uid,"ACL_ACTIVE")
    if uid in SESSION_CACHE: return SESSION_CACHE[uid]
    for _ in range(3):
        try:
            c=DB(); r=c.execute("SELECT sess FROM sessions WHERE uid=?",(uid,)).fetchone(); c.close()
            if r: SESSION_CACHE[uid]=r['sess']; return r['sess']
            return None
        except: time.sleep(0.2)
    return None
def s_del(uid):
    SESSION_CACHE.pop(uid,None)
    c=DB(); c.execute("DELETE FROM sessions WHERE uid=?",(uid,)); c.commit(); c.close()

def ch_add(uid,src_id,src_name,ch_name):
    c=DB()
    cur=c.execute("INSERT INTO channels(uid,ch_name,src_id,src_name,created) VALUES(?,?,?,?,?)",(uid,ch_name,src_id,src_name,time.time()))
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

def f_add(cid,uid,ft,val,extra=""):
    c=DB(); c.execute("INSERT INTO flt(ch_id,uid,ftype,val,extra) VALUES(?,?,?,?,?)",(cid,uid,ft,val,extra)); c.commit(); c.close()
def f_get(cid):
    c=DB(); r=c.execute("SELECT * FROM flt WHERE ch_id=?",(cid,)).fetchall(); c.close()
    return [dict(i) for i in r]
def f_get_type(cid,ftype):
    c=DB(); r=c.execute("SELECT * FROM flt WHERE ch_id=? AND ftype=?",(cid,ftype)).fetchall(); c.close()
    return [dict(i) for i in r]
def f_del(fid): c=DB(); c.execute("DELETE FROM flt WHERE id=?",(fid,)); c.commit(); c.close()

def rp_add(cid,uid,rt,o,n):
    c=DB(); c.execute("INSERT INTO repl(ch_id,uid,rtype,old_val,new_val) VALUES(?,?,?,?,?)",(cid,uid,rt,o,n)); c.commit(); c.close()
def rp_get(cid,rt=None):
    c=DB()
    if rt: r=c.execute("SELECT * FROM repl WHERE ch_id=? AND rtype=?",(cid,rt)).fetchall()
    else:  r=c.execute("SELECT * FROM repl WHERE ch_id=?",(cid,)).fetchall()
    c.close(); return [dict(i) for i in r]
def rp_del(rpid): c=DB(); c.execute("DELETE FROM repl WHERE id=?",(rpid,)); c.commit(); c.close()

def l_add(uid,cid,mid,src):
    c=DB(); c.execute("INSERT INTO fwd_log(uid,ch_id,msg_id,src,ts) VALUES(?,?,?,?,?)",(uid,cid,mid,str(src),time.time())); c.commit(); c.close()
def l_dup(cid,mid,src):
    c=DB(); r=c.execute("SELECT id FROM fwd_log WHERE ch_id=? AND msg_id=? AND src=?",(cid,mid,str(src))).fetchone(); c.close()
    return r is not None
def l_stats(uid):
    c=DB()
    tot=c.execute("SELECT COUNT(*) as n FROM fwd_log WHERE uid=?",(uid,)).fetchone()['n']
    tod=c.execute("SELECT COUNT(*) as n FROM fwd_log WHERE uid=? AND ts>?",(uid,time.time()-86400)).fetchone()['n']
    wk=c.execute("SELECT COUNT(*) as n FROM fwd_log WHERE uid=? AND ts>?",(uid,time.time()-604800)).fetchone()['n']
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
#   MEDIA DETECTION
# ═══════════════════════════════════════════════════════════════
def detect_media_type(msg) -> str | None:
    """Returns one of the MEDIA_TYPES keys, or None for text/webpage."""
    media=getattr(msg,'media',None)
    if not media or isinstance(media,MessageMediaWebPage): return None
    if isinstance(media,MessageMediaPhoto): return "photo"
    if isinstance(media,MessageMediaDocument):
        doc=media.document
        if not doc or not hasattr(doc,'attributes'): return "doc"
        for a in (doc.attributes or []):
            if isinstance(a,DocumentAttributeSticker):  return "sticker"
            if isinstance(a,DocumentAttributeAnimated): return "gif"
            if isinstance(a,DocumentAttributeVideo):
                return "vidmsg" if getattr(a,'round_message',False) else "video"
            if isinstance(a,DocumentAttributeAudio):
                return "voice" if getattr(a,'voice',False) else "audio"
        return "doc"
    return None

def is_media_blocked(ch:dict, mtype:str|None) -> bool:
    """True if this media type should be blocked for this channel."""
    if mtype is None: return False
    col = MEDIA_TYPES.get(mtype,(None,None))[1]
    if col and ch.get(col): return True
    return False

# ═══════════════════════════════════════════════════════════════
#   PARAGRAPH BLOCK / REPLACE
# ═══════════════════════════════════════════════════════════════
def para_process(text:str, ch_id:int) -> str|None:
    """
    para_block  → remove paragraph that contains trigger word
    para (repl) → replace whole paragraph with new text
    Returns None if entire message should be dropped.
    """
    if not text: return text
    paragraphs=re.split(r'\n{2,}',text)
    para_blocks=f_get_type(ch_id,'para_block')
    para_repls=rp_get(ch_id,'para')
    if not para_blocks and not para_repls: return text
    result=[]
    for para in paragraphs:
        pl=para.lower(); blocked=False; replaced=False
        for f in para_blocks:
            if f['val'].lower() in pl: blocked=True; break
        if blocked: continue
        for r in para_repls:
            if r['old_val'].lower() in pl:
                nv=r['new_val']
                if nv and nv!='-': result.append(nv)
                replaced=True; break
        if not replaced: result.append(para)
    if not result: return None
    return "\n\n".join(result)

# ═══════════════════════════════════════════════════════════════
#   CHAT CACHE
# ═══════════════════════════════════════════════════════════════
# ── Separate caches: source (all) vs destination (admin only) ──
SRC_CACHE:dict={}   # uid -> list of ALL channels user is in
DEST_CACHE:dict={}  # uid -> list of channels where user can send/post
CHAT_CACHE:dict={}  # alias kept for logout cleanup

def _make_sid(e)->str:
    sid=str(e.id)
    if not sid.startswith('-'): sid=f"-100{sid}"
    return sid

def _can_post(e)->bool:
    """
    Returns True if user can send/forward to this entity.
    Broadcast Channel  → must be creator OR have post_messages admin right
    Group / Megagroup  → member is enough (default_banned_rights.send_messages=False)
    """
    is_broadcast=getattr(e,'broadcast',False)
    is_mega=getattr(e,'megagroup',False)

    if isinstance(e,Chat):
        # Normal group — member can always send (unless left/kicked, won't appear in dialogs)
        return True

    if isinstance(e,Channel):
        # Creator always can post
        if getattr(e,'creator',False): return True

        admin_rights=getattr(e,'admin_rights',None)

        if is_broadcast and not is_mega:
            # Broadcast channel: MUST be admin with post_messages
            if admin_rights is None: return False
            return bool(getattr(admin_rights,'post_messages',False))

        # Megagroup / supergroup
        if admin_rights:
            # Admin — can always send
            return True
        # Regular member — check if send_messages is banned by default
        dbr=getattr(e,'default_banned_rights',None)
        if dbr and getattr(dbr,'send_messages',False):
            return False   # everyone is restricted
        return True

    return False

async def fetch_chats(uid:int)->list:
    """Fetch ALL channels (for source picker). Returns full list."""
    client=ACL.get(uid); sess=s_get(uid)
    if not client and not sess: return []
    own=False
    if not client:
        client=TelegramClient(StringSession(sess),API_ID,API_HASH)
        await client.connect(); own=True
    src_list=[]; dest_list=[]
    try:
        dialogs=await client.get_dialogs(limit=500)
        for d in dialogs:
            e=d.entity
            if not isinstance(e,(Channel,Chat)): continue
            is_broadcast=getattr(e,'broadcast',False)
            is_mega=getattr(e,'megagroup',False)
            name=getattr(e,'title',None) or "Unknown"
            sid=_make_sid(e)

            # Icon
            if is_broadcast and not is_mega: icon="📢"
            elif is_mega:                    icon="💬"
            else:                            icon="👥"

            entry={'id':sid,'name':name,'icon':icon}
            src_list.append(entry)
            if _can_post(e):
                dest_list.append(entry)

        SRC_CACHE[uid]=src_list
        DEST_CACHE[uid]=dest_list
        CHAT_CACHE[uid]=src_list  # backward compat
        LOG.info(f"✅ Fetched uid={uid}: {len(src_list)} total, {len(dest_list)} dest-capable")
    except Exception as ex:
        LOG.error(f"fetch_chats {uid}: {ex}")
    finally:
        if own:
            try: await client.disconnect()
            except: pass
    return src_list

async def fetch_chats_dest(uid:int)->list:
    """Force refresh destination list only (fast re-check)."""
    await fetch_chats(uid)
    return DEST_CACHE.get(uid,[])

def chat_list_text(chats,page=0):
    start=page*CHATS_PER_PAGE; end=start+CHATS_PER_PAGE; pc=chats[start:end]
    return "\n".join(f"{start+i+1}. {c['icon']} {c['name']}" for i,c in enumerate(pc)), pc

def num_kb(chats,page,prefix,total):
    start=page*CHATS_PER_PAGE; end=min(start+CHATS_PER_PAGE,total)
    nums=list(range(start+1,end+1)); rows=[]
    r1=[IB(str(n),callback_data=f"{prefix}_{n-1}") for n in nums[:7]]
    r2=[IB(str(n),callback_data=f"{prefix}_{n-1}") for n in nums[7:14]]
    if r1: rows.append(r1)
    if r2: rows.append(r2)
    nav=[]
    if page>0:    nav.append(IB("◀ Prev",callback_data=f"pg_{prefix}_{page-1}"))
    if end<total: nav.append(IB("Next ▶",callback_data=f"pg_{prefix}_{page+1}"))
    if nav: rows.append(nav)
    rows.append([IB("🔄 Refresh",callback_data=f"refr_{prefix}"),IB("🏠 Home",callback_data="main")])
    return IM(rows)

# ═══════════════════════════════════════════════════════════════
#   TEXT STYLE ENGINE
# ═══════════════════════════════════════════════════════════════
STYLE_DEFS={
    "bold":"𝗕𝗼𝗹𝗱","italic":"𝘐𝘵𝘢𝘭𝘪𝗰","bold_italic":"𝗕𝗼𝗹𝗱+𝘐𝘵𝘢𝘭𝘊",
    "caps":"CAPS LOCK","lower":"lowercase","title":"Title Case",
    "clean":"🧹 Clean","lines":"📋 Each Line ▪️","mono":"`Mono`",
}
STYLE_COMBOS={"bold_italic":("bold","italic")}
EMOJI_POS={"off":"❌ Off","start":"▶️ Start","end":"◀️ End","both":"↔️ Both","lines":"📋 Every Line"}
AI_STYLES={
    "none":("❌ Off",""),"news":("📰 News","Professional news bulletin"),
    "casual":("💬 Casual","Friendly casual tone"),"formal":("🎩 Formal","Formal professional tone"),
    "short":("✂️ Short","Summarize in 2-3 lines"),"bullet":("• Bullets","Convert to bullet points"),
    "emoji":("😊 Emoji","Add emojis"),"clickbait":("🔥 Clickbait","Catchy attention-grabbing"),
    "clean":("🧹 Clean","Remove promos and links"),"custom":("✏️ Custom","My custom AI prompt"),
}

def _he(t): return t.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
def _short(text,n=20):
    if not text: return "—"
    text=text.strip(); return text[:n]+"…" if len(text)>n else text
def parse_style_set(s):
    if not s or s=='none': return set()
    return {x.strip().lstrip('fs_') for x in s.split(',') if x.strip() and x.strip()!='none'}
def encode_style_set(st):
    cleaned={s for s in st if s and s!='none'}
    return ','.join(sorted(cleaned)) if cleaned else 'none'

def apply_styles(text,styles_str):
    if not text or not styles_str or styles_str=='none': return text,False
    active=set()
    for s in styles_str.split(','):
        s=s.strip().lstrip('fs_')
        if not s or s=='none': continue
        if s in STYLE_COMBOS:
            for sub in STYLE_COMBOS[s]: active.add(sub)
        else: active.add(s)
    if not active: return text,False
    use_html=bool(active & {'bold','italic','mono'}); result=text
    if 'caps' in active:   result=result.upper()
    elif 'lower' in active: result=result.lower()
    elif 'title' in active: result=result.title()
    if 'clean' in active:
        result=re.sub(r' {2,}',' ',result); result=re.sub(r'\n{3,}','\n\n',result); result=result.strip()
    if 'lines' in active:
        result='\n'.join(f"▪️ {l}" if l.strip() else l for l in result.split('\n'))
    if use_html:
        lns=result.split('\n'); new_lns=[]
        for line in lns:
            if line.strip():
                l=_he(line)
                if 'bold' in active and 'italic' in active: l=f"<b><i>{l}</i></b>"
                elif 'bold' in active:   l=f"<b>{l}</b>"
                elif 'italic' in active: l=f"<i>{l}</i>"
                if 'mono' in active: l=f"<code>{l}</code>"
                new_lns.append(l)
            else: new_lns.append(line)
        result='\n'.join(new_lns)
    return result,use_html

def apply_emoji(text,emoji_str,position):
    if not text or not emoji_str or position=='off': return text
    e=emoji_str.strip()
    if not e: return text
    if position=='start':  return f"{e} {text}"
    elif position=='end':  return f"{text} {e}"
    elif position=='both': return f"{e} {text} {e}"
    elif position=='lines':return '\n'.join(f"{e} {l}" if l.strip() else l for l in text.split('\n'))
    return text

async def ai_rewrite(text,ch):
    if not text: return text,False
    use_html=False; styles_str=ch.get('free_style','none')
    if styles_str and styles_str!='none': text,use_html=apply_styles(text,styles_str)
    es=ch.get('emoji_str',''); ep=ch.get('emoji_pos','off')
    if es and ep!='off': text=apply_emoji(text,es,ep)
    style=ch.get('ai_style','none')
    if style=='none' or not ch.get('ai_on'): return text,use_html
    prompt=ch.get('ai_prompt','')
    system=prompt if (style=='custom' and prompt) else (
        f"Rewrite as: {AI_STYLES.get(style,('',''))[1]}. Keep core info. Return ONLY rewritten text.")
    try:
        import httpx
        async with httpx.AsyncClient(timeout=10) as cl:
            r=await cl.post("https://api.anthropic.com/v1/messages",
                headers={"x-api-key":CLAUDE_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
                json={"model":"claude-haiku-4-5-20251001","max_tokens":800,
                      "system":system,"messages":[{"role":"user","content":f"Rewrite:\n{text}"}]})
            d=r.json()
            if "content" in d: return d["content"][0]["text"].strip(),use_html
    except: pass
    return text,use_html

# ═══════════════════════════════════════════════════════════════
#   LINK STRIPPER
# ═══════════════════════════════════════════════════════════════
_TLDS=(r'com|net|org|in|co|io|info|biz|tv|me|online|live|site|web|app|pro'
       r'|club|shop|store|tech|top|xyz|bet|win|casino|games|vip|to|cc|gg|uk|us|eu|au|pk|bd|lk|np')
def strip_links(text,b_all,b_www,b_tme,b_at):
    if not text: return text
    if b_all or b_www:
        text=re.sub(r'https?://\S+','',text,flags=re.IGNORECASE)
        text=re.sub(r'www\.\S+','',text,flags=re.IGNORECASE)
        text=re.sub(r'\b[A-Za-z0-9](?:[A-Za-z0-9\-]{0,61}[A-Za-z0-9])?\.(?:'+_TLDS+r')(?:/\S*)?\b','',text,flags=re.IGNORECASE)
    if b_all or b_tme: text=re.sub(r't\.me/\S+','',text,flags=re.IGNORECASE)
    if b_all or b_at:  text=re.sub(r'@[A-Za-z0-9_]+','',text)
    text=re.sub(r'[ \t]{2,}',' ',text); text=re.sub(r'\n{3,}','\n\n',text)
    return text.strip()

# ═══════════════════════════════════════════════════════════════
#   ENGINE
# ═══════════════════════════════════════════════════════════════
ACL:dict={}; TSK:dict={}; REPLY_MAP:dict={}

# Menu message tracker — last bot menu msg per user {uid: Message}
MENU_MSG:dict={}

async def _fwd(cl,msg,did,ch,repls,reply_to_msg_id=None):
    try:
        try: did=int(did)
        except: pass

        # ── CRITICAL: use raw_text (plain text, NO markdown stars) ──
        # msg.text in Telethon returns **bold** markers for bold entities
        # msg.raw_text always returns clean plain text without any ** __ ~~ markers
        raw = msg.raw_text or ""

        # ── Check if we need to transform text at all ──
        needs_transform = bool(
            repls or
            ch.get('block_all_links') or ch.get('block_links') or
            ch.get('block_www') or ch.get('block_tme') or ch.get('block_at') or
            (ch.get('free_style') and ch.get('free_style') != 'none') or
            (ch.get('emoji_str') and ch.get('emoji_pos') != 'off') or
            ch.get('fmt_bold') or ch.get('fmt_clean') or
            ch.get('header') or ch.get('footer') or ch.get('remove_cap') or
            ch.get('ai_on')
        )

        # ── Word/link replacements ──
        for r in repls:
            if r['rtype'] in ('link','word'): raw=raw.replace(r['old_val'],r['new_val'])

        # ── Link blocking ──
        b_all=bool(ch.get('block_all_links') or ch.get('block_links'))
        b_www=bool(ch.get('block_www')); b_tme=bool(ch.get('block_tme')); b_at=bool(ch.get('block_at'))
        if b_all or b_www or b_tme or b_at: raw=strip_links(raw,b_all,b_www,b_tme,b_at)

        # ── Para block/replace ──
        if raw:
            raw=para_process(raw,ch['id'])
            if raw is None: return None

        # ── Text styles + emoji (returns HTML-safe text when needed) ──
        use_html=False
        styles_str=ch.get('free_style','none'); es=ch.get('emoji_str',''); ep=ch.get('emoji_pos','off')
        if (styles_str and styles_str!='none') or (es and ep!='off'):
            raw,use_html=await ai_rewrite(raw or '',ch)

        # ── Clean extra blank lines ──
        if ch.get('fmt_clean') and raw: raw=re.sub(r'\n{3,}','\n\n',raw).strip()

        # ── Bold Title: ALWAYS use HTML <b> — NEVER ** which shows as stars ──
        if ch.get('fmt_bold') and raw:
            use_html=True   # force HTML mode
            lines=raw.split('\n')
            # Find first non-empty line and bold it
            for i,line in enumerate(lines):
                if line.strip():
                    lines[i]=f"<b>{_he(line.strip())}</b>"
                    break
            raw='\n'.join(lines)

        # ── Header / Footer (with same style) ──
        hdr=(ch.get('header') or '').strip()
        ftr=(ch.get('footer') or '').strip()
        if hdr:
            if styles_str and styles_str!='none':
                h,h_html=apply_styles(hdr,styles_str)
                use_html=use_html or h_html
            else:
                h=_he(hdr) if use_html else hdr
            raw=f"{h}\n\n{raw}" if raw else h
        if ftr:
            if styles_str and styles_str!='none':
                f2,f2_html=apply_styles(ftr,styles_str)
                use_html=use_html or f2_html
            else:
                f2=_he(ftr) if use_html else ftr
            raw=f"{raw}\n\n{f2}" if raw else f2

        if ch.get('remove_cap'): raw=""

        parse_mode='html' if use_html else None
        sil=bool(ch.get('silent'))

        # ── Detect media ──
        is_wp=isinstance(getattr(msg,'media',None),MessageMediaWebPage)
        real_media=bool(msg.media and not is_wp)
        is_sticker=False; is_gif=False; is_vidmsg=False
        if real_media and isinstance(msg.media,MessageMediaDocument):
            doc=msg.media.document
            if doc and hasattr(doc,'attributes'):
                for attr in doc.attributes:
                    if isinstance(attr,DocumentAttributeSticker): is_sticker=True
                    if isinstance(attr,DocumentAttributeAnimated): is_gif=True
                    if isinstance(attr,DocumentAttributeVideo) and getattr(attr,'round_message',False): is_vidmsg=True

        if ch.get('copy_mode'):
            if real_media:
                no_cap=(is_sticker or is_gif or is_vidmsg)
                cap=(raw.strip() or None) if not no_cap else None
                s=None; import os as _os

                # When no custom transformations done AND no parse_mode needed,
                # pass original entities so Telegram bold/italic is preserved naturally
                orig_entities=None
                if not needs_transform and not use_html:
                    orig_entities=getattr(msg,'entities',None)

                try:
                    if orig_entities:
                        s=await cl.send_file(did,file=msg.media,caption=raw.strip() or None,
                                             caption_entities=orig_entities,
                                             silent=sil,reply_to=reply_to_msg_id)
                    else:
                        s=await cl.send_file(did,file=msg.media,caption=cap,silent=sil,
                                             reply_to=reply_to_msg_id,parse_mode=parse_mode)
                except Exception as e1: LOG.warning(f"send_file err:{e1}")

                if not s:
                    tmp=f"tmp_{did}.bin"
                    try:
                        await cl.download_media(msg,file=tmp)
                        s=await cl.send_file(did,file=tmp,caption=cap,silent=sil,
                                             reply_to=reply_to_msg_id,parse_mode=parse_mode)
                    except Exception as e2: LOG.warning(f"dl+upload err:{e2}")
                    try: _os.remove(tmp)
                    except: pass
                if not s:
                    try: s=await cl.forward_messages(did,msg)
                    except Exception as e3: LOG.error(f"ALL FAILED {did}:{e3}"); return None

            elif raw.strip():
                # Text-only message
                orig_entities=None
                if not needs_transform and not use_html:
                    orig_entities=getattr(msg,'entities',None)

                if orig_entities:
                    # Preserve original Telegram bold/italic/etc entities — no stars ever
                    s=await cl.send_message(did,raw.strip(),silent=sil,
                                            link_preview=False,reply_to=reply_to_msg_id,
                                            formatting_entities=orig_entities)
                else:
                    s=await cl.send_message(did,raw.strip(),silent=sil,
                                            link_preview=False,reply_to=reply_to_msg_id,
                                            parse_mode=parse_mode)
            else:
                return None
        else:
            try: s=await cl.forward_messages(did,msg)
            except Exception as ef: LOG.error(f"fwd err:{ef}"); return None

        if ch.get('pin_msg') and s:
            try: await cl.pin_message(did,s,notify=not sil)
            except: pass
        return s
    except Exception as e: LOG.error(f"FWD→{did}:{e}"); return None

async def eng_start(uid):
    sess=s_get(uid)
    if not sess: return False
    await eng_stop(uid)
    try:
        cl=TelegramClient(StringSession(sess),API_ID,API_HASH,connection_retries=None,retry_delay=5,auto_reconnect=True,request_retries=5)
        await cl.connect()
        if not await cl.is_user_authorized():
            await asyncio.sleep(5)
            try:
                await cl.connect()
                if not await cl.is_user_authorized(): s_del(uid); await cl.disconnect(); return False
            except: await cl.disconnect(); return False
        s_save(uid,cl.session.save()); ACL[uid]=cl
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
            try:
                if not u_bot_on(uid) or not u_ok(uid): return
                matched=sm.get(ev.chat_id,[]) or sm.get(str(ev.chat_id),[])
                for _ch_stale in matched:
                    try:
                        ch=ch_get(_ch_stale['id'])
                        if not ch or not ch['enabled']: continue
                        if ch.get('dup_check') and l_dup(ch['id'],ev.message.id,str(ev.chat_id)): continue
                        is_wp2=isinstance(getattr(ev.message,'media',None),MessageMediaWebPage)
                        has_real=bool(ev.message.media and not is_wp2)
                        # ── Per-type media block ──
                        if has_real:
                            mtype=detect_media_type(ev.message)
                            if is_media_blocked(ch,mtype): continue
                        if ch.get('media_only') and not has_real: continue
                        if ch.get('text_only') and has_real: continue
                        # ── Text filters ──
                        txt=(ev.message.text or "").lower()
                        if not has_real:
                            fils=f_get(ch['id'])
                            bl2=[f for f in fils if f['ftype']=='blacklist']
                            wl2=[f for f in fils if f['ftype']=='whitelist']
                            if txt and any(f['val'].lower() in txt for f in bl2): continue
                            if wl2 and txt and not any(f['val'].lower() in txt for f in wl2): continue
                        repls=rp_get(ch['id']); dests=json.loads(ch['dests']); cnt=0
                        src_rep=None
                        if ev.message.reply_to and hasattr(ev.message.reply_to,'reply_to_msg_id'):
                            src_rep=ev.message.reply_to.reply_to_msg_id
                        for d in dests:
                            try:
                                rtid=None
                                if src_rep:
                                    rmap=REPLY_MAP.get(uid,{}).get(ch['id'],{}).get(d['id'],{})
                                    rtid=rmap.get(src_rep)
                                r=await _fwd(cl,ev.message,d['id'],ch,repls,reply_to_msg_id=rtid)
                                if r:
                                    cnt+=1; ds=str(d['id'])
                                    REPLY_MAP.setdefault(uid,{}).setdefault(ch['id'],{}).setdefault(ds,{})
                                    REPLY_MAP[uid][ch['id']][ds][ev.message.id]=r.id
                                    rm2=REPLY_MAP[uid][ch['id']][ds]
                                    if len(rm2)>500:
                                        for k2 in list(rm2.keys())[:100]: del rm2[k2]
                            except Exception as de: LOG.warning(f"dest err {d['id']}:{de}")
                            if ch.get('delay_sec',0)>0: await asyncio.sleep(ch['delay_sec'])
                        if cnt: l_add(uid,ch['id'],ev.message.id,str(ev.chat_id)); u_inc(uid,cnt)
                    except Exception as ce: LOG.warning(f"ch err uid={uid}:{ce}")
            except Exception as he: LOG.warning(f"handler err uid={uid}:{he}")

        async def keep_alive():
            while uid in ACL:
                try:
                    await asyncio.sleep(3*3600)
                    if uid not in ACL: break
                    c2=ACL.get(uid)
                    if c2 and await c2.is_user_authorized(): s_save(uid,c2.session.save())
                except asyncio.CancelledError: break
                except Exception as e: LOG.warning(f"keep_alive uid={uid}:{e}")
        asyncio.create_task(keep_alive())

        async def runner():
            fails=0
            while True:
                try: await cl.run_until_disconnected(); fails=0
                except asyncio.CancelledError: break
                except Exception as e:
                    fails+=1; await asyncio.sleep(min(5*fails,60))
                if uid not in ACL: break
                try:
                    if not cl.is_connected(): await cl.connect()
                    if await cl.is_user_authorized(): s_save(uid,cl.session.save()); fails=0; continue
                except: pass
                if fails>=5: asyncio.create_task(eng_start(uid)); break

        TSK[uid]=asyncio.create_task(runner())
        LOG.info(f"✅ Engine uid={uid}"); return True
    except Exception as e: LOG.error(f"eng_start {uid}:{e}"); return False

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
def B(t,c): return IB(t,callback_data=c)
def KB(*rows): return IM(list(rows))
def OO(v): return "🟢" if v else "🔴"

# ── Message cleanup helpers ─────────────────────────────────────
async def _safe_delete(msg):
    if msg is None: return
    try: await msg.delete()
    except: pass

async def clean_send(chat_obj, uid:int, text:str, kb=None):
    """Delete previous menu msg, send new, track it."""
    old=MENU_MSG.pop(uid,None)
    await _safe_delete(old)
    try:
        msg=await chat_obj.reply_text(text,reply_markup=kb)
        MENU_MSG[uid]=msg; return msg
    except Exception as e:
        LOG.warning(f"clean_send uid={uid}: {e}"); return None

async def clean_edit(q, uid:int, text:str, kb=None):
    """Edit in-place or delete+resend. Always tracks result in MENU_MSG."""
    try:
        await q.edit_message_text(text,reply_markup=kb)
        MENU_MSG[uid]=q.message; return
    except: pass
    # Edit failed — delete old, send fresh
    old=MENU_MSG.pop(uid,None)
    await _safe_delete(old)
    try: await _safe_delete(q.message)
    except: pass
    try:
        msg=await q.message.reply_text(text,reply_markup=kb)
        MENU_MSG[uid]=msg
    except Exception as e:
        LOG.warning(f"clean_edit uid={uid}: {e}")

def KB_MAIN(uid):
    eng=uid in ACL; on=u_bot_on(uid)
    st="🟢 Running" if (on and eng) else ("🔴 Paused" if on else "⛔ Bot Off")
    return IM([
        [B(f"{st}  •  {BOT_NAME}","noop")],
        [B("📡 My Channels","chs"),   B("⚙️ Settings","settings")],
        [B("📊 Statistics","stats"),  B("💳 Subscription","sub")],
        [B("🆘 Support","support"),   B("🚪 Logout","logout")],
    ])

def KB_SETTINGS(uid):
    on=u_bot_on(uid); eng=uid in ACL
    return IM([
        [B("⚙️ Settings Panel","noop")],
        [B(f"{OO(on)} Bot {'ON' if on else 'OFF'}","bot_tog"),
         B(f"{'⚡ Engine ON' if eng else '⛔ Engine OFF'}","eng_r")],
        [B("✨ Text Style","ai_g"),  B("🔄 Replace","repl_g")],
        [B("📐 Format","fmt_g"),    B("🗂 All Channels","ctrl_g")],
        [B("‹ Back","main")],
    ])

def KB_CHS(uid):
    chs=ch_all(uid); rows=[]
    cur,mx=u_channel_limit(uid); lt=f"  [{cur}/{mx}]" if mx>0 else ""
    rows.append([B(f"📡 My Channels{lt}","noop")])
    for ch in chs:
        on="🟢" if ch['enabled'] else "🔴"
        dests=json.loads(ch['dests'])
        rows.append([
            B(f"{on} {ch['ch_name'][:20]} ({len(dests)}▶)",f"ch_{ch['id']}"),
            B("⚙",f"ctrl_{ch['id']}"),B("🗑",f"ch_del_{ch['id']}"),
        ])
    if not chs: rows.append([B("No channels yet","noop")])
    rows.append([B("➕ Add Channel","ch_new"),B("‹ Back","main")]); return IM(rows)

def KB_CH(cid):
    if not ch_get(cid): return KB([B("❌ Not found","noop"),B("‹ Back","chs")])
    ch=ch_get(cid); dests=json.loads(ch['dests'])
    ball=bool(ch.get('block_all_links') or ch.get('block_links')); bat=bool(ch.get('block_at'))
    blk_count=sum(1 for _,(lbl,col) in MEDIA_TYPES.items() if ch.get(col))
    blk_info=f"🔴 {blk_count} Blocked" if blk_count else "✅ All Allowed"
    return IM([
        [B(f"{'🟢' if ch['enabled'] else '🔴'} {ch['ch_name'][:28]}",f"ch_tog_{cid}")],
        [B(f"📤 {len(dests)} dest",f"dests_{cid}"),  B("⚙️ Control",f"ctrl_{cid}")],
        [B("✨ Style",f"ai_{cid}"),                   B("🔄 Replace",f"repls_{cid}")],
        [B("✂️ Filters",f"flt_{cid}"),               B("📐 Format",f"fmt_{cid}")],
        [B(f"📹 Media  [{blk_info}]",f"media_{cid}")],
        [B(f"{'🔴 Links Blocked' if ball else '⚪ Block Links'}",f"t_ball_{cid}"),
         B(f"{'🔴 @Blocked' if bat else '⚪ Block @User'}",f"t_bat_{cid}")],
        [B("✏️ Rename",f"ch_ren_{cid}"),             B("📊 Stats",f"ch_stat_{cid}")],
        [B("‹ Channels","chs"),                      B("🏠 Home","main")],
    ])

# ═══════════════════════════════════════════════════════════════
#   MEDIA CONTROL PANEL — Individual toggles per type
# ═══════════════════════════════════════════════════════════════
def KB_MEDIA(cid):
    ch=ch_get(cid)
    if not ch: return KB([B("❌ Not found","noop"),B("‹ Back","chs")])

    rows=[
        [B(f"📹 Media Control  •  {ch['ch_name'][:20]}","noop")],
        [B("🟢 = Allowed  |  🔴 = Blocked","noop")],
        [B("Tap any button to toggle ↕️","noop")],
        [B("──────────────────────────────","noop")],
    ]

    for group_header, type_keys in MEDIA_GROUPS:
        rows.append([B(group_header,"noop")])
        for mkey in type_keys:
            mlabel, mcol = MEDIA_TYPES[mkey]
            is_blocked = bool(ch.get(mcol, 0))
            # Show current state clearly + what happens on tap
            if is_blocked:
                btn_text = f"🔴 {mlabel}  ← tap to ALLOW"
            else:
                btn_text = f"🟢 {mlabel}  ← tap to BLOCK"
            rows.append([B(btn_text, f"mt_{cid}_{mcol}")])

    rows.append([B("──────────────────────────────","noop")])
    rows.append([
        B("🚫 Block ALL","media_blkall_{cid}".replace("{cid}",str(cid))),
        B("✅ Allow ALL","media_alwall_{cid}".replace("{cid}",str(cid))),
    ])
    rows.append([B("‹ Back",f"ch_{cid}"),B("🏠 Home","main")])
    return IM(rows)

def KB_CTRL(cid):
    ch=ch_get(cid)
    if not ch: return KB([B("❌ Not found","noop"),B("‹ Back","chs")])
    ball=bool(ch.get('block_all_links') or ch.get('block_links')); bat=bool(ch.get('block_at'))
    hdr_t=f"📝 Hdr: {_short(ch.get('header',''),15)}" if ch.get('header') else "📝 Header —"
    ftr_t=f"📝 Ftr: {_short(ch.get('footer',''),15)}" if ch.get('footer') else "📝 Footer —"
    blk_count=sum(1 for _,(lbl,col) in MEDIA_TYPES.items() if ch.get(col))
    return IM([
        [B(f"⚙️ {ch['ch_name'][:28]}","noop")],
        [B(f"{OO(ch['copy_mode'])} Copy Mode",  f"t_copy_{cid}"),
         B(f"{OO(ch['silent'])} Silent",          f"t_sil_{cid}")],
        [B(f"{OO(ch['pin_msg'])} Pin",            f"t_pin_{cid}"),
         B(f"{OO(ch['remove_cap'])} No Caption",  f"t_cap_{cid}")],
        [B(f"{OO(ch['media_only'])} Media Only",  f"t_med_{cid}"),
         B(f"{OO(ch['text_only'])} Text Only",    f"t_txt_{cid}")],
        [B(f"{OO(ch['dup_check'])} Dup Check",    f"t_dup_{cid}"),
         B(f"{OO(ch['enabled'])} Enable",          f"ch_tog_{cid}")],
        [B(f"⏱ Delay: {ch['delay_sec']}s",        f"s_dly_{cid}")],
        [B(hdr_t,f"s_hdr_{cid}"),B(ftr_t,f"s_ftr_{cid}")],
        [B(f"{'🔴 Block ALL Links ON' if ball else '⚪ Block ALL Links OFF'}",f"t_ball_{cid}")],
        [B(f"{'🔴 Block @Username ON' if bat  else '⚪ Block @Username OFF'}",f"t_bat_{cid}")],
        [B(f"📹 Media [{blk_count} blocked]",f"media_{cid}"),
         B("✨ Style",f"ai_{cid}"),B("✂️ Filters",f"flt_{cid}")],
        [B("‹ Back",f"ch_{cid}"),B("🏠 Home","main")],
    ])

def KB_CTRL_G(uid):
    on=u_bot_on(uid); eng=uid in ACL; chs=ch_all(uid)
    on_c=sum(1 for c in chs if c['enabled'])
    return IM([
        [B(f"{OO(on)} Bot","bot_tog"),B(f"{'⚡ ON' if eng else '⛔ OFF'} Engine","eng_r")],
        [B(f"📡 {len(chs)} channels  •  🟢 {on_c} active","noop")],
        [B("📡 Channels","chs"),B("‹ Back","settings")],
    ])

def KB_FLT(cid):
    ch=ch_get(cid)
    if not ch: return KB([B("❌ Not found","noop"),B("‹ Back","chs")])
    fils=f_get(cid)
    wl=[f for f in fils if f['ftype']=='whitelist']
    bl=[f for f in fils if f['ftype']=='blacklist']
    pbl=[f for f in fils if f['ftype']=='para_block']
    all_on=bool(ch.get('block_all_links') or ch.get('block_links'))
    www_on=bool(ch.get('block_www')); tme_on=bool(ch.get('block_tme')); at_on=bool(ch.get('block_at'))
    rows=[
        [B(f"✂️ Filters  •  {ch['ch_name'][:20]}","noop")],
        [B("─── 🔗 LINK BLOCKING ───","noop")],
        [B(f"{'🔴 BLOCK ALL LINKS ✅ ON' if all_on else '⚪ BLOCK ALL LINKS ❌ OFF'}",f"t_ball_{cid}")],
        [B(f"{OO(www_on)} http/https/www/domains",f"t_bwww_{cid}")],
        [B(f"{OO(tme_on)} t.me Links",f"t_btme_{cid}")],
        [B("─── 👤 @USERNAME ───","noop")],
        [B(f"{OO(at_on)} @username Mentions",f"t_bat_{cid}")],
        [B("─── ✅ WHITELIST (Forward IF contains) ───","noop")],
    ]
    for f in wl: rows.append([B(f"✅ {f['val'][:30]}","noop"),B("🗑",f"d_flt_{f['id']}")])
    rows.append([B("➕ Add Whitelist",f"add_wl_{cid}")])
    rows.append([B("─── 🚫 BLACKLIST (Skip IF contains) ───","noop")])
    for f in bl: rows.append([B(f"🚫 {f['val'][:30]}","noop"),B("🗑",f"d_flt_{f['id']}")])
    rows.append([B("➕ Add Blacklist",f"add_bl_{cid}")])
    rows.append([B("─── 📝 PARA BLOCK (Remove paragraph) ───","noop")])
    rows.append([B("💡 Word milne pe sirf WO paragraph delete","noop")])
    for f in pbl: rows.append([B(f"📝 {f['val'][:30]}","noop"),B("🗑",f"d_flt_{f['id']}")])
    rows.append([B("➕ Add Para Block",f"add_pb_{cid}")])
    rows.append([B("‹ Back",f"ch_{cid}"),B("🏠 Home","main")])
    return IM(rows)

def KB_REPLS(cid):
    ch=ch_get(cid)
    lrp=rp_get(cid,'link'); wrp=rp_get(cid,'word'); prp=rp_get(cid,'para')
    rows=[[B(f"🔄 {ch['ch_name'][:26]}","noop")]]
    rows.append([B(f"🔗 Link Replace ({len(lrp)})","noop")])
    for r in lrp: rows.append([B(f"🔗 {r['old_val'][:18]}→{r['new_val'][:12]}","noop"),B("🗑",f"d_rp_{r['id']}")])
    rows.append([B("➕ Add Link Replace",f"add_lrp_{cid}")])
    rows.append([B(f"📝 Word Replace ({len(wrp)})","noop")])
    for r in wrp: rows.append([B(f"📝 {r['old_val'][:18]}→{r['new_val'][:12]}","noop"),B("🗑",f"d_rp_{r['id']}")])
    rows.append([B("➕ Add Word Replace",f"add_wrp_{cid}")])
    rows.append([B(f"📋 Para Replace ({len(prp)})","noop")])
    rows.append([B("💡 Para mein word mile → pura para replace","noop")])
    for r in prp:
        nv="[DELETE]" if (not r['new_val'] or r['new_val']=='-') else r['new_val'][:15]
        rows.append([B(f"📋 '{r['old_val'][:12]}' → {nv}","noop"),B("🗑",f"d_rp_{r['id']}")])
    rows.append([B("➕ Add Para Replace",f"add_prp_{cid}")])
    rows.append([B("‹ Back",f"ch_{cid}"),B("🏠 Home","main")]); return IM(rows)

def KB_REPL_G(uid):
    chs=ch_all(uid); rows=[[B("🔄 Replacements","noop")]]
    for ch in chs:
        lc=len(rp_get(ch['id'],'link')); wc=len(rp_get(ch['id'],'word')); pc=len(rp_get(ch['id'],'para'))
        rows.append([B(f"📡 {ch['ch_name'][:22]}  🔗{lc} 📝{wc} 📋{pc}",f"repls_{ch['id']}")])
    if not chs: rows.append([B("No channels!","ch_new")])
    rows.append([B("‹ Back","settings")]); return IM(rows)

def KB_FMT(cid):
    ch=ch_get(cid)
    hp=f"📝 Hdr: {_short(ch.get('header',''),18)}" if ch.get('header') else "📝 Header: —"
    fp=f"📝 Ftr: {_short(ch.get('footer',''),18)}" if ch.get('footer') else "📝 Footer: —"
    return IM([
        [B(f"📐 Format  •  {ch['ch_name'][:20]}","noop")],
        [B(f"{OO(ch['fmt_bold'])} Bold Title",  f"t_bold_{cid}"),
         B(f"{OO(ch['fmt_clean'])} Clean Text", f"t_clean_{cid}")],
        [B(f"{OO(ch['block_links'])} No Links", f"t_blk_{cid}"),
         B(f"{OO(ch['remove_cap'])} No Caption",f"t_cap_{cid}")],
        [B(hp,f"s_hdr_{cid}")],[B(fp,f"s_ftr_{cid}")],
        [B("✨ Text Style",f"ai_{cid}")],
        [B("‹ Back",f"ch_{cid}"),B("🏠 Home","main")],
    ])

def KB_FMT_G(uid):
    chs=ch_all(uid); rows=[[B("📐 Format","noop")]]
    for ch in chs: rows.append([B(f"📡 {ch['ch_name'][:28]}",f"fmt_{ch['id']}")])
    if not chs: rows.append([B("No channels","ch_new")])
    rows.append([B("‹ Back","settings")]); return IM(rows)

def KB_AI(cid):
    ch=ch_get(cid); active=parse_style_set(ch.get('free_style','none'))
    ep=ch.get('emoji_pos','off'); es=ch.get('emoji_str','') or '—'
    rows=[[B(f"✨ Text Style  •  {ch['ch_name'][:22]}","noop")],
          [B("── Tap to toggle (multi-select allowed) ──","noop")]]
    for key,label in [("bold","𝗕𝗼𝗹𝗱"),("italic","𝘐𝘵𝘢𝘭𝘪𝗰"),("bold_italic","𝗕+𝘐 Bold+Italic ⚡"),
                       ("caps","CAPS"),("lower","lowercase"),("title","Title Case"),
                       ("clean","🧹 Clean"),("lines","📋 Each Line ▪️"),("mono","`Mono`")]:
        t="✅ " if key in active else "☐ "
        rows.append([B(f"{t}{label}",f"st_tog_{cid}_{key}")])
    rows.append([B("🗑 Clear All Styles",f"st_clr_{cid}")])
    if active: rows.append([B(f"Active: {', '.join(sorted(active))}","noop")])
    rows.append([B("── 😊 Emoji ──","noop")])
    rows.append([B(f"Emoji: {es}  (tap to set)",f"emoji_set_{cid}")])
    for pk,plabel in EMOJI_POS.items():
        t="✅ " if ep==pk else ""
        rows.append([B(f"{t}{plabel}",f"emoji_pos_{cid}_{pk}")])
    rows.append([B("🗑 Clear Emoji",f"emoji_clr_{cid}")])
    rows.append([B("🧪 Test Style",f"ai_t_{cid}")])
    rows.append([B("‹ Back",f"ch_{cid}"),B("🏠 Home","main")]); return IM(rows)

def KB_AI_G(uid):
    chs=ch_all(uid); rows=[[B("✨ Text Style","noop")],[B("🌐 Change ALL Channels","ai_g_all")]]
    for ch in chs:
        active=parse_style_set(ch.get('free_style','none'))
        lbl=', '.join(sorted(active)) if active else "❌ Off"
        rows.append([B(f"{ch['ch_name'][:24]} — {lbl}",f"ai_{ch['id']}")])
    if not chs: rows.append([B("No channels","ch_new")])
    rows.append([B("‹ Back","settings")]); return IM(rows)

def KB_AI_GLOBAL(uid):
    rows=[[B("🌐 Change ALL Style","noop")]]
    for k,lbl in [("bold","𝗕𝗼𝗹𝗱"),("italic","𝘐𝘵𝘢𝘭𝘊"),("bold_italic","𝗕+𝘐 Bold+Italic"),
                   ("caps","CAPS"),("lower","lower"),("title","Title"),
                   ("clean","🧹 Clean"),("lines","📋 Lines"),("mono","`Mono`"),("none","❌ Clear All")]:
        rows.append([B(lbl,f"ai_g_set_{k}")])
    rows.append([B("‹ Back","ai_g")]); return IM(rows)

def KB_DESTS(cid):
    ch=ch_get(cid); dests=json.loads(ch['dests'])
    rows=[[B(f"📤 Destinations  •  {ch['ch_name'][:18]}","noop")]]
    for d in dests: rows.append([B(f"▸ {d['name'][:30]}","noop"),B("🗑",f"d_dest_{cid}_{d['id']}")])
    if not dests: rows.append([B("No destinations yet","noop")])
    rows.append([B("➕ Add Destination",f"dest_pick_{cid}")])
    rows.append([B("‹ Back",f"ch_{cid}"),B("🏠 Home","main")]); return IM(rows)

def KB_SUB(uid):
    plans=[p for p in plan_all() if p['enabled']]
    rows=[[B(f"💳  {u_sub_str(uid)}","noop")],[B(f"Expires: {u_sub_end_str(uid)}","noop")]]
    for p in plans:
        mx=p.get('max_channels',0); ct="∞ ch" if mx==0 else f"{mx} ch"
        rows.append([B(f"{p['label']}  •  ₹{p['price']}  •  {p['days']}d  •  {ct}",f"buy_{p['key']}")])
    rows.append([B("🔄 Renew","sub_renew"),B(f"💬 {ADMIN_USERNAME}","support")])
    rows.append([B("‹ Back","main")]); return IM(rows)

def KB_ADM():
    tu,au,tf,tr,tb,td,nw=adm_stats()
    return IM([
        [B(f"🔧 Admin  •  {tu}👥  {au}✅  {tb}🚫","noop")],
        [B(f"📨 {tf} fwd  •  📅 {td} today  •  🆕 {nw}/7d","noop")],
        [B("👥 Users","a_users"),         B("✅ Active","a_active")],
        [B("✅ Give Sub","a_give"),        B("❌ Revoke","a_revoke")],
        [B("🚫 Ban","a_ban"),              B("✅ Unban","a_unban")],
        [B("🔍 Search","a_search")],
        [B("📢 Broadcast All","a_bc_all"),B("📢 Active Only","a_bc_act")],
        [B("📊 Stats","a_stats"),          B("📡 Channels","a_chs")],
        [B("💰 Revenue","a_revenue"),      B("🔄 Restart All","a_restart")],
        [B("💳 Plan Management","a_plans")],
    ])

def KB_PLANS():
    plans=plan_all(); trial=get_trial_days()
    rows=[[B("💳 Plan Management","noop")],
          [B(f"🎁 Trial Days: {trial}  (tap to change)","a_trial")],
          [B("─────────────────────","noop")]]
    for p in plans:
        st="🟢" if p['enabled'] else "🔴"; mx=p.get('max_channels',0); ct="∞" if mx==0 else str(mx)
        rows.append([
            B(f"{st} {p['label']}  ₹{p['price']}  {p['days']}d  [{ct} ch]",f"a_plan_edit_{p['key']}"),
            B("🔴" if p['enabled'] else "🟢",f"a_plan_tog_{p['key']}"),
            B("🗑",f"a_plan_del_{p['key']}"),
        ])
    rows.append([B("➕ Add New Plan","a_plan_add")]); rows.append([B("‹ Back","adm")]); return IM(rows)

# ═══════════════════════════════════════════════════════════════
#   QR LOGIN
# ═══════════════════════════════════════════════════════════════
QR_CL:dict={}

async def do_qr(update:Update,ctx):
    uid=update.effective_user.id
    msg=update.callback_query.message if update.callback_query else update.message
    if s_get(uid):
        if uid not in ACL: asyncio.create_task(eng_start(uid))
        # Already logged in — clean edit/send main menu
        if update.callback_query:
            await clean_edit(update.callback_query,uid,f"⚡ {BOT_NAME}  •  {u_sub_str(uid)}",KB_MAIN(uid))
        else:
            await clean_send(msg,uid,f"⚡ {BOT_NAME}  •  {u_sub_str(uid)}",KB_MAIN(uid))
        return
    old_cl=QR_CL.pop(uid,None)
    if old_cl:
        try: await old_cl.disconnect()
        except: pass
    cl=TelegramClient(StringSession(),API_ID,API_HASH)
    await cl.connect(); QR_CL[uid]=cl; fp=f"qr_{uid}.png"
    # Delete any old menu message before starting QR flow
    await _safe_delete(MENU_MSG.pop(uid,None))
    prog=await msg.reply_text("⏳ Generating QR code...")
    try:
        qr=await cl.qr_login(); qrcode.make(qr.url).save(fp)
        await _safe_delete(prog)
        qrmsg=await msg.reply_photo(open(fp,"rb"),
            caption=f"📱 {BOT_NAME} Login\n\n1️⃣ Open Telegram\n2️⃣ Settings → Devices\n3️⃣ Link Device → Scan\n\n⏳ Expires: 30s")
        if os.path.exists(fp): os.remove(fp)
        start_time=time.time()
        timer_msg=await msg.reply_text("⏳ QR: 0:30 remaining...")
        timer_task=asyncio.create_task(_qr_countdown(timer_msg,start_time,QR_TIMEOUT))
        try: await asyncio.wait_for(qr.wait(),timeout=QR_TIMEOUT)
        except asyncio.TimeoutError:
            timer_task.cancel()
            await _safe_delete(timer_msg); await _safe_delete(qrmsg)
            # Show expired notice as clean menu
            expired=await msg.reply_text("⏰ QR Expired! Tap below to try again.",reply_markup=KB([B("🔄 Try Again","qr_login")]))
            MENU_MSG[uid]=expired
            try: await cl.disconnect()
            except: pass
            return
        timer_task.cancel()
        # ── Login SUCCESS — delete ALL QR messages, show clean menu ──
        await _safe_delete(timer_msg)
        await _safe_delete(qrmsg)
        sess=cl.session.save(); s_save(uid,sess); SESSION_CACHE[uid]=sess; KNOWN_USERS.add(uid)
        is_new=u_upsert(uid,update.effective_user.username or "",update.effective_user.first_name or "")
        me=await cl.get_me(); nm=me.first_name or ""; un=f"@{me.username}" if me.username else str(me.id)
        ACL[uid]=cl
        greet="🎁 Free Trial: "+str(get_trial_days())+" days!" if is_new else "💳 "+u_sub_str(uid)
        menu=await msg.reply_text(
            f"✅ Login Successful!\n👤 {nm}  ({un})\n{greet}",
            reply_markup=KB_MAIN(uid))
        MENU_MSG[uid]=menu
        asyncio.create_task(eng_start(uid)); asyncio.create_task(fetch_chats(uid))
    except Exception as e:
        LOG.error(f"QR:{e}")
        err=await msg.reply_text(f"❌ Error: {e}\n\n/start karo")
        MENU_MSG[uid]=err
        try: await cl.disconnect()
        except: pass
    finally:
        if os.path.exists(fp): os.remove(fp)
        QR_CL.pop(uid,None)

async def _qr_countdown(timer_msg,start_time,total):
    try:
        for sec in [25,20,15,10,5]:
            st=total-sec-(time.time()-start_time)
            if st>0: await asyncio.sleep(st)
            try: await timer_msg.edit_text(f"⏳ QR expires in: {sec} seconds...")
            except: pass
    except asyncio.CancelledError: pass

# ═══════════════════════════════════════════════════════════════
#   CHANNEL PICKER
# ═══════════════════════════════════════════════════════════════
async def show_picker(uid,mode,page,edit_target,cid_dest=None):
    if mode=='src':
        # Source: ALL channels user is member of
        chats=SRC_CACHE.get(uid,[])
        if not chats:
            chats=await fetch_chats(uid)
            chats=SRC_CACHE.get(uid,[])
        hdr=(
            "📡  Select SOURCE Channel\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "💡 All channels/groups shown\n\n"
        )
        prefix="ps"
        empty_msg="❌ Koi channel nahi mila!\n\nKisi channel/group ko join karo pehle."
    else:
        # Destination: ONLY channels/groups where user is admin or can post
        chats=DEST_CACHE.get(uid,[])
        if not chats:
            chats=await fetch_chats(uid)
            chats=DEST_CACHE.get(uid,[])
        hdr=(
            "📤  Select DESTINATION Channel\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "✅ Sirf wahi channels jahan\n"
            "   aapko Admin access hai\n\n"
        )
        prefix=f"pd_{cid_dest}"
        empty_msg=(
            "❌ Koi destination nahi mili!\n\n"
            "Destination ke liye:\n"
            "• Broadcast channel mein Admin bano\n"
            "• Ya kisi group mein message bhejne\n"
            "  ka permission lo\n\n"
            "Phir 🔄 Refresh karo."
        )

    if not chats:
        try: await edit_target.edit_text(empty_msg,reply_markup=IM([[IB("🔄 Refresh",callback_data=f"refr_{'ps' if mode=='src' else f'pd_{cid_dest}'}")]]))
        except: await edit_target.reply_text(empty_msg)
        return

    total=len(chats); list_txt,_=chat_list_text(chats,page)
    txt=hdr+list_txt+f"\n━━━━━━━━━━━━━━━━━━━━\n📋 Total: {total}"
    kb=num_kb(chats,page,prefix,total)
    try:
        await edit_target.edit_text(txt,reply_markup=kb)
        MENU_MSG[uid]=edit_target
    except:
        await _safe_delete(MENU_MSG.pop(uid,None))
        try:
            sent=await edit_target.reply_text(txt,reply_markup=kb)
            MENU_MSG[uid]=sent
        except: pass

# ═══════════════════════════════════════════════════════════════
#   STATE
# ═══════════════════════════════════════════════════════════════
TEMP:dict={}
(ST_LO,ST_LN,ST_WO,ST_WN,ST_WL,ST_BL,ST_PB,
 ST_HDR,ST_FTR,ST_DLY,ST_AIT,ST_REN,
 ST_BC,ST_GID,ST_GDY,ST_SRCH,
 ST_PLAN_PRICE,ST_PLAN_DAYS,ST_PLAN_MAXCH,ST_PLAN_NEW,ST_TRIAL,
 ST_EMOJI,ST_PRO,ST_PRN) = range(24)

# ═══════════════════════════════════════════════════════════════
#   COMMANDS
# ═══════════════════════════════════════════════════════════════
async def _delete_cmd(u):
    """Delete the user's command message silently."""
    try: await u.message.delete()
    except: pass

async def cmd_start(u:Update,ctx):
    uid=u.effective_user.id
    await _delete_cmd(u)  # delete /start command message
    sess=s_get(uid)
    if uid in ADMIN_IDS:
        await clean_send(u.message,uid,"🔧 Admin Panel",KB_ADM()); return
    if sess or uid in ACL:
        if sess and uid not in SESSION_CACHE: SESSION_CACHE[uid]=sess
        if sess and uid not in ACL: asyncio.create_task(eng_start(uid))
        u_upsert(uid,u.effective_user.username or "",u.effective_user.first_name or "")
        await clean_send(u.message,uid,f"⚡ {BOT_NAME}  •  {u_sub_str(uid)}",KB_MAIN(uid)); return
    await clean_send(u.message,uid,
        f"⚡ {BOT_NAME} — Pro Forwarder\n\n📡 Auto channel detect\n"
        f"📹 Media control • ✂️ Filters\n📝 Para block/replace\n"
        f"🎁 {get_trial_days()} days free trial\n\nAdmin: {ADMIN_USERNAME}\n\n👇 Login — No OTP!",
        KB([B("📱 Login with QR Code","qr_login")]))

async def cmd_menu(u:Update,ctx):
    uid=u.effective_user.id
    await _delete_cmd(u)
    if uid in ADMIN_IDS:
        await clean_send(u.message,uid,"🔧 Admin Panel",KB_ADM()); return
    if not s_get(uid):
        await clean_send(u.message,uid,"❌ Pehle /start karo!"); return
    await clean_send(u.message,uid,f"⚡ {BOT_NAME}  •  {u_sub_str(uid)}",KB_MAIN(uid))

async def cmd_admin(u:Update,ctx):
    if u.effective_user.id not in ADMIN_IDS: return
    await _delete_cmd(u)
    await clean_send(u.message,u.effective_user.id,"🔧 Admin Panel",KB_ADM())

async def cmd_cancel(u:Update,ctx):
    uid=u.effective_user.id; ctx.user_data.clear(); TEMP.pop(uid,None)
    await _delete_cmd(u)
    kb=KB_ADM() if uid in ADMIN_IDS else (KB_MAIN(uid) if s_get(uid) else None)
    if kb:
        txt="🔧 Admin Panel" if uid in ADMIN_IDS else f"⚡ {BOT_NAME}  •  {u_sub_str(uid)}"
        await clean_send(u.message,uid,txt,kb)

# ═══════════════════════════════════════════════════════════════
#   CALLBACK
# ═══════════════════════════════════════════════════════════════
async def cbk(update:Update,ctx):
    q=update.callback_query; await q.answer()
    d=q.data; uid=update.effective_user.id

    async def ED(txt,kb=None):
        # Use clean_edit: edits in-place, falls back to delete+resend
        await clean_edit(q,uid,txt,kb)
    async def ANS(m,alert=False):
        try: await q.answer(m,show_alert=alert)
        except: pass

    if d=="noop": return
    if d=="qr_login": await do_qr(update,ctx); return

    is_admin=uid in ADMIN_IDS
    if is_admin and (d in ("main","adm") or d.startswith("a_")):
        await _admin_cbk(d,uid,q,ctx,ED,ANS); return

    if not is_admin:
        has_sess=(uid in ACL) or (uid in SESSION_CACHE) or (uid in KNOWN_USERS)
        if not has_sess:
            try:
                c=DB(); r=c.execute("SELECT sess FROM sessions WHERE uid=?",(uid,)).fetchone(); c.close()
                if r and r['sess']: SESSION_CACHE[uid]=r['sess']; KNOWN_USERS.add(uid); has_sess=True
            except: pass
        if not has_sess:
            await clean_edit(q,uid,f"⚡ {BOT_NAME}\n\nLogin karo:",KB([B("📱 Login with QR Code","qr_login")]))
            return
        if uid not in ACL: asyncio.create_task(eng_start(uid))

    # ── MAIN ──
    if d=="main":
        if uid in ADMIN_IDS: await ED("🔧 Admin Panel",KB_ADM()); return
        await ED(f"⚡ {BOT_NAME}  •  {u_sub_str(uid)}",KB_MAIN(uid))
    elif d=="settings": await ED("⚙️ Settings",KB_SETTINGS(uid))
    elif d=="bot_tog":
        cur=u_bot_on(uid); u_toggle_bot(uid,not cur)
        await ANS(f"Bot {'🟢 ON' if not cur else '🔴 OFF'}",True); await ED("⚙️ Settings",KB_SETTINGS(uid))
    elif d=="eng_r":
        await eng_stop(uid); ok=await eng_start(uid)
        await ANS("✅ Engine Restarted!" if ok else "❌ Error!",True); await ED("⚙️ Settings",KB_SETTINGS(uid))
    elif d=="logout":
        await eng_stop(uid); s_del(uid)
        SRC_CACHE.pop(uid,None); DEST_CACHE.pop(uid,None); CHAT_CACHE.pop(uid,None)
        await ED("🚪 Logged out!\n/start se login karo.")
    elif d=="support":
        await ED(f"🆘 Support\n\nAdmin: {ADMIN_USERNAME}\n\nYour ID: `{uid}`",KB([B(f"💬 Contact {ADMIN_USERNAME}","noop"),B("‹ Back","main")]))

    # ── CHANNELS ──
    elif d=="chs": await ED("📡  My Channels",KB_CHS(uid))
    elif d=="ch_new":
        if not u_ok(uid): await ANS("❌ Subscription needed!",True); return
        can,msg2=u_can_add_channel(uid)
        if not can: await ED(msg2,KB([B("💳 Upgrade Plan","sub"),B("‹ Back","chs")])); return
        chats=SRC_CACHE.get(uid,[])
        if not chats:
            await _safe_delete(MENU_MSG.pop(uid,None))
            p=await q.message.reply_text("📡 Fetching all channels...")
            await fetch_chats(uid)
            await _safe_delete(p)
        await show_picker(uid,'src',0,q.message)

    elif d.startswith("ch_") and d[3:].isdigit():
        cid=int(d[3:]); ch=ch_get(cid)
        if not ch: return
        dests=json.loads(ch['dests'])
        await ED(f"📡  {ch['ch_name']}\n▸ {ch['src_name']}\n📤 {len(dests)} dest  •  {'🟢 ON' if ch['enabled'] else '🔴 OFF'}",KB_CH(cid))

    elif d.startswith("ch_del_"):
        cid=int(d[7:]); ch_del(cid,uid); await eng_stop(uid); asyncio.create_task(eng_start(uid))
        await ANS("🗑 Deleted!",True); await ED("📡 My Channels",KB_CHS(uid))
    elif d.startswith("ch_tog_"):
        cid=int(d[7:]); new=ch_toggle(cid); await eng_stop(uid); asyncio.create_task(eng_start(uid))
        await ANS(f"{'🟢 Enabled' if new else '🔴 Disabled'}",True)
        await ED(f"📡 {ch_get(cid)['ch_name']}",KB_CH(cid))
    elif d.startswith("ch_ren_"):
        cid=int(d[7:]); TEMP[uid]={'cid':cid}; ctx.user_data['st']=ST_REN
        await _safe_delete(MENU_MSG.pop(uid,None))
        p=await q.message.reply_text("✏️ New name bhejo:\n\n/cancel")
        TEMP[uid]['prompt_id']=p.message_id
    elif d.startswith("ch_stat_"):
        cid=int(d[8:]); ch=ch_get(cid)
        c=DB()
        tot=c.execute("SELECT COUNT(*) as n FROM fwd_log WHERE ch_id=?",(cid,)).fetchone()['n']
        tod=c.execute("SELECT COUNT(*) as n FROM fwd_log WHERE ch_id=? AND ts>?",(cid,time.time()-86400)).fetchone()['n']
        c.close()
        blk=[MEDIA_TYPES[k][0] for k,(_,col) in [(k,MEDIA_TYPES[k]) for k in MEDIA_TYPES] if ch.get(col)]
        active=parse_style_set(ch.get('free_style','none'))
        hdr_t=f"\n📝 Header: {_short(ch.get('header',''),30)}" if ch.get('header') else ""
        ftr_t=f"\n📝 Footer: {_short(ch.get('footer',''),30)}" if ch.get('footer') else ""
        blk_t=f"\n📹 Blocked: {', '.join(blk)}" if blk else "\n📹 Blocked: None"
        await ED(
            f"📊  Stats: {ch['ch_name']}\n\n"
            f"📨 Total Fwd : {tot}\n📅 Today     : {tod}\n"
            f"📤 Dest      : {len(json.loads(ch['dests']))}\n"
            f"✨ Style     : {', '.join(sorted(active)) or 'None'}"
            f"{hdr_t}{ftr_t}{blk_t}",
            KB([B("‹ Back",f"ch_{cid}"),B("🏠 Home","main")]))

    # ═══════════════════════════════════════════════════════════
    #   MEDIA CONTROL CALLBACKS
    # ═══════════════════════════════════════════════════════════
    elif d.startswith("media_") and d[6:].isdigit():
        # media_{cid} — open media panel
        cid=int(d[6:]); await ED("📹 Media Control",KB_MEDIA(cid))

    elif d.startswith("media_blkall_"):
        # Block ALL media types
        cid=int(d[13:])
        kw={col:1 for _,(lbl,col) in MEDIA_TYPES.items()}; ch_upd(cid,**kw)
        await ANS("🔴 ALL Media Blocked!",True); await ED("📹 Media Control",KB_MEDIA(cid))

    elif d.startswith("media_alwall_"):
        # Allow ALL media types
        cid=int(d[13:])
        kw={col:0 for _,(lbl,col) in MEDIA_TYPES.items()}; ch_upd(cid,**kw)
        await ANS("✅ ALL Media Allowed!",True); await ED("📹 Media Control",KB_MEDIA(cid))

    elif d.startswith("mt_"):
        # mt_{cid}_{col}  — toggle individual media type
        # Format: mt_123_blk_voice  or  mt_123_blk_vidmsg
        rest=d[3:]                          # "123_blk_voice"
        underscore_idx=rest.index('_')     # first underscore
        cid=int(rest[:underscore_idx])     # 123
        col=rest[underscore_idx+1:]        # "blk_voice"
        ch=ch_get(cid)
        if not ch: await ANS("❌ Not found!",True); return
        # Validate col is a known media col
        valid_cols={v[1] for v in MEDIA_TYPES.values()}
        if col not in valid_cols: await ANS("❌ Invalid!",True); return
        current_val=bool(ch.get(col,0))
        new_val=0 if current_val else 1
        ch_upd(cid,**{col:new_val})
        # Find label
        mname=next((v[0] for v in MEDIA_TYPES.values() if v[1]==col),"Media")
        status="🔴 BLOCKED" if new_val else "✅ ALLOWED"
        await ANS(f"{status}: {mname}",True)
        await ED("📹 Media Control",KB_MEDIA(cid))

    # ── PICKER ──
    elif d.startswith("pg_"):
        parts=d.split("_"); page=int(parts[-1])
        if "ps" in d: await show_picker(uid,'src',page,q.message)
        elif "pd_" in d:
            # pg_pd_{cid}_{page}
            cid=int(parts[2]); await show_picker(uid,'dest',page,q.message,cid)
    elif d.startswith("refr_"):
        await _safe_delete(MENU_MSG.pop(uid,None))
        p=await q.message.reply_text("🔄 Refreshing channels...")
        await fetch_chats(uid)
        await _safe_delete(p)
        if "ps" in d:
            await show_picker(uid,'src',0,q.message)
        else:
            cid=TEMP.get(uid,{}).get('cid')
            await show_picker(uid,'dest',0,q.message,cid)
    elif d.startswith("ps_"):
        idx=int(d[3:]); chats=SRC_CACHE.get(uid,[])
        if idx>=len(chats): await ANS("❌ Invalid!"); return
        chat=chats[idx]; cid=ch_add(uid,chat['id'],chat['name'],chat['name'][:25])
        asyncio.create_task(eng_start(uid)); TEMP[uid]={'cid':cid}
        await show_picker(uid,'dest',0,q.message,cid)
    elif d.startswith("pd_"):
        parts=d.split("_"); cid=int(parts[1]); idx=int(parts[2])
        chats=DEST_CACHE.get(uid,[])
        if idx>=len(chats): await ANS("❌ Invalid!"); return
        chat=chats[idx]; ch_add_dest(cid,chat['id'],chat['name']); asyncio.create_task(eng_start(uid))
        ch=ch_get(cid)
        await ED(
            f"╔══ ✅ Setup Complete! ══╗\n\n📡 Source : {ch['src_name']}\n📤 Dest   : {chat['name']}\n\n🎉 Forwarding active!\n╚{'═'*24}╝",
            IM([[B("➕ More Dest",f"dest_pick_{cid}")],
                [B("⚙️ Control",f"ctrl_{cid}"),B("📹 Media",f"media_{cid}")],
                [B("✂️ Filters",f"flt_{cid}"),B("✨ Style",f"ai_{cid}")],
                [B("📡 Channels","chs"),B("🏠 Home","main")]]))
    elif d.startswith("dest_pick_"):
        cid=int(d[10:]); TEMP[uid]={'cid':cid}; await show_picker(uid,'dest',0,q.message,cid)

    # ── CONTROL ──
    elif d=="ctrl_g": await ED("⚙️ All Channels",KB_CTRL_G(uid))
    elif d.startswith("ctrl_") and d[5:].isdigit(): cid=int(d[5:]); await ED("⚙️ Control",KB_CTRL(cid))
    elif d.startswith("t_copy_"):
        cid=int(d[7:]); ch=ch_get(cid); ch_upd(cid,copy_mode=0 if ch['copy_mode'] else 1); await ED("⚙️",KB_CTRL(cid))
    elif d.startswith("t_sil_"):
        cid=int(d[6:]); ch=ch_get(cid); ch_upd(cid,silent=0 if ch['silent'] else 1); await ED("⚙️",KB_CTRL(cid))
    elif d.startswith("t_pin_"):
        cid=int(d[6:]); ch=ch_get(cid); ch_upd(cid,pin_msg=0 if ch['pin_msg'] else 1); await ED("⚙️",KB_CTRL(cid))
    elif d.startswith("t_cap_"):
        cid=int(d[6:]); ch=ch_get(cid); ch_upd(cid,remove_cap=0 if ch['remove_cap'] else 1); await ED("⚙️",KB_CTRL(cid))
    elif d.startswith("t_med_"):
        cid=int(d[6:]); ch=ch_get(cid); ch_upd(cid,media_only=0 if ch['media_only'] else 1,text_only=0); await ED("⚙️",KB_CTRL(cid))
    elif d.startswith("t_txt_"):
        cid=int(d[6:]); ch=ch_get(cid); ch_upd(cid,text_only=0 if ch['text_only'] else 1,media_only=0); await ED("⚙️",KB_CTRL(cid))
    elif d.startswith("t_blk_"):
        cid=int(d[6:]); ch=ch_get(cid); ch_upd(cid,block_links=0 if ch['block_links'] else 1); await ED("⚙️",KB_CTRL(cid))
    elif d.startswith("t_ball_"):
        cid=int(d[7:]); ch=ch_get(cid)
        cur=bool(ch.get('block_all_links') or ch.get('block_links')); nv=0 if cur else 1
        ch_upd(cid,block_all_links=nv,block_links=nv,block_www=nv,block_tme=nv)
        await ANS("🔴 ALL Links BLOCKED!" if nv else "⚪ OFF",True); await ED("✂️ Filters",KB_FLT(cid))
    elif d.startswith("t_bwww_"):
        cid=int(d[7:]); ch=ch_get(cid); ch_upd(cid,block_www=0 if ch.get('block_www') else 1); await ED("✂️",KB_FLT(cid))
    elif d.startswith("t_btme_"):
        cid=int(d[7:]); ch=ch_get(cid); ch_upd(cid,block_tme=0 if ch.get('block_tme') else 1); await ED("✂️",KB_FLT(cid))
    elif d.startswith("t_bat_"):
        cid=int(d[6:]); ch=ch_get(cid); ch_upd(cid,block_at=0 if ch.get('block_at') else 1); await ED("✂️",KB_FLT(cid))
    elif d.startswith("t_dup_"):
        cid=int(d[6:]); ch=ch_get(cid); ch_upd(cid,dup_check=0 if ch['dup_check'] else 1); await ED("⚙️",KB_CTRL(cid))
    elif d.startswith("t_bold_"):
        cid=int(d[7:]); ch=ch_get(cid); ch_upd(cid,fmt_bold=0 if ch['fmt_bold'] else 1); await ED("📐",KB_FMT(cid))
    elif d.startswith("t_clean_"):
        cid=int(d[8:]); ch=ch_get(cid); ch_upd(cid,fmt_clean=0 if ch['fmt_clean'] else 1); await ED("📐",KB_FMT(cid))
    elif d.startswith("s_dly_"):
        cid=int(d[6:]); TEMP[uid]={'cid':cid}; ctx.user_data['st']=ST_DLY
        m=await q.message.reply_text("⏱ Delay seconds (0-3600):\n\n/cancel"); TEMP[uid]['prompt_id']=m.message_id
    elif d.startswith("s_hdr_"):
        cid=int(d[6:]); ch=ch_get(cid); cur=ch.get('header','') or ''
        TEMP[uid]={'cid':cid}; ctx.user_data['st']=ST_HDR
        await _safe_delete(MENU_MSG.pop(uid,None))
        m=await q.message.reply_text(f"📝 Header bhejo:\n{'Current: '+_short(cur,40) if cur else 'None'}\n\n(- = clear)\n\n/cancel")
        TEMP[uid]['prompt_id']=m.message_id
    elif d.startswith("s_ftr_"):
        cid=int(d[6:]); ch=ch_get(cid); cur=ch.get('footer','') or ''
        TEMP[uid]={'cid':cid}; ctx.user_data['st']=ST_FTR
        await _safe_delete(MENU_MSG.pop(uid,None))
        m=await q.message.reply_text(f"📝 Footer bhejo:\n{'Current: '+_short(cur,40) if cur else 'None'}\n\n(- = clear)\n\n/cancel")
        TEMP[uid]['prompt_id']=m.message_id

    # ── FORMAT ──
    elif d.startswith("fmt_") and d[4:].isdigit(): cid=int(d[4:]); await ED("📐",KB_FMT(cid))
    elif d=="fmt_g": await ED("📐 Format",KB_FMT_G(uid))

    # ── TEXT STYLE ──
    elif d=="ai_g": await ED("✨ Text Style",KB_AI_G(uid))
    elif d.startswith("ai_") and d[3:].isdigit(): cid=int(d[3:]); await ED("✨ Style",KB_AI(cid))
    elif d.startswith("st_tog_"):
        rest=d[7:]; ui=rest.index('_'); cid=int(rest[:ui]); sk=rest[ui+1:]
        ch=ch_get(cid)
        if not ch: await ANS("❌",True); return
        cur=parse_style_set(ch.get('free_style','none'))
        if sk in cur:
            cur.discard(sk)
            if sk in STYLE_COMBOS:
                for sub in STYLE_COMBOS[sk]: cur.discard(sub)
        else:
            cur.add(sk)
            if sk in STYLE_COMBOS:
                for sub in STYLE_COMBOS[sk]: cur.add(sub)
        ch_upd(cid,free_style=encode_style_set(cur))
        await ANS(f"{'✅' if sk in cur else '☐'} {STYLE_DEFS.get(sk,sk)}",False); await ED("✨ Style",KB_AI(cid))
    elif d.startswith("st_clr_"):
        cid=int(d[7:]); ch_upd(cid,free_style='none'); await ANS("🗑 Cleared!",True); await ED("✨ Style",KB_AI(cid))
    elif d.startswith("emoji_set_"):
        cid=int(d[10:]); TEMP[uid]={'cid':cid}; ctx.user_data['st']=ST_EMOJI
        m=await q.message.reply_text("😊 Emoji bhejo:\n\nExample: 🔥  ✅  📊🔥\n\n/cancel"); TEMP[uid]['prompt_id']=m.message_id
    elif d.startswith("emoji_pos_"):
        rest=d[10:]; i=rest.index('_'); cid=int(rest[:i]); pos=rest[i+1:]
        ch_upd(cid,emoji_pos=pos); await ANS(f"✅ {EMOJI_POS.get(pos,'?')}",True); await ED("✨ Style",KB_AI(cid))
    elif d.startswith("emoji_clr_"):
        cid=int(d[10:]); ch_upd(cid,emoji_str='',emoji_pos='off'); await ANS("🗑 Cleared!",True); await ED("✨ Style",KB_AI(cid))
    elif d=="ai_g_all": await ED("🌐 Change ALL",KB_AI_GLOBAL(uid))
    elif d.startswith("ai_g_set_"):
        style=d[9:]; chs=ch_all(uid)
        for ch in chs: ch_upd(ch['id'],free_style='none' if style=='none' else style)
        await ANS(f"✅ {len(chs)} channels updated",True); await ED("✨ Style",KB_AI_G(uid))
    elif d.startswith("ai_t_"):
        cid=int(d[5:]); TEMP[uid]={'cid':cid}; ctx.user_data['st']=ST_AIT
        m=await q.message.reply_text("🧪 Test text bhejo:\n\n/cancel"); TEMP[uid]['prompt_id']=m.message_id

    # ── REPLACEMENTS ──
    elif d=="repl_g": await ED("🔄 Replacements",KB_REPL_G(uid))
    elif d.startswith("repls_"): cid=int(d[6:]); await ED("🔄",KB_REPLS(cid))
    elif d.startswith("add_lrp_"):
        cid=int(d[8:]); TEMP[uid]={'cid':cid,'rt':'link'}; ctx.user_data['st']=ST_LO
        m=await q.message.reply_text("🔗 Purana link bhejo:\n\n/cancel"); TEMP[uid]['prompt_id']=m.message_id
    elif d.startswith("add_wrp_"):
        cid=int(d[8:]); TEMP[uid]={'cid':cid,'rt':'word'}; ctx.user_data['st']=ST_WO
        m=await q.message.reply_text("📝 Purana word bhejo:\n\n/cancel"); TEMP[uid]['prompt_id']=m.message_id
    elif d.startswith("add_prp_"):
        cid=int(d[8:]); TEMP[uid]={'cid':cid}; ctx.user_data['st']=ST_PRO
        await _safe_delete(MENU_MSG.pop(uid,None))
        m=await q.message.reply_text("📋 Para Replace\n\nTrigger word bhejo:\n(Jis paragraph mein ye word ho)\n\n/cancel")
        TEMP[uid]['prompt_id']=m.message_id
    elif d.startswith("d_rp_"): rp_del(int(d[5:])); await ANS("✅ Removed!")

    # ── FILTERS ──
    elif d.startswith("flt_"): cid=int(d[4:]); await ED("✂️ Filters",KB_FLT(cid))
    elif d.startswith("add_wl_"):
        cid=int(d[7:]); TEMP[uid]={'cid':cid,'ft':'whitelist'}; ctx.user_data['st']=ST_WL
        m=await q.message.reply_text("✅ Whitelist word:\n\n/cancel"); TEMP[uid]['prompt_id']=m.message_id
    elif d.startswith("add_bl_"):
        cid=int(d[7:]); TEMP[uid]={'cid':cid,'ft':'blacklist'}; ctx.user_data['st']=ST_BL
        m=await q.message.reply_text("🚫 Blacklist word:\n\n/cancel"); TEMP[uid]['prompt_id']=m.message_id
    elif d.startswith("add_pb_"):
        cid=int(d[7:]); TEMP[uid]={'cid':cid,'ft':'para_block'}; ctx.user_data['st']=ST_PB
        await _safe_delete(MENU_MSG.pop(uid,None))
        m=await q.message.reply_text(
            "📝 Para Block\n\nTrigger word bhejo:\nJis paragraph mein ye word hoga wo DELETE ho jaayega.\n\n/cancel")
        TEMP[uid]['prompt_id']=m.message_id
    elif d.startswith("d_flt_"): f_del(int(d[6:])); await ANS("✅ Removed!")

    # ── DESTINATIONS ──
    elif d.startswith("dests_"): cid=int(d[6:]); await ED("📤 Destinations",KB_DESTS(cid))
    elif d.startswith("d_dest_"):
        parts=d.split("_"); cid=int(parts[2]); did=parts[3]
        ch_del_dest(cid,did); asyncio.create_task(eng_start(uid))
        await ANS("✅ Removed!"); await ED("📤",KB_DESTS(cid))

    # ── SUBSCRIPTION ──
    elif d=="sub": await ED("💳 Subscription",KB_SUB(uid))
    elif d=="sub_renew":
        plans=[p for p in plan_all() if p['enabled']]
        rows=[[B(f"{p['label']}  ₹{p['price']}  {p['days']}d",f"buy_{p['key']}")] for p in plans]+[[B("‹ Back","sub")]]
        await ED(f"🔄 Renewal\n\nCurrent: {u_sub_str(uid)}\n\nPlan choose karo 👇",IM(rows))
    elif d.startswith("buy_"):
        pk=d[4:]; pl=plan_get(pk)
        if not pl: await ANS("❌ Not found!",True); return
        mx=pl.get('max_channels',0); ct="Unlimited channels" if mx==0 else f"Max {mx} channel(s)"
        await ED(
            f"💳 {pl['label']}\n\n💰 Price: ₹{pl['price']}\n⏳ Period: {pl['days']} days\n📡 {ct}\n\n"
            f"Payment:\n━━━━━━━━━\n1️⃣  UPI: (contact admin)\n2️⃣  Amount: ₹{pl['price']}\n"
            f"3️⃣  Screenshot → {ADMIN_USERNAME}\n4️⃣  Write: Plan:{pl['label']} | ID:{uid}\n✅ Activated in 5-10 min",
            KB([B(f"💬 Contact {ADMIN_USERNAME}","support"),B("‹ Back","sub")]))

    # ── STATS ──
    elif d=="stats":
        tf,td,tw=l_stats(uid); chs=ch_all(uid); on_c=sum(1 for c in chs if c['enabled'])
        eng="🟢 Running" if uid in ACL else "🔴 Stopped"
        cur,mx=u_channel_limit(uid); lt=f"{cur}/{mx}" if mx>0 else f"{cur}/∞"
        await ED(
            f"📊  Statistics\n\n📨 Total Fwd : {tf}\n📅 Today     : {td}\n📆 This Week : {tw}\n"
            f"━━━━━━━━━━━━━\n📡 Channels  : {len(chs)}  [{lt}]\n🟢 Active    : {on_c}\n"
            f"━━━━━━━━━━━━━\n⚡ Engine    : {eng}\n💳 {u_sub_str(uid)}",
            KB([B("‹ Back","main")]))

# ── ADMIN CALLBACKS ────────────────────────────────────────────
async def _admin_cbk(d,uid,q,ctx,ED,ANS):
    if d in ("main","adm"): await ED("🔧 Admin Panel",KB_ADM()); return
    elif d=="a_users":
        users=u_all(); lines=[]
        for u in users[:25]:
            st="✅" if u_ok(u['uid']) else "❌"; bn="🚫" if u['is_banned'] else ""
            lines.append(f"{st}{bn} {u.get('name','?')[:12]} | `{u['uid']}` | {u['total_fwd']}fwd")
        await ED(f"👥 Users ({len(users)})\n\n"+"\n".join(lines)+(f"\n...+{len(users)-25} more" if len(users)>25 else ""),KB([B("‹ Back","adm")]))
    elif d=="a_active":
        users=[u for u in u_all() if u_ok(u['uid'])]
        lines=[f"✅ {u.get('name','?')[:14]} | `{u['uid']}` | {u_sub_str(u['uid'])}" for u in users[:25]]
        await ED(f"✅ Active ({len(users)})\n\n"+"\n".join(lines or ["None"]),KB([B("‹ Back","adm")]))
    elif d=="a_stats":
        tu,au,tf,tr,tb,td,nw=adm_stats()
        import datetime; today=datetime.date.today().strftime("%d %b %Y")
        await ED(f"📊 Stats  •  {today}\n\n👥 Total: {tu}\n✅ Active: {au}\n🚫 Banned: {tb}\n🆕 New(7d): {nw}\n⚡ Engines: {len(ACL)}\n━━━━━━━━━━━\n📨 Total Fwd: {tf}\n📅 Today Fwd: {td}\n📡 Channels: {tr}",KB([B("‹ Back","adm")]))
    elif d=="a_revenue":
        users=u_all(); au=sum(1 for u in users if u_ok(u['uid']))
        plans=plan_all(); mp=next((p['price'] for p in plans if p['key']=='m'),49)
        lines="\n".join([f"  {p['label']} ₹{p['price']} | ch: {'∞' if p.get('max_channels',0)==0 else p['max_channels']}" for p in plans])
        await ED(f"💰 Revenue\n\n✅ Active: {au}\n💵 Est.(monthly): ₹{au*mp}\n\nPlans:\n{lines}",KB([B("💳 Plans","a_plans"),B("‹ Back","adm")]))
    elif d=="a_chs":
        c=DB(); rows=c.execute("SELECT ch.id,ch.ch_name,ch.uid,ch.enabled FROM channels ch ORDER BY ch.id DESC LIMIT 30").fetchall(); c.close()
        lines=[f"{'🟢' if r['enabled'] else '🔴'} {r['ch_name'][:16]} | uid:{r['uid']}" for r in rows]
        await ED(f"📡 Channels ({len(rows)})\n\n"+"\n".join(lines or ["None"]),KB([B("‹ Back","adm")]))
    elif d=="a_restart":
        await ANS("🔄 Restarting...",True); asyncio.create_task(eng_restart_all())
    elif d=="a_search": ctx.user_data['st']=ST_SRCH; await q.message.reply_text("🔍 User ID:\n\n/cancel")
    elif d=="a_give":   ctx.user_data['st']=ST_GID; ctx.user_data['sa']='give';   await q.message.reply_text("✅ User ID:\n\n/cancel")
    elif d=="a_revoke": ctx.user_data['st']=ST_GID; ctx.user_data['sa']='revoke'; await q.message.reply_text("❌ User ID:\n\n/cancel")
    elif d=="a_ban":    ctx.user_data['st']=ST_GID; ctx.user_data['sa']='ban';    await q.message.reply_text("🚫 User ID:\n\n/cancel")
    elif d=="a_unban":  ctx.user_data['st']=ST_GID; ctx.user_data['sa']='unban';  await q.message.reply_text("✅ User ID:\n\n/cancel")
    elif d=="a_bc_all": ctx.user_data['st']=ST_BC; ctx.user_data['bt']='all';    await q.message.reply_text("📢 Message (ALL):\n\n/cancel")
    elif d=="a_bc_act": ctx.user_data['st']=ST_BC; ctx.user_data['bt']='active'; await q.message.reply_text("📢 Message (ACTIVE):\n\n/cancel")
    elif d=="a_plans": await ED("💳 Plans",KB_PLANS())
    elif d=="a_trial":
        ctx.user_data['st']=ST_TRIAL
        await q.message.reply_text(f"🎁 Trial Days\nCurrent: {get_trial_days()}\nNaya number:\n\n/cancel")
    elif d.startswith("a_plan_tog_"):
        pk=d[11:]; p=plan_get(pk)
        if p: plan_upd(pk,enabled=0 if p['enabled'] else 1)
        await ANS(f"{'🟢' if not p['enabled'] else '🔴'}",True); await ED("💳 Plans",KB_PLANS())
    elif d.startswith("a_plan_del_"):
        pk=d[11:]; plan_del(pk); await ANS("🗑 Deleted!",True); await ED("💳 Plans",KB_PLANS())
    elif d.startswith("a_plan_edit_"):
        pk=d[12:]; p=plan_get(pk)
        if not p: return
        TEMP[uid]={'plan_key':pk}; ctx.user_data['st']=ST_PLAN_PRICE
        mx=p.get('max_channels',0)
        await q.message.reply_text(f"✏️ Edit: {p['label']}\n\nPrice: ₹{p['price']}\nDays: {p['days']}\nMax Ch: {'∞' if mx==0 else mx}\n\nNaya PRICE (₹):\n\n/cancel")
    elif d=="a_plan_add":
        ctx.user_data['st']=ST_PLAN_NEW
        await q.message.reply_text("➕ New Plan\n\nFormat: LABEL,DAYS,PRICE,MAX_CH\nExample: 💎 Pro,90,149,5\n(MAX_CH=0 = unlimited)\n\n/cancel")

# ═══════════════════════════════════════════════════════════════
#   MESSAGE HANDLER
# ═══════════════════════════════════════════════════════════════
async def msg_hdl(update:Update,ctx):
    uid=update.effective_user.id; txt=(update.message.text or "").strip(); st=ctx.user_data.get('st')

    async def _dp():
        pid=TEMP.get(uid,{}).get('prompt_id')
        if pid:
            try: await ctx.bot.delete_message(uid,pid)
            except: pass
    async def _du():
        try: await update.message.delete()
        except: pass

    if txt=="/cancel":
        await _dp(); ctx.user_data.clear(); TEMP.pop(uid,None)
        await update.message.reply_text("❌ Cancelled.",
            reply_markup=KB_ADM() if uid in ADMIN_IDS else (KB_MAIN(uid) if s_get(uid) else None))
        return

    def _cid(): return TEMP.get(uid,{}).get('cid')

    if st==ST_REN:
        cid=_cid(); ch_upd(cid,ch_name=txt[:30])
        await _dp(); await _du(); ctx.user_data.pop('st',None); TEMP.pop(uid,None)
        msg=await update.message.reply_text(f"✅ Renamed: {txt[:30]}",reply_markup=KB([B("‹ Back",f"ch_{cid or 0}")]))
        MENU_MSG[uid]=msg

    elif st==ST_LO:
        TEMP.setdefault(uid,{})['old']=txt; ctx.user_data['st']=ST_LN
        await _dp(); await _du()
        m=await update.message.reply_text(f"✅ Old: `{txt}`\nNaya link bhejo:\n\n/cancel"); TEMP[uid]['prompt_id']=m.message_id
    elif st==ST_LN:
        cid=_cid(); old=TEMP.get(uid,{}).get('old',''); rp_add(cid,uid,'link',old,txt)
        await _dp(); await _du(); ctx.user_data.pop('st',None); TEMP.pop(uid,None)
        await update.message.reply_text(f"✅ `{old}` → `{txt}`",reply_markup=KB([B("➕ More",f"add_lrp_{cid or 0}"),B("‹",f"repls_{cid or 0}")]))

    elif st==ST_WO:
        TEMP.setdefault(uid,{})['old']=txt; ctx.user_data['st']=ST_WN
        await _dp(); await _du()
        m=await update.message.reply_text(f"✅ Old: `{txt}`\nNaya word bhejo:\n\n/cancel"); TEMP[uid]['prompt_id']=m.message_id
    elif st==ST_WN:
        cid=_cid(); old=TEMP.get(uid,{}).get('old',''); rp_add(cid,uid,'word',old,txt)
        await _dp(); await _du(); ctx.user_data.pop('st',None); TEMP.pop(uid,None)
        await update.message.reply_text(f"✅ `{old}` → `{txt}`",reply_markup=KB([B("➕ More",f"add_wrp_{cid or 0}"),B("‹",f"repls_{cid or 0}")]))

    elif st==ST_PRO:
        TEMP.setdefault(uid,{})['old']=txt; ctx.user_data['st']=ST_PRN
        await _dp(); await _du()
        m=await update.message.reply_text(
            f"📋 Para Replace — Step 2\n\nTrigger: `{txt}`\n\nAb replacement text bhejo:\n('-' = paragraph delete karo)\n\n/cancel")
        TEMP[uid]['prompt_id']=m.message_id
    elif st==ST_PRN:
        cid=_cid(); old=TEMP.get(uid,{}).get('old',''); rp_add(cid,uid,'para',old,txt)
        await _dp(); await _du(); ctx.user_data.pop('st',None); TEMP.pop(uid,None)
        action="🗑 Delete paragraph" if txt=='-' else f"Replace → {_short(txt,30)}"
        await update.message.reply_text(f"✅ Para Replace!\n\nIf para has: `{old}`\n→ {action}",
            reply_markup=KB([B("➕ More",f"add_prp_{cid or 0}"),B("‹",f"repls_{cid or 0}")]))

    elif st in (ST_WL,ST_BL,ST_PB):
        cid=_cid(); ft=TEMP.get(uid,{}).get('ft','whitelist')
        f_add(cid,uid,ft,txt); await _dp(); await _du()
        ctx.user_data.pop('st',None); TEMP.pop(uid,None)
        icon={"whitelist":"✅","blacklist":"🚫","para_block":"📝"}.get(ft,"✅")
        lbl={"whitelist":"Whitelist","blacklist":"Blacklist","para_block":"Para Block"}.get(ft,ft)
        kb_add={"whitelist":f"add_wl_{cid or 0}","blacklist":f"add_bl_{cid or 0}","para_block":f"add_pb_{cid or 0}"}.get(ft,f"flt_{cid or 0}")
        await update.message.reply_text(f"{icon} {lbl}: `{txt}`",reply_markup=KB([B("➕ More",kb_add),B("‹",f"flt_{cid or 0}")]))

    elif st==ST_DLY:
        cid=_cid()
        try:
            v=max(0,min(3600,int(txt))); ch_upd(cid,delay_sec=v)
            await _dp(); await _du()
            await update.message.reply_text(f"✅ Delay: {v}s",reply_markup=KB([B("‹",f"ctrl_{cid or 0}")]))
        except: await update.message.reply_text("❌ Number (0-3600)!")
        ctx.user_data.pop('st',None); TEMP.pop(uid,None)

    elif st==ST_HDR:
        cid=_cid(); ch_upd(cid,header="" if txt=="-" else txt)
        await _dp(); await _du(); ctx.user_data.pop('st',None); TEMP.pop(uid,None)
        p=f"\nPreview: {_short(txt,40)}" if txt!="-" else ""
        await update.message.reply_text(f"✅ Header {'cleared' if txt=='-' else 'set'}!{p}",reply_markup=KB([B("‹",f"ctrl_{cid or 0}")]))

    elif st==ST_FTR:
        cid=_cid(); ch_upd(cid,footer="" if txt=="-" else txt)
        await _dp(); await _du(); ctx.user_data.pop('st',None); TEMP.pop(uid,None)
        p=f"\nPreview: {_short(txt,40)}" if txt!="-" else ""
        await update.message.reply_text(f"✅ Footer {'cleared' if txt=='-' else 'set'}!{p}",reply_markup=KB([B("‹",f"ctrl_{cid or 0}")]))

    elif st==ST_AIT:
        cid=_cid(); ch=ch_get(cid) if cid else None
        await _dp(); await _du(); ctx.user_data.pop('st',None); TEMP.pop(uid,None)
        if not ch: await update.message.reply_text("❌ Channel not found."); return
        await update.message.reply_text("⏳ Processing...")
        styled,use_html=await ai_rewrite(txt,ch)
        hdr=(ch.get('header') or '').strip(); ftr=(ch.get('footer') or '').strip()
        ss=ch.get('free_style','none')
        if hdr:
            h,_=apply_styles(hdr,ss) if (ss and ss!='none') else (_he(hdr) if use_html else hdr,False)
            styled=f"{h}\n\n{styled}" if styled else h
        if ftr:
            f2,_=apply_styles(ftr,ss) if (ss and ss!='none') else (_he(ftr) if use_html else ftr,False)
            styled=f"{styled}\n\n{f2}" if styled else f2
        para_r=para_process(txt,ch['id']); para_note=f"\n📝 Para: {'[BLOCKED]' if para_r is None else _short(para_r,40)}"
        active=parse_style_set(ch.get('free_style','none'))
        await update.message.reply_text(
            f"🧪 Style Test\nStyles: {', '.join(sorted(active)) or 'None'}{para_note}\n\n📥 Original:\n{txt[:200]}\n\n📤 Styled:",
            reply_markup=KB([B("⚙️ Style",f"ai_{cid}"),B("🏠 Home","main")]))
        pm='HTML' if use_html else None
        try: await update.message.reply_text(styled[:1000],parse_mode=pm)
        except: await update.message.reply_text(f"(preview err)\n{styled[:500]}")

    elif st==ST_EMOJI:
        cid=TEMP.get(uid,{}).get('cid')
        await _dp(); await _du(); ctx.user_data.pop('st',None); TEMP.pop(uid,None)
        if cid:
            ch_upd(cid,emoji_str=txt.strip(),emoji_pos='start')
            await update.message.reply_text(f"✅ Emoji: {txt.strip()}\nPosition choose karo:",reply_markup=KB_AI(cid))
        else: await update.message.reply_text("❌ Error.")

    elif st==ST_BC:
        if uid not in ADMIN_IDS: return
        bt=ctx.user_data.get('bt','all'); all_u=u_all()
        targets=[u for u in all_u if bt=='all' or u_ok(u['uid'])]
        ctx.user_data.pop('st',None); sent=0
        for u in targets:
            try:
                await ctx.bot.send_message(u['uid'],f"📢  {BOT_NAME}\n━━━━━━━━━\n{txt}\n━━━━━━━━━\n{ADMIN_USERNAME}")
                sent+=1; await asyncio.sleep(0.05)
            except: pass
        await update.message.reply_text(f"✅ Broadcast: {sent}/{len(targets)}",reply_markup=KB_ADM())

    elif st==ST_GID:
        if uid not in ADMIN_IDS: return
        try:
            tid=int(txt); act=ctx.user_data.get('sa','give')
            if act in ('revoke','ban','unban'):
                if act=='revoke': u_revoke(tid); msg2="❌ Sub revoked."; ok=f"✅ Revoked: {tid}"
                elif act=='ban':  u_ban(tid,True); await eng_stop(tid); msg2="🚫 Banned."; ok=f"🚫 Banned: {tid}"
                else: u_ban(tid,False); msg2="✅ Unbanned! /start"; ok=f"✅ Unbanned: {tid}"
                try: await ctx.bot.send_message(tid,msg2)
                except: pass
                await update.message.reply_text(ok,reply_markup=KB_ADM()); ctx.user_data.pop('st',None)
            else:
                TEMP[uid]={'tid':tid}; ctx.user_data['st']=ST_GDY
                plans=plan_all()
                pi="\n".join([f"  {p['key']} = {p['label']} ({p['days']}d, {'∞' if p.get('max_channels',0)==0 else p['max_channels']} ch)" for p in plans if p['enabled']])
                await update.message.reply_text(f"User: {tid}\nDays bhejo:\n\nPlans:\n{pi}\n\n(e.g. '30' or '30 m')")
        except: await update.message.reply_text("❌ Valid User ID!")

    elif st==ST_GDY:
        if uid not in ADMIN_IDS: return
        try:
            parts=txt.strip().split(); days=int(parts[0]); pk=parts[1] if len(parts)>1 else None
            tid=TEMP.get(uid,{}).get('tid'); u_give(tid,days)
            if pk:
                p=plan_get(pk)
                if p: u_set_plan(tid,pk)
            try: await ctx.bot.send_message(tid,f"🎉 Subscription!\n\n✅ {days} Days\n⚡ {BOT_NAME} Pro!\n\nSupport: {ADMIN_USERNAME}")
            except: pass
            await update.message.reply_text(f"✅ {days} days → {tid}"+(f"\nPlan: {pk}" if pk else ""),reply_markup=KB_ADM())
        except: await update.message.reply_text("❌ Format: '30' or '30 m'")
        ctx.user_data.pop('st',None); TEMP.pop(uid,None)

    elif st==ST_SRCH:
        if uid not in ADMIN_IDS: return
        ctx.user_data.pop('st',None)
        try:
            tid=int(txt); u=u_get(tid)
            if u:
                chs=ch_all(tid); tf,td,_=l_stats(tid); cur,mx=u_channel_limit(tid)
                await update.message.reply_text(
                    f"👤 Found\n\nName: {u.get('name','?')}\nID: `{u['uid']}`\nSub: {u_sub_str(tid)}\n"
                    f"Channels: {len(chs)} (max: {'∞' if mx==0 else mx})\nFwd: {tf} (Today: {td})\nBanned: {'Yes' if u['is_banned'] else 'No'}",
                    reply_markup=IM([[B("✅ Give","a_give"),B("❌ Revoke","a_revoke")],
                                     [B("🚫 Ban","a_ban"),B("✅ Unban","a_unban")],[B("‹ Back","adm")]]))
            else: await update.message.reply_text("❌ Not found.",reply_markup=KB_ADM())
        except: await update.message.reply_text("❌ Valid User ID!",reply_markup=KB_ADM())

    elif st==ST_TRIAL:
        if uid not in ADMIN_IDS: return
        ctx.user_data.pop('st',None)
        try:
            days=max(0,int(txt)); setting_set('trial_days',days)
            await update.message.reply_text(f"✅ Trial: {days} days",reply_markup=KB([B("💳 Plans","a_plans"),B("‹","adm")]))
        except: await update.message.reply_text("❌ Number!")

    elif st==ST_PLAN_PRICE:
        if uid not in ADMIN_IDS: return
        pk=TEMP.get(uid,{}).get('plan_key')
        try:
            price=max(0,int(txt)); TEMP.setdefault(uid,{})['new_price']=price
            ctx.user_data['st']=ST_PLAN_DAYS; p=plan_get(pk)
            await update.message.reply_text(f"✅ Price: ₹{price}\n\nDays bhejo (current: {p['days']}):\n\n/cancel")
        except: await update.message.reply_text("❌ Valid price!")

    elif st==ST_PLAN_DAYS:
        if uid not in ADMIN_IDS: return
        try:
            days=max(1,int(txt)); TEMP.setdefault(uid,{})['new_days']=days
            ctx.user_data['st']=ST_PLAN_MAXCH; pk=TEMP.get(uid,{}).get('plan_key'); p=plan_get(pk); mc=p.get('max_channels',0)
            await update.message.reply_text(f"✅ Days: {days}\n\nMax Channels:\n(Current: {'∞' if mc==0 else mc})\n0=unlimited\n\n/cancel")
        except: await update.message.reply_text("❌ Valid days!")

    elif st==ST_PLAN_MAXCH:
        if uid not in ADMIN_IDS: return
        pk=TEMP.get(uid,{}).get('plan_key'); np=TEMP.get(uid,{}).get('new_price'); nd=TEMP.get(uid,{}).get('new_days')
        ctx.user_data.pop('st',None); TEMP.pop(uid,None)
        try:
            mc=max(0,int(txt)); plan_upd(pk,price=np,days=nd,max_channels=mc)
            p=plan_get(pk)
            await update.message.reply_text(
                f"✅ Plan Updated!\n\n{p['label']}\n₹{np} | {nd}d | {'∞' if mc==0 else mc} ch",
                reply_markup=KB([B("💳 Plans","a_plans"),B("‹","adm")]))
        except: await update.message.reply_text("❌ Number! (0=unlimited)")

    elif st==ST_PLAN_NEW:
        if uid not in ADMIN_IDS: return
        ctx.user_data.pop('st',None); TEMP.pop(uid,None)
        try:
            parts=txt.split(',')
            if len(parts)<3: raise ValueError
            label=parts[0].strip(); days=max(1,int(parts[1].strip())); price=max(0,int(parts[2].strip()))
            mc=max(0,int(parts[3].strip())) if len(parts)>3 else 0
            import hashlib; key=hashlib.md5(label.encode()).hexdigest()[:4]
            plan_add(key,label,days,price,mc)
            await update.message.reply_text(
                f"✅ Plan Added!\n\n{label}\n₹{price} | {days}d | {'∞' if mc==0 else mc} ch",
                reply_markup=KB([B("💳 Plans","a_plans"),B("‹","adm")]))
        except: await update.message.reply_text("❌ Format: LABEL,DAYS,PRICE,MAX_CH\nExample: 💎 Pro,90,149,5")

# ═══════════════════════════════════════════════════════════════
#   MAIN
# ═══════════════════════════════════════════════════════════════
async def post_init(app):
    db_init()
    try:
        c=DB(); rows=c.execute("SELECT uid,sess FROM sessions").fetchall(); c.close()
        for row in rows:
            if row['sess']: SESSION_CACHE[int(row['uid'])]=row['sess']; KNOWN_USERS.add(int(row['uid']))
        LOG.info(f"✅ {len(SESSION_CACHE)} sessions loaded")
    except Exception as e: LOG.error(f"Session preload:{e}")
    await app.bot.set_my_commands([
        BotCommand("start","Open bot"),BotCommand("menu","Dashboard"),
        BotCommand("admin","Admin panel"),BotCommand("cancel","Cancel"),
    ])
    async def _start():
        await asyncio.sleep(1); await eng_restart_all()
    asyncio.create_task(_start())
    LOG.info(f"✅ {BOT_NAME} {BOT_VERSION} Ready!")

def main():
    from telegram.request import HTTPXRequest
    req=HTTPXRequest(read_timeout=10,write_timeout=10,connect_timeout=10,pool_timeout=3)
    app=(Application.builder().token(BOT_TOKEN).request(req).post_init(post_init).concurrent_updates(True).build())
    app.add_handler(CommandHandler("start",cmd_start))
    app.add_handler(CommandHandler("menu",cmd_menu))
    app.add_handler(CommandHandler("admin",cmd_admin))
    app.add_handler(CommandHandler("cancel",cmd_cancel))
    app.add_handler(CallbackQueryHandler(cbk))
    app.add_handler(MessageHandler(tg_filters.TEXT & ~tg_filters.COMMAND,msg_hdl))
    print(f"\n{'═'*58}")
    print(f"  🚀  {BOT_NAME}  {BOT_VERSION}")
    print(f"  Individual Media Toggles | Para Block/Replace")
    print(f"  Multi-Style | Plan Limits | Clean UI")
    print(f"  Admin: {ADMIN_USERNAME}")
    print(f"{'═'*58}\n")
    app.run_polling(drop_pending_updates=True,allowed_updates=Update.ALL_TYPES)

if __name__=="__main__":
    main()
