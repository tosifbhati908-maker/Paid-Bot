"""
Telegram Board Hosted — Multi-Customer Subscription Service
Railway + PostgreSQL deployment.

Environment variables (all required unless noted):
    API_ID           Telegram API ID
    API_HASH         Telegram API hash
    BOT_TOKEN        BotFather bot token
    SESSION_STRING   Telethon user-session string
    ADMIN_ID         Telegram numeric user ID of the administrator (REQUIRED)
    DATABASE_URL     PostgreSQL connection URL (Railway provides this automatically)
"""

import asyncio
import io
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

import asyncpg
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl import types as tl_types
from telethon.errors import FloodWaitError, ChatWriteForbiddenError
from telegram import (
    Bot, InlineKeyboardButton, InlineKeyboardMarkup, Update,
    InputMediaPhoto,
)
from telegram.ext import (
    Application, CallbackQueryHandler, CommandHandler,
    ContextTypes, MessageHandler, filters,
)

# ─────────────────────────── configuration ────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("board")

API_ID          = int(os.environ["API_ID"])
API_HASH        = os.environ["API_HASH"]
BOT_TOKEN       = os.environ["BOT_TOKEN"]
SESSION_STRING  = os.environ["SESSION_STRING"].strip()
ADMIN_ID        = int(os.environ["ADMIN_ID"])
DATABASE_URL    = os.environ["DATABASE_URL"]

PLANS = {
    "7":  {"label": "7 Days",  "price": 49,  "days": 7},
    "15": {"label": "15 Days", "price": 89,  "days": 15},
    "30": {"label": "30 Days", "price": 149, "days": 30},
}

MAX_GROUPS          = 20
MSG_INTERVAL_SEC    = 1200          # 20 minutes fixed
DAILY_MSG_LIMIT     = 1300          # rolling 24-hour window per customer
FLOOD_SLEEP_CAP     = 300
MIN_SEND_DELAY      = 2             # seconds between individual sends

# ─────────────────────────── database ─────────────────────────────────────────

pool: asyncpg.Pool = None           # initialised in post_init

SCHEMA = """
CREATE TABLE IF NOT EXISTS customers (
    tg_id       BIGINT PRIMARY KEY,
    name        TEXT,
    username    TEXT,
    sub_expires TIMESTAMPTZ,
    blocked     BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS destinations (
    id          SERIAL PRIMARY KEY,
    tg_id       BIGINT NOT NULL REFERENCES customers(tg_id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    link        TEXT NOT NULL,
    UNIQUE(tg_id, link)
);

CREATE TABLE IF NOT EXISTS schedule_config (
    tg_id       BIGINT PRIMARY KEY REFERENCES customers(tg_id) ON DELETE CASCADE,
    message     JSONB,
    enabled     BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS usage_log (
    id          BIGSERIAL PRIMARY KEY,
    tg_id       BIGINT NOT NULL,
    sent_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_usage_tgid_sent ON usage_log (tg_id, sent_at);

CREATE TABLE IF NOT EXISTS payment_requests (
    id          SERIAL PRIMARY KEY,
    tg_id       BIGINT NOT NULL,
    plan        TEXT NOT NULL,
    amount      INT NOT NULL,
    utr         TEXT NOT NULL UNIQUE,
    screenshot_file_id TEXT,
    status      TEXT NOT NULL DEFAULT 'pending',   -- pending | approved | rejected
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS admin_settings (
    key         TEXT PRIMARY KEY,
    value       TEXT
);

INSERT INTO admin_settings (key, value)
    VALUES ('upi_id', 'tosifbhati065@okaxis')
    ON CONFLICT (key) DO NOTHING;
INSERT INTO admin_settings (key, value)
    VALUES ('upi_qr_file_id', '')
    ON CONFLICT (key) DO NOTHING;
INSERT INTO admin_settings (key, value)
    VALUES ('example_video_file_id', '')
    ON CONFLICT (key) DO NOTHING;
"""


async def db_setting(key: str) -> Optional[str]:
    row = await pool.fetchrow(
        "SELECT value FROM admin_settings WHERE key=$1", key
    )
    return row["value"] if row else None


async def db_set_setting(key: str, value: str):
    await pool.execute(
        "INSERT INTO admin_settings (key, value) VALUES ($1,$2) "
        "ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value",
        key, value,
    )


async def get_customer(tg_id: int) -> Optional[asyncpg.Record]:
    return await pool.fetchrow(
        "SELECT * FROM customers WHERE tg_id=$1", tg_id
    )


async def upsert_customer(tg_id: int, name: str, username: str):
    await pool.execute(
        "INSERT INTO customers (tg_id, name, username) VALUES ($1,$2,$3) "
        "ON CONFLICT (tg_id) DO UPDATE SET name=EXCLUDED.name, username=EXCLUDED.username",
        tg_id, name, username or "",
    )


async def rolling_24h_count(tg_id: int) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    row = await pool.fetchrow(
        "SELECT COUNT(*) AS n FROM usage_log WHERE tg_id=$1 AND sent_at>=$2",
        tg_id, cutoff,
    )
    return row["n"] if row else 0


async def log_send(tg_id: int):
    await pool.execute(
        "INSERT INTO usage_log (tg_id) VALUES ($1)", tg_id
    )


async def get_destinations(tg_id: int):
    return await pool.fetch(
        "SELECT * FROM destinations WHERE tg_id=$1 ORDER BY id", tg_id
    )


async def add_destination(tg_id: int, name: str, link: str) -> bool:
    try:
        await pool.execute(
            "INSERT INTO destinations (tg_id, name, link) VALUES ($1,$2,$3)",
            tg_id, name, link,
        )
        return True
    except asyncpg.UniqueViolationError:
        return False


async def remove_destination(tg_id: int, dest_id: int):
    await pool.execute(
        "DELETE FROM destinations WHERE id=$1 AND tg_id=$2", dest_id, tg_id
    )


async def get_schedule(tg_id: int) -> Optional[asyncpg.Record]:
    return await pool.fetchrow(
        "SELECT * FROM schedule_config WHERE tg_id=$1", tg_id
    )


async def save_schedule(tg_id: int, message: dict, enabled: bool):
    await pool.execute(
        "INSERT INTO schedule_config (tg_id, message, enabled) VALUES ($1,$2,$3) "
        "ON CONFLICT (tg_id) DO UPDATE SET message=EXCLUDED.message, enabled=EXCLUDED.enabled",
        tg_id, json.dumps(message), enabled,
    )


async def set_schedule_enabled(tg_id: int, enabled: bool):
    await pool.execute(
        "INSERT INTO schedule_config (tg_id, message, enabled) VALUES ($1,'{}', $2) "
        "ON CONFLICT (tg_id) DO UPDATE SET enabled=EXCLUDED.enabled",
        tg_id, enabled,
    )


async def is_active_subscriber(tg_id: int) -> bool:
    row = await pool.fetchrow(
        "SELECT sub_expires, blocked FROM customers WHERE tg_id=$1", tg_id
    )
    if not row:
        return False
    if row["blocked"]:
        return False
    if not row["sub_expires"]:
        return False
    return row["sub_expires"] > datetime.now(timezone.utc)


# ─────────────────────────── telethon helpers ─────────────────────────────────

telethon_client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)


def _bot_entities_to_telethon(entities):
    result = []
    for e in entities or []:
        t = e.get("type")
        o = int(e.get("offset", 0))
        l = int(e.get("length", 0))
        cls = {
            "mention":      tl_types.MessageEntityMention,
            "hashtag":      tl_types.MessageEntityHashtag,
            "cashtag":      tl_types.MessageEntityCashtag,
            "bot_command":  tl_types.MessageEntityBotCommand,
            "url":          tl_types.MessageEntityUrl,
            "email":        tl_types.MessageEntityEmail,
            "phone_number": tl_types.MessageEntityPhone,
            "bold":         tl_types.MessageEntityBold,
            "italic":       tl_types.MessageEntityItalic,
            "underline":    tl_types.MessageEntityUnderline,
            "strikethrough":tl_types.MessageEntityStrike,
            "spoiler":      tl_types.MessageEntitySpoiler,
            "code":         tl_types.MessageEntityCode,
            "blockquote":   tl_types.MessageEntityBlockquote,
        }.get(t)
        if cls:
            result.append(cls(offset=o, length=l))
        elif t == "pre":
            result.append(tl_types.MessageEntityPre(
                offset=o, length=l, language=e.get("language", "") or ""
            ))
        elif t == "text_link":
            result.append(tl_types.MessageEntityTextUrl(
                offset=o, length=l, url=e.get("url", "") or ""
            ))
        elif t == "text_mention":
            uid = e.get("user_id")
            if uid is not None:
                result.append(tl_types.MessageEntityMentionName(
                    offset=o, length=l, user_id=int(uid)
                ))
        elif t == "custom_emoji":
            emoji_id = e.get("custom_emoji_id")
            if emoji_id is not None:
                result.append(tl_types.MessageEntityCustomEmoji(
                    offset=o, length=l, document_id=int(emoji_id)
                ))
    return result


def _serialize_entities(entities):
    out = []
    for e in entities or []:
        item = {
            "type":   getattr(e, "type", ""),
            "offset": int(getattr(e, "offset", 0)),
            "length": int(getattr(e, "length", 0)),
        }
        for attr in ("url", "language", "custom_emoji_id"):
            v = getattr(e, attr, None)
            if v is not None:
                item[attr] = str(v)
        u = getattr(e, "user", None)
        if u and getattr(u, "id", None):
            item["user_id"] = int(u.id)
        out.append(item)
    return out


async def telethon_send(entity_link: str, message: dict) -> None:
    """Send via the Telethon user account. Raises on failure."""
    entity = await telethon_client.get_entity(entity_link)
    text       = message.get("text", "") or ""
    ents_json  = message.get("entities", [])
    tl_ents    = _bot_entities_to_telethon(ents_json)
    await telethon_client.send_message(
        entity,
        text,
        formatting_entities=tl_ents or None,
        parse_mode=None,
    )


# ─────────────────────────── per-customer scheduler ───────────────────────────

# Maps tg_id -> asyncio.Task
_scheduler_tasks: dict[int, asyncio.Task] = {}


async def _run_customer_scheduler(tg_id: int, bot: Bot):
    """Loop: send to all configured destinations every MSG_INTERVAL_SEC."""
    log.info("Scheduler started for customer %d", tg_id)
    while True:
        try:
            # Re-check subscription and block status every cycle.
            if not await is_active_subscriber(tg_id):
                log.info("Customer %d subscription lapsed; stopping scheduler.", tg_id)
                break

            # Check rolling daily limit.
            used = await rolling_24h_count(tg_id)
            if used >= DAILY_MSG_LIMIT:
                log.info("Customer %d hit daily limit (%d); sleeping 60s.", tg_id, used)
                await asyncio.sleep(60)
                continue

            sch = await get_schedule(tg_id)
            if not sch or not sch["enabled"] or not sch["message"]:
                await asyncio.sleep(30)
                continue

            message   = json.loads(sch["message"]) if isinstance(sch["message"], str) else dict(sch["message"])
            dests     = await get_destinations(tg_id)

            if not dests or not message.get("text"):
                await asyncio.sleep(30)
                continue

            for dest in dests:
                if not await is_active_subscriber(tg_id):
                    break
                used = await rolling_24h_count(tg_id)
                if used >= DAILY_MSG_LIMIT:
                    await bot.send_message(
                        tg_id,
                        "⚠️ Daily limit of 1,300 messages reached. "
                        "Sending will resume once 24 hours have passed from older sends.",
                    )
                    break
                try:
                    await telethon_send(dest["link"], message)
                    await log_send(tg_id)
                except FloodWaitError as e:
                    sleep_for = min(e.seconds, FLOOD_SLEEP_CAP)
                    log.warning("FloodWait %ds for customer %d.", sleep_for, tg_id)
                    await asyncio.sleep(sleep_for)
                except (ChatWriteForbiddenError, Exception) as exc:
                    log.warning("Send error for customer %d dest %s: %s", tg_id, dest["link"], exc)
                await asyncio.sleep(MIN_SEND_DELAY)

            # Wait full interval before next round.
            await asyncio.sleep(MSG_INTERVAL_SEC)

        except asyncio.CancelledError:
            log.info("Scheduler cancelled for customer %d.", tg_id)
            break
        except Exception as exc:
            log.exception("Scheduler error for customer %d: %s", tg_id, exc)
            await asyncio.sleep(30)


def start_customer_scheduler(tg_id: int, bot: Bot):
    stop_customer_scheduler(tg_id)
    task = asyncio.create_task(_run_customer_scheduler(tg_id, bot))
    _scheduler_tasks[tg_id] = task


def stop_customer_scheduler(tg_id: int):
    task = _scheduler_tasks.pop(tg_id, None)
    if task and not task.done():
        task.cancel()


# ─────────────────────────── UI helpers ───────────────────────────────────────

def _ts(dt: Optional[datetime]) -> str:
    if not dt:
        return "—"
    return dt.astimezone(timezone.utc).strftime("%d %b %Y %H:%M UTC")


def main_menu_markup():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💳 Buy Subscription",   callback_data="buy")],
        [InlineKeyboardButton("🔍 Payment Status",     callback_data="pay_status")],
        [InlineKeyboardButton("📊 My Dashboard",       callback_data="dashboard")],
        [InlineKeyboardButton("📋 My Groups",          callback_data="my_groups")],
        [InlineKeyboardButton("📈 My Usage",           callback_data="my_usage")],
        [InlineKeyboardButton("🆘 Help & Support",     callback_data="help")],
        [InlineKeyboardButton("🎥 Video / How to Use", callback_data="howto")],
    ])


def admin_menu_markup():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👥 Customers",          callback_data="adm_customers")],
        [InlineKeyboardButton("⏳ Pending Payments",   callback_data="adm_pending")],
        [InlineKeyboardButton("📜 Payment History",    callback_data="adm_history")],
        [InlineKeyboardButton("⚙️ Settings",           callback_data="adm_settings")],
    ])


def back_to_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Back to menu", callback_data="home")]
    ])


def back_admin():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Back", callback_data="admin")]
    ])


# ─────────────────────────── conversation state ───────────────────────────────

# Minimal in-memory state per user for multi-step flows.
# Keys: tg_id -> dict with "mode" and scratch fields.
_state: dict[int, dict] = {}


def get_state(uid: int) -> dict:
    return _state.setdefault(uid, {})


def clear_state(uid: int):
    _state.pop(uid, None)


# ─────────────────────────── /start ───────────────────────────────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await upsert_customer(user.id, user.full_name, user.username or "")
    clear_state(user.id)

    if user.id == ADMIN_ID:
        await update.message.reply_text(
            "👋 Welcome back, Admin.\n\nChoose an action:",
            reply_markup=admin_menu_markup(),
        )
        return

    sub = await get_customer(user.id)
    blocked = sub and sub["blocked"]
    if blocked:
        await update.message.reply_text("🚫 Your account has been blocked. Contact support.")
        return

    active = await is_active_subscriber(user.id)
    expiry = _ts(sub["sub_expires"]) if sub else "—"
    greeting = (
        f"👋 Welcome, {user.first_name}!\n\n"
        f"{'✅ Subscription active until ' + expiry if active else '❌ No active subscription.'}\n\n"
        "What would you like to do?"
    )
    await update.message.reply_text(greeting, reply_markup=main_menu_markup())


# ─────────────────────────── callback router ──────────────────────────────────

async def callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid  = update.effective_user.id
    data = q.data

    # ── route ──────────────────────────────────────────────────────────────────
    if data == "home":
        await _show_home(update, ctx)
    elif data == "admin":
        if uid != ADMIN_ID:
            return
        await q.edit_message_text("👑 Admin Board", reply_markup=admin_menu_markup())

    # ── customer flows ─────────────────────────────────────────────────────────
    elif data == "buy":
        await _buy_start(update, ctx)
    elif data.startswith("plan:"):
        await _plan_selected(update, ctx, data.split(":")[1])
    elif data.startswith("buy_confirm:"):
        await _buy_confirm(update, ctx, data.split(":")[1])
    elif data == "pay_status":
        await _pay_status(update, ctx)
    elif data == "dashboard":
        await _dashboard(update, ctx)
    elif data == "my_groups":
        await _my_groups(update, ctx)
    elif data.startswith("remove_group:"):
        await _remove_group(update, ctx, int(data.split(":")[1]))
    elif data == "add_group":
        await _add_group_prompt(update, ctx)
    elif data == "my_usage":
        await _my_usage(update, ctx)
    elif data == "help":
        await q.edit_message_text(
            "🆘 *Help & Support*\n\n"
            "For any issues, contact the admin or reply here.\n"
            "Common issues:\n"
            "• Payment not confirmed — wait up to 15 minutes after submission.\n"
            "• Group not posting — make sure the Telegram account has send permission in that group.\n"
            "• Daily limit hit — resumes automatically after 24 hours from oldest send.",
            parse_mode="Markdown",
            reply_markup=back_to_menu(),
        )
    elif data == "howto":
        await _howto(update, ctx)

    # ── scheduler controls ─────────────────────────────────────────────────────
    elif data == "sched_setup":
        await _sched_setup_start(update, ctx)
    elif data == "sched_start":
        await _sched_start(update, ctx)
    elif data == "sched_stop":
        await _sched_stop(update, ctx)
    elif data == "set_message":
        await _set_message_prompt(update, ctx)

    # ── admin flows ─────────────────────────────────────────────────────────────
    elif uid == ADMIN_ID:
        if data == "adm_customers":
            await _adm_customers(update, ctx)
        elif data.startswith("adm_cust:"):
            await _adm_cust_detail(update, ctx, int(data.split(":")[1]))
        elif data.startswith("adm_block:"):
            await _adm_block(update, ctx, int(data.split(":")[1]), True)
        elif data.startswith("adm_unblock:"):
            await _adm_block(update, ctx, int(data.split(":")[1]), False)
        elif data.startswith("adm_extend:"):
            await _adm_extend_prompt(update, ctx, int(data.split(":")[1]))
        elif data == "adm_pending":
            await _adm_pending(update, ctx)
        elif data.startswith("adm_approve:"):
            await _adm_resolve_payment(update, ctx, int(data.split(":")[1]), "approved")
        elif data.startswith("adm_reject:"):
            await _adm_resolve_payment(update, ctx, int(data.split(":")[1]), "rejected")
        elif data == "adm_history":
            await _adm_history(update, ctx)
        elif data == "adm_settings":
            await _adm_settings(update, ctx)
        elif data == "adm_change_upi":
            get_state(uid)["mode"] = "adm_set_upi"
            await q.edit_message_text("🔗 Send the new UPI ID:")
        elif data == "adm_upload_qr":
            get_state(uid)["mode"] = "adm_set_qr"
            await q.edit_message_text("📷 Send the QR code image:")
        elif data == "adm_upload_video":
            get_state(uid)["mode"] = "adm_set_video"
            await q.edit_message_text("🎬 Send the tutorial video:")


async def _show_home(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    clear_state(user.id)
    q = update.callback_query
    if user.id == ADMIN_ID:
        await q.edit_message_text("👑 Admin Board", reply_markup=admin_menu_markup())
        return
    sub = await get_customer(user.id)
    active  = await is_active_subscriber(user.id)
    expiry  = _ts(sub["sub_expires"]) if sub else "—"
    text = (
        f"👋 {user.first_name}\n\n"
        f"{'✅ Active until ' + expiry if active else '❌ No active subscription.'}\n\n"
        "Choose an option:"
    )
    await q.edit_message_text(text, reply_markup=main_menu_markup())


# ─────────────────────────── buy subscription flow ────────────────────────────

async def _buy_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = update.effective_user.id
    if await is_active_subscriber(uid):
        cust = await get_customer(uid)
        await q.edit_message_text(
            f"✅ You already have an active subscription until {_ts(cust['sub_expires'])}.\n\n"
            "You can buy a new plan — it will extend from your current expiry date.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"₹{v['price']} — {v['label']}", callback_data=f"plan:{k}")]
                for k, v in PLANS.items()
            ] + [[InlineKeyboardButton("⬅️ Back", callback_data="home")]]),
        )
        return
    await q.edit_message_text(
        "💳 *Choose a Plan*\n\nAll plans include:\n"
        "• Up to 20 groups\n• Message every 20 minutes\n• Up to 1,300 messages/day",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"₹{v['price']} — {v['label']}", callback_data=f"plan:{k}")]
            for k, v in PLANS.items()
        ] + [[InlineKeyboardButton("⬅️ Back", callback_data="home")]]),
    )


async def _plan_selected(update: Update, ctx: ContextTypes.DEFAULT_TYPE, plan_key: str):
    q = update.callback_query
    uid = update.effective_user.id
    plan = PLANS.get(plan_key)
    if not plan:
        await q.answer("Invalid plan.", show_alert=True)
        return

    get_state(uid)["pending_plan"] = plan_key
    upi_id    = await db_setting("upi_id") or "tosifbhati065@okaxis"
    qr_fid    = await db_setting("upi_qr_file_id") or ""
    video_fid = await db_setting("example_video_file_id") or ""

    text = (
        f"📦 *Plan: {plan['label']} — ₹{plan['price']}*\n\n"
        f"UPI ID: `{upi_id}`\n\n"
        "1. Pay using any UPI app.\n"
        "2. Note the UTR / reference number.\n"
        "3. Tap ✅ Submit Payment below.\n\n"
        "⏳ Manual verification takes up to 15 minutes."
    )
    buttons = [
        [InlineKeyboardButton("✅ Submit Payment", callback_data=f"buy_confirm:{plan_key}")],
        [InlineKeyboardButton("⬅️ Back", callback_data="buy")],
    ]

    # Show QR if available, then optionally send tutorial video separately.
    if qr_fid:
        try:
            await ctx.bot.send_photo(
                uid,
                qr_fid,
                caption=text,
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(buttons),
            )
            await q.message.delete()
        except Exception:
            await q.edit_message_text(text, parse_mode="Markdown",
                                       reply_markup=InlineKeyboardMarkup(buttons))
    else:
        await q.edit_message_text(text, parse_mode="Markdown",
                                   reply_markup=InlineKeyboardMarkup(buttons))

    # Send tutorial video (no subscription required).
    if video_fid:
        try:
            await ctx.bot.send_video(
                uid,
                video_fid,
                caption="🎥 *How to Use — Watch Before Buying*",
                parse_mode="Markdown",
            )
        except Exception:
            pass


async def _buy_confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE, plan_key: str):
    q = update.callback_query
    uid = update.effective_user.id
    plan = PLANS.get(plan_key)
    if not plan:
        return
    st = get_state(uid)
    st["mode"]         = "await_utr"
    st["pending_plan"] = plan_key
    await q.edit_message_text(
        f"✅ *Confirming: {plan['label']} — ₹{plan['price']}*\n\n"
        "Please send your *UTR / Reference Number* (12-digit number from UPI app):",
        parse_mode="Markdown",
    )


# ─────────────────────────── payment status ───────────────────────────────────

async def _pay_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    uid  = update.effective_user.id
    rows = await pool.fetch(
        "SELECT * FROM payment_requests WHERE tg_id=$1 ORDER BY submitted_at DESC LIMIT 5",
        uid,
    )
    if not rows:
        await q.edit_message_text("No payment requests found.", reply_markup=back_to_menu())
        return
    lines = ["📋 *Recent Payment Requests*\n"]
    for r in rows:
        emoji = {"pending": "⏳", "approved": "✅", "rejected": "❌"}.get(r["status"], "?")
        lines.append(
            f"{emoji} {PLANS.get(r['plan'], {}).get('label', r['plan'])} — ₹{r['amount']}\n"
            f"   UTR: `{r['utr']}`\n"
            f"   Submitted: {_ts(r['submitted_at'])}\n"
            f"   Status: *{r['status'].upper()}*\n"
        )
    await q.edit_message_text(
        "\n".join(lines), parse_mode="Markdown", reply_markup=back_to_menu()
    )


# ─────────────────────────── dashboard ───────────────────────────────────────

async def _dashboard(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    uid = update.effective_user.id
    cust  = await get_customer(uid)
    active = await is_active_subscriber(uid)
    used   = await rolling_24h_count(uid)
    dests  = await get_destinations(uid)
    sch    = await get_schedule(uid)
    sched_on = sch and sch["enabled"]

    expiry_line = (
        f"✅ Active — expires {_ts(cust['sub_expires'])}"
        if active else "❌ No active subscription"
    )

    text = (
        f"📊 *Your Dashboard*\n\n"
        f"Subscription: {expiry_line}\n"
        f"Groups added: {len(dests)} / {MAX_GROUPS}\n"
        f"Messages (last 24h): {used} / {DAILY_MSG_LIMIT}\n"
        f"Scheduler: {'▶️ Running' if sched_on else '⏹ Stopped'}\n"
    )

    buttons = [
        [InlineKeyboardButton("📋 My Groups", callback_data="my_groups")],
        [InlineKeyboardButton("✉️ Set Message", callback_data="set_message")],
    ]
    if active:
        if sched_on:
            buttons.append([InlineKeyboardButton("⏹ Stop Scheduler", callback_data="sched_stop")])
        else:
            buttons.append([InlineKeyboardButton("▶️ Start Scheduler", callback_data="sched_start")])
    buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="home")])

    await q.edit_message_text(text, parse_mode="Markdown",
                               reply_markup=InlineKeyboardMarkup(buttons))


# ─────────────────────────── groups ───────────────────────────────────────────

async def _my_groups(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    uid  = update.effective_user.id
    dests = await get_destinations(uid)

    rows = []
    for d in dests:
        rows.append([InlineKeyboardButton(
            f"🗑 {d['name']}", callback_data=f"remove_group:{d['id']}"
        )])

    if len(dests) < MAX_GROUPS:
        rows.append([InlineKeyboardButton("➕ Add Group / Channel", callback_data="add_group")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="dashboard")])

    text = (
        f"📋 *My Groups* ({len(dests)}/{MAX_GROUPS})\n\n"
        + ("\n".join(f"• {d['name']}" for d in dests) or "No groups added yet.")
        + "\n\nTap a name to remove it."
    )
    await q.edit_message_text(text, parse_mode="Markdown",
                               reply_markup=InlineKeyboardMarkup(rows))


async def _remove_group(update: Update, ctx: ContextTypes.DEFAULT_TYPE, dest_id: int):
    uid = update.effective_user.id
    await remove_destination(uid, dest_id)
    await _my_groups(update, ctx)


async def _add_group_prompt(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    uid = update.effective_user.id
    if not await is_active_subscriber(uid):
        await q.answer("Active subscription required.", show_alert=True)
        return
    dests = await get_destinations(uid)
    if len(dests) >= MAX_GROUPS:
        await q.answer(f"Maximum {MAX_GROUPS} groups reached.", show_alert=True)
        return
    get_state(uid)["mode"] = "add_group"
    await q.edit_message_text(
        "🔗 Send the Telegram group or channel link/username now.\n\n"
        "Example: @mygroupname or https://t.me/mygroupname"
    )


# ─────────────────────────── usage ────────────────────────────────────────────

async def _my_usage(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    uid = update.effective_user.id
    used = await rolling_24h_count(uid)
    remaining = max(0, DAILY_MSG_LIMIT - used)
    text = (
        f"📈 *Usage (rolling 24h)*\n\n"
        f"Sent: {used} / {DAILY_MSG_LIMIT}\n"
        f"Remaining: {remaining}\n\n"
        "The 24-hour window is rolling — it does not reset at midnight."
    )
    await q.edit_message_text(text, parse_mode="Markdown", reply_markup=back_to_menu())


# ─────────────────────────── how-to video ────────────────────────────────────

async def _howto(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    uid = update.effective_user.id
    video_fid = await db_setting("example_video_file_id") or ""
    if video_fid:
        try:
            await ctx.bot.send_video(
                uid,
                video_fid,
                caption="🎥 *How to Use Telegram Board*",
                parse_mode="Markdown",
            )
            await q.edit_message_text(
                "Video sent above. ☝️", reply_markup=back_to_menu()
            )
            return
        except Exception:
            pass
    await q.edit_message_text(
        "🎥 No tutorial video has been uploaded yet. Check back soon.",
        reply_markup=back_to_menu(),
    )


# ─────────────────────────── scheduler controls ───────────────────────────────

async def _set_message_prompt(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    uid = update.effective_user.id
    if not await is_active_subscriber(uid):
        await q.answer("Active subscription required.", show_alert=True)
        return
    get_state(uid)["mode"] = "set_message"
    await q.edit_message_text(
        "✉️ Send the message you want to broadcast.\n\n"
        "Formatting (bold, italic, links) will be preserved."
    )


async def _sched_setup_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await _set_message_prompt(update, ctx)


async def _sched_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    uid = update.effective_user.id
    if not await is_active_subscriber(uid):
        await q.answer("Active subscription required.", show_alert=True)
        return
    sch = await get_schedule(uid)
    if not sch or not sch["message"]:
        await q.answer("Set a broadcast message first.", show_alert=True)
        return
    dests = await get_destinations(uid)
    if not dests:
        await q.answer("Add at least one group first.", show_alert=True)
        return
    msg = json.loads(sch["message"]) if isinstance(sch["message"], str) else dict(sch["message"])
    if not msg.get("text"):
        await q.answer("Broadcast message is empty.", show_alert=True)
        return
    await set_schedule_enabled(uid, True)
    start_customer_scheduler(uid, ctx.bot)
    await q.edit_message_text(
        "▶️ Scheduler started! Messages will be sent to all your groups every 20 minutes.",
        reply_markup=back_to_menu(),
    )


async def _sched_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    uid = update.effective_user.id
    await set_schedule_enabled(uid, False)
    stop_customer_scheduler(uid)
    await q.edit_message_text(
        "⏹ Scheduler stopped.", reply_markup=back_to_menu()
    )


# ─────────────────────────── admin: customers ─────────────────────────────────

async def _adm_customers(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    rows = await pool.fetch(
        "SELECT * FROM customers WHERE tg_id != $1 ORDER BY created_at DESC LIMIT 30",
        ADMIN_ID,
    )
    if not rows:
        await q.edit_message_text("No customers yet.", reply_markup=back_admin())
        return
    buttons = []
    for r in rows:
        label = r["name"] or str(r["tg_id"])
        if r["blocked"]:
            label = "🚫 " + label
        elif r["sub_expires"] and r["sub_expires"] > datetime.now(timezone.utc):
            label = "✅ " + label
        else:
            label = "❌ " + label
        buttons.append([InlineKeyboardButton(label, callback_data=f"adm_cust:{r['tg_id']}")])
    buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="admin")])
    await q.edit_message_text(
        f"👥 Customers ({len(rows)})", reply_markup=InlineKeyboardMarkup(buttons)
    )


async def _adm_cust_detail(update: Update, ctx: ContextTypes.DEFAULT_TYPE, cust_id: int):
    q    = update.callback_query
    cust = await get_customer(cust_id)
    if not cust:
        await q.answer("Customer not found.", show_alert=True)
        return
    used  = await rolling_24h_count(cust_id)
    dests = await get_destinations(cust_id)
    text  = (
        f"👤 *{cust['name']}* (@{cust['username'] or '—'})\n"
        f"ID: `{cust_id}`\n"
        f"Status: {'🚫 Blocked' if cust['blocked'] else '✅ Active' if cust['sub_expires'] and cust['sub_expires'] > datetime.now(timezone.utc) else '❌ No sub'}\n"
        f"Expires: {_ts(cust['sub_expires'])}\n"
        f"Groups: {len(dests)}\n"
        f"24h msgs: {used}\n"
        f"Since: {_ts(cust['created_at'])}"
    )
    block_btn = (
        InlineKeyboardButton("✅ Unblock", callback_data=f"adm_unblock:{cust_id}")
        if cust["blocked"]
        else InlineKeyboardButton("🚫 Block", callback_data=f"adm_block:{cust_id}")
    )
    await q.edit_message_text(
        text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [block_btn],
            [InlineKeyboardButton("📅 Extend Subscription", callback_data=f"adm_extend:{cust_id}")],
            [InlineKeyboardButton("⬅️ Back", callback_data="adm_customers")],
        ]),
    )


async def _adm_block(update: Update, ctx: ContextTypes.DEFAULT_TYPE, cust_id: int, block: bool):
    q = update.callback_query
    await pool.execute(
        "UPDATE customers SET blocked=$1 WHERE tg_id=$2", block, cust_id
    )
    if block:
        stop_customer_scheduler(cust_id)
        try:
            await ctx.bot.send_message(cust_id, "🚫 Your account has been blocked by the administrator.")
        except Exception:
            pass
    else:
        try:
            await ctx.bot.send_message(cust_id, "✅ Your account has been unblocked.")
        except Exception:
            pass
    await q.answer(f"{'Blocked' if block else 'Unblocked'}.", show_alert=True)
    await _adm_cust_detail(update, ctx, cust_id)


async def _adm_extend_prompt(update: Update, ctx: ContextTypes.DEFAULT_TYPE, cust_id: int):
    q = update.callback_query
    st = get_state(ADMIN_ID)
    st["mode"]    = "adm_extend"
    st["ext_cid"] = cust_id
    await q.edit_message_text(
        f"📅 Extending subscription for `{cust_id}`.\n\n"
        "Send number of *days* to add:",
        parse_mode="Markdown",
    )


# ─────────────────────────── admin: payments ──────────────────────────────────

async def _adm_pending(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    rows = await pool.fetch(
        "SELECT * FROM payment_requests WHERE status='pending' ORDER BY submitted_at ASC"
    )
    if not rows:
        await q.edit_message_text("No pending payments. ✅", reply_markup=back_admin())
        return
    for r in rows:
        cust = await get_customer(r["tg_id"])
        name = cust["name"] if cust else str(r["tg_id"])
        text = (
            f"⏳ *Pending Payment #{r['id']}*\n\n"
            f"Customer: {name} (`{r['tg_id']}`)\n"
            f"Plan: {PLANS.get(r['plan'], {}).get('label', r['plan'])} — ₹{r['amount']}\n"
            f"UTR: `{r['utr']}`\n"
            f"Submitted: {_ts(r['submitted_at'])}"
        )
        buttons = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Approve", callback_data=f"adm_approve:{r['id']}"),
                InlineKeyboardButton("❌ Reject",  callback_data=f"adm_reject:{r['id']}"),
            ]
        ])
        if r["screenshot_file_id"]:
            try:
                await ctx.bot.send_photo(ADMIN_ID, r["screenshot_file_id"],
                                          caption=text, parse_mode="Markdown",
                                          reply_markup=buttons)
            except Exception:
                await ctx.bot.send_message(ADMIN_ID, text, parse_mode="Markdown",
                                            reply_markup=buttons)
        else:
            await ctx.bot.send_message(ADMIN_ID, text, parse_mode="Markdown",
                                        reply_markup=buttons)
    await q.edit_message_text("Pending payments listed above. ☝️", reply_markup=back_admin())


async def _adm_resolve_payment(
    update: Update, ctx: ContextTypes.DEFAULT_TYPE, req_id: int, decision: str
):
    q   = update.callback_query
    row = await pool.fetchrow(
        "SELECT * FROM payment_requests WHERE id=$1", req_id
    )
    if not row or row["status"] != "pending":
        await q.answer("Already processed.", show_alert=True)
        return

    await pool.execute(
        "UPDATE payment_requests SET status=$1, resolved_at=$2 WHERE id=$3",
        decision, datetime.now(timezone.utc), req_id,
    )

    cust_id = row["tg_id"]
    plan    = PLANS.get(row["plan"], {})

    if decision == "approved":
        days = plan.get("days", 0)
        cust = await get_customer(cust_id)
        base = (
            cust["sub_expires"]
            if cust and cust["sub_expires"] and cust["sub_expires"] > datetime.now(timezone.utc)
            else datetime.now(timezone.utc)
        )
        new_expiry = base + timedelta(days=days)
        await pool.execute(
            "UPDATE customers SET sub_expires=$1 WHERE tg_id=$2",
            new_expiry, cust_id,
        )
        msg = (
            f"✅ *Payment Approved!*\n\n"
            f"Plan: {plan.get('label', row['plan'])}\n"
            f"Active until: {_ts(new_expiry)}\n\n"
            "Your scheduler is ready. Open the bot to get started!"
        )
    else:
        msg = (
            "❌ *Payment Rejected*\n\n"
            "Your payment could not be verified. "
            "Please check the UTR number and contact support if you believe this is an error."
        )

    try:
        await ctx.bot.send_message(cust_id, msg, parse_mode="Markdown")
    except Exception:
        pass

    await q.edit_message_text(
        f"{'✅ Approved' if decision == 'approved' else '❌ Rejected'} payment #{req_id}.",
        reply_markup=back_admin(),
    )


async def _adm_history(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    rows = await pool.fetch(
        "SELECT pr.*, c.name FROM payment_requests pr "
        "LEFT JOIN customers c ON c.tg_id = pr.tg_id "
        "ORDER BY pr.submitted_at DESC LIMIT 20"
    )
    if not rows:
        await q.edit_message_text("No payment history.", reply_markup=back_admin())
        return
    lines = ["📜 *Payment History (latest 20)*\n"]
    for r in rows:
        emoji = {"pending": "⏳", "approved": "✅", "rejected": "❌"}.get(r["status"], "?")
        lines.append(
            f"{emoji} #{r['id']} {r['name'] or r['tg_id']} — "
            f"₹{r['amount']} ({PLANS.get(r['plan'],{}).get('label','?')}) "
            f"{_ts(r['submitted_at'])}"
        )
    await q.edit_message_text(
        "\n".join(lines), parse_mode="Markdown", reply_markup=back_admin()
    )


# ─────────────────────────── admin: settings ──────────────────────────────────

async def _adm_settings(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q      = update.callback_query
    upi_id = await db_setting("upi_id") or "—"
    qr     = await db_setting("upi_qr_file_id") or ""
    vid    = await db_setting("example_video_file_id") or ""
    text   = (
        f"⚙️ *Admin Settings*\n\n"
        f"UPI ID: `{upi_id}`\n"
        f"QR Code: {'✅ Uploaded' if qr else '❌ None'}\n"
        f"Tutorial Video: {'✅ Uploaded' if vid else '❌ None'}"
    )
    await q.edit_message_text(
        text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ Change UPI ID",      callback_data="adm_change_upi")],
            [InlineKeyboardButton("📷 Upload/Replace QR",  callback_data="adm_upload_qr")],
            [InlineKeyboardButton("🎬 Upload/Replace Video", callback_data="adm_upload_video")],
            [InlineKeyboardButton("⬅️ Back",               callback_data="admin")],
        ]),
    )


# ─────────────────────────── message handler ──────────────────────────────────

async def text_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    uid  = user.id
    st   = get_state(uid)
    mode = st.get("mode", "")
    msg  = update.message

    # ── UTR submission ─────────────────────────────────────────────────────────
    if mode == "await_utr":
        utr = (msg.text or "").strip()
        if not re.fullmatch(r"\d{10,25}", utr):
            await msg.reply_text(
                "❌ That doesn't look like a valid UTR. "
                "Please enter the 10-25 digit reference number from your UPI app."
            )
            return
        # Check for duplicate UTR.
        existing = await pool.fetchrow(
            "SELECT id FROM payment_requests WHERE utr=$1", utr
        )
        if existing:
            await msg.reply_text(
                "❌ This UTR has already been submitted. "
                "Contact support if you believe this is an error."
            )
            clear_state(uid)
            return
        st["utr"] = utr
        st["mode"] = "await_screenshot"
        await msg.reply_text(
            f"✅ UTR recorded: `{utr}`\n\n"
            "Now send a *screenshot* of the payment confirmation:",
            parse_mode="Markdown",
        )
        return

    # ── set broadcast message ──────────────────────────────────────────────────
    if mode == "set_message":
        payload = {
            "text":     msg.text or "",
            "entities": _serialize_entities(msg.entities),
        }
        plan_key = st.get("pending_plan")
        sch = await get_schedule(uid)
        enabled = sch["enabled"] if sch else False
        await save_schedule(uid, payload, enabled)
        clear_state(uid)
        await msg.reply_text(
            "✅ Broadcast message saved!\n\nGo to *My Dashboard* to start the scheduler.",
            parse_mode="Markdown",
            reply_markup=main_menu_markup(),
        )
        return

    # ── add group ─────────────────────────────────────────────────────────────
    if mode == "add_group":
        link = (msg.text or "").strip()
        dests = await get_destinations(uid)
        if len(dests) >= MAX_GROUPS:
            await msg.reply_text(f"❌ Maximum {MAX_GROUPS} groups reached.", reply_markup=main_menu_markup())
            clear_state(uid)
            return
        try:
            entity = await telethon_client.get_entity(link)
            name   = getattr(entity, "title", link)
            added  = await add_destination(uid, name, link)
            if added:
                await msg.reply_text(f"✅ Added: *{name}*", parse_mode="Markdown",
                                      reply_markup=main_menu_markup())
            else:
                await msg.reply_text("⚠️ Already in your list.", reply_markup=main_menu_markup())
        except Exception as exc:
            await msg.reply_text(
                f"❌ Could not access that group: {type(exc).__name__}\n"
                "Make sure the Telegram account has access to it.",
                reply_markup=main_menu_markup(),
            )
        clear_state(uid)
        return

    # ── admin: change UPI ID ───────────────────────────────────────────────────
    if uid == ADMIN_ID and mode == "adm_set_upi":
        new_upi = (msg.text or "").strip()
        await db_set_setting("upi_id", new_upi)
        clear_state(uid)
        await msg.reply_text(f"✅ UPI ID updated to `{new_upi}`", parse_mode="Markdown",
                              reply_markup=admin_menu_markup())
        return

    # ── admin: extend subscription ────────────────────────────────────────────
    if uid == ADMIN_ID and mode == "adm_extend":
        cust_id = st.get("ext_cid")
        try:
            days = int((msg.text or "").strip())
            if days <= 0:
                raise ValueError
        except ValueError:
            await msg.reply_text("❌ Send a positive whole number of days.")
            return
        cust = await get_customer(cust_id)
        base = (
            cust["sub_expires"]
            if cust and cust["sub_expires"] and cust["sub_expires"] > datetime.now(timezone.utc)
            else datetime.now(timezone.utc)
        )
        new_exp = base + timedelta(days=days)
        await pool.execute(
            "UPDATE customers SET sub_expires=$1 WHERE tg_id=$2", new_exp, cust_id
        )
        clear_state(uid)
        try:
            await ctx.bot.send_message(
                cust_id,
                f"📅 Your subscription has been extended by *{days} day(s)*.\n"
                f"New expiry: {_ts(new_exp)}",
                parse_mode="Markdown",
            )
        except Exception:
            pass
        await msg.reply_text(
            f"✅ Extended {cust_id} by {days} day(s). New expiry: {_ts(new_exp)}",
            reply_markup=admin_menu_markup(),
        )
        return


async def photo_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    uid  = user.id
    st   = get_state(uid)
    mode = st.get("mode", "")
    msg  = update.message
    photo = msg.photo[-1] if msg.photo else None

    # ── payment screenshot ────────────────────────────────────────────────────
    if mode == "await_screenshot":
        plan_key = st.get("pending_plan")
        utr      = st.get("utr")
        plan     = PLANS.get(plan_key)
        if not plan or not utr:
            clear_state(uid)
            await msg.reply_text("❌ Session expired. Start again.", reply_markup=main_menu_markup())
            return

        fid = photo.file_id if photo else None
        await pool.execute(
            "INSERT INTO payment_requests (tg_id, plan, amount, utr, screenshot_file_id) "
            "VALUES ($1,$2,$3,$4,$5)",
            uid, plan_key, plan["price"], utr, fid,
        )
        clear_state(uid)

        # Notify customer.
        await msg.reply_text(
            "✅ *Payment submitted!*\n\n"
            "⏳ Verification takes up to 15 minutes.\n"
            "You'll be notified once confirmed.",
            parse_mode="Markdown",
            reply_markup=main_menu_markup(),
        )

        # Notify admin.
        cust = await get_customer(uid)
        notif = (
            f"💰 *New Payment Request*\n\n"
            f"From: {cust['name']} (@{cust['username'] or '—'}) `{uid}`\n"
            f"Plan: {plan['label']} — ₹{plan['price']}\n"
            f"UTR: `{utr}`\n"
            f"Time: {_ts(datetime.now(timezone.utc))}"
        )
        req_row = await pool.fetchrow(
            "SELECT id FROM payment_requests WHERE utr=$1", utr
        )
        req_id  = req_row["id"] if req_row else 0
        buttons = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Approve", callback_data=f"adm_approve:{req_id}"),
                InlineKeyboardButton("❌ Reject",  callback_data=f"adm_reject:{req_id}"),
            ]
        ])
        if fid:
            await ctx.bot.send_photo(ADMIN_ID, fid, caption=notif,
                                      parse_mode="Markdown", reply_markup=buttons)
        else:
            await ctx.bot.send_message(ADMIN_ID, notif,
                                        parse_mode="Markdown", reply_markup=buttons)
        return

    # ── admin: upload QR ──────────────────────────────────────────────────────
    if uid == ADMIN_ID and mode == "adm_set_qr":
        if photo:
            await db_set_setting("upi_qr_file_id", photo.file_id)
            clear_state(uid)
            await msg.reply_text("✅ QR code updated.", reply_markup=admin_menu_markup())
        return


async def video_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    st   = get_state(uid)
    mode = st.get("mode", "")
    msg  = update.message

    if uid == ADMIN_ID and mode == "adm_set_video":
        video = msg.video or msg.document
        if video:
            await db_set_setting("example_video_file_id", video.file_id)
            clear_state(uid)
            await msg.reply_text("✅ Tutorial video updated.", reply_markup=admin_menu_markup())


# ─────────────────────────── boot ─────────────────────────────────────────────

app_global: Application = None


async def post_init(app: Application):
    global app_global, pool
    app_global = app

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    await pool.execute(SCHEMA)
    log.info("Database ready.")

    await telethon_client.connect()
    if not await telethon_client.is_user_authorized():
        raise RuntimeError(
            "Telethon session not authorized. Re-run make_session.py and update SESSION_STRING."
        )
    log.info("Telethon connected.")

    # Restart schedulers for customers who had them running.
    rows = await pool.fetch(
        "SELECT tg_id FROM schedule_config WHERE enabled=TRUE"
    )
    for r in rows:
        cid = r["tg_id"]
        if await is_active_subscriber(cid):
            start_customer_scheduler(cid, app.bot)
            log.info("Resumed scheduler for customer %d.", cid)


async def post_shutdown(app: Application):
    for task in list(_scheduler_tasks.values()):
        task.cancel()
    await asyncio.gather(*_scheduler_tasks.values(), return_exceptions=True)
    _scheduler_tasks.clear()
    await telethon_client.disconnect()
    if pool:
        await pool.close()


application = (
    Application.builder()
    .token(BOT_TOKEN)
    .post_init(post_init)
    .post_shutdown(post_shutdown)
    .build()
)

application.add_handler(CommandHandler("start", cmd_start))
application.add_handler(CallbackQueryHandler(callback))
application.add_handler(MessageHandler(
    filters.PHOTO & ~filters.COMMAND, photo_handler
))
application.add_handler(MessageHandler(
    (filters.VIDEO | filters.Document.VIDEO) & ~filters.COMMAND, video_handler
))
application.add_handler(MessageHandler(
    filters.TEXT & ~filters.COMMAND, text_handler
))

if __name__ == "__main__":
    log.info("Telegram Board starting…")
    application.run_polling(allowed_updates=Update.ALL_TYPES)
