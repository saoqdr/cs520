import os
import time
import json
import re
import asyncio
import threading
import traceback
import functools
import copy
import requests
import sqlite3
import io
from pathlib import Path
import html
from datetime import datetime, timezone, timedelta
import uuid
import ipaddress
from concurrent.futures import TimeoutError as FuturesTimeoutError

from telethon import TelegramClient, events, utils
from telethon.tl.types import (
    User, Channel, PeerChannel, PeerChat, Chat, Dialog, UserStatusOffline, UserStatusOnline,
    BusinessWorkHours, BusinessLocation, GeoPoint
)
from telethon.errors.rpcerrorlist import (
    FloodWaitError, ApiIdInvalidError, AuthKeyDuplicatedError, PeerIdInvalidError,
    ChannelPrivateError, TimeoutError as TelethonTimeoutError, UserNotParticipantError,
    UsernameInvalidError, InviteHashExpiredError, ChannelsTooMuchError, ChatAdminRequiredError,
    UserAlreadyParticipantError
)
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest
from telethon.tl.functions.messages import GetFullChatRequest

import telebot
from telebot import types
from telebot.apihelper import ApiTelegramException
from telebot.types import ReplyParameters

# ---------------------- 新增：赞助功能所需的库 ----------------------
import hashlib
import urllib.parse
from flask import Flask, request, jsonify
import logging
# ---------------------- 新增结束 ----------------------

# ---------------------- 配置参数 ----------------------
CHINA_TZ = timezone(timedelta(hours=8))
CONFIG = {
    "api_id": 25119018,
    "api_hash": 'd425a243e40ef87ef991efee8f1e5f87',
    "session_name": 'session_name',
    "BOT_TOKEN": '7194606928:AAEKMcVIsiUdcrU8GrtbJNuUbOdb1BsDvaw',
    "ADMIN_ID": 5170745913,
    "LOG_CHANNEL_ID": -1002780775598,
    "REQUIRED_CHANNEL": "@KINGHEN888",
    "CHANNELS_FILE": "channels.json",
    "REPORTS_FILE": "reports.json",
    "DATABASE_FILE": "history.db",
    "ONLINE_THRESHOLD": 300,
    "TELETHON_TIMEOUT": 45,
    "TELEBOT_API_TIMEOUT": 40,
    "TELEBOT_POLLING_TIMEOUT": 30,
    "LOG_BATCH_INTERVAL": 15,
    "LOG_MAX_MESSAGE_LENGTH": 4000,
    "PROFILE_HISTORY_PAGE_SIZE": 15,
    "GROUP_HISTORY_PAGE_SIZE": 10,
    "SCAM_CHANNEL_SEARCH_LIMIT": 5,
    "SCAM_CHANNEL_SEARCH_TIMEOUT": 40,
    "COMMON_GROUPS_TIMEOUT": 90,
    "BUSINESS_SCAN_COOLDOWN": 60,
    "OFFLINE_REPLY_COOLDOWN": 10,
    # ---------------------- 新增：OKPay 赞助功能配置 ----------------------
    "OKPAY_ID": 21536,  # 请替换为你的 OKPay 商户 APP ID
    "OKPAY_TOKEN": "98V6feDUqgvxBCszGHIKNObSYL24Jw7n",  # 请替换为你的 OKPay 商户 Token
    "SERVER_PUBLIC_IP": "38.22.90.236",  # 请替换为你的服务器公网 IP
    "WEBHOOK_PORT": 1010,  # 用于接收支付回调的端口
    "WEBAPP_CONFIG_FILE": "webapp_config.json",
    # ---------------------- 新增结束 ----------------------
}
BOT_VERSION = "v24.8.17.5 | Sponsorship Update"
DONE_SUBMISSION_COMMAND = "/done"

# 动态生成回调 URL
CONFIG["CALLBACK_URL"] = f'http://{CONFIG["SERVER_PUBLIC_IP"]}:{CONFIG["WEBHOOK_PORT"]}/okpay'
CONFIG["WEBAPP_URL"] = f'http://{CONFIG["SERVER_PUBLIC_IP"]}:{CONFIG["WEBHOOK_PORT"]}/webapp'

BASE_DIR = Path(__file__).resolve().parent


def resolve_project_path(candidate: str) -> Path:
    """Resolve a potentially relative path against the project directory."""

    path = Path(candidate).expanduser()
    if not path.is_absolute():
        path = (BASE_DIR / path).resolve()
    return path


def ensure_parent_dir(path: Path):
    """Make sure the parent directory of *path* exists."""

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        # Directory creation best-effort; permission issues will surface on file write.
        pass


WEBAPP_CONFIG_PATH = resolve_project_path(CONFIG.get("WEBAPP_CONFIG_FILE", "webapp_config.json"))
_webapp_url_loaded = False
_webapp_url_override = None
_webapp_url_explicit = False

for _cfg_key in ("CHANNELS_FILE", "REPORTS_FILE", "DATABASE_FILE"):
    if CONFIG.get(_cfg_key):
        CONFIG[_cfg_key] = resolve_project_path(CONFIG[_cfg_key])


# Manually define all content types for compatibility with any py-telegram-bot-api version
ALL_CONTENT_TYPES = [
    'text', 'audio', 'document', 'animation', 'game', 'photo', 'sticker', 'video', 'video_note',
    'voice', 'contact', 'location', 'venue', 'dice', 'new_chat_members', 'left_chat_member',
    'new_chat_title', 'new_chat_photo', 'delete_chat_photo', 'group_chat_created',
    'supergroup_chat_created', 'channel_chat_created', 'migrate_to_chat_id',
    'migrate_from_chat_id', 'pinned_message', 'invoice', 'successful_payment', 'connected_website',
    'poll', 'passport_data', 'proximity_alert_triggered', 'video_chat_scheduled', 'video_chat_started',
    'video_chat_ended', 'video_chat_participants_invited', 'web_app_data', 'message_auto_delete_timer_changed',
    'forum_topic_created', 'forum_topic_edited', 'forum_topic_closed', 'forum_topic_reopened',
    'general_forum_topic_hidden', 'general_forum_topic_unhidden', 'write_access_allowed',
    'user_shared', 'chat_shared', 'story'
]


# ---------------------- 并发控制、状态与日志缓冲 ----------------------
channels_lock = threading.Lock()
reports_lock = threading.Lock()
db_lock = threading.Lock()
log_buffer_lock = threading.Lock()
log_buffer = []

# (user_id, contact_id): timestamp
offline_reply_cooldown_cache = {}
# user_id: {'flow': '...', 'data': {...}, 'messages': []}
user_settings_state = {}
# ---------------------- 新增：赞助流程状态 ----------------------
user_sponsorship_state = {}


# ---------------------- 新增：Flask 和 OKPay 支付逻辑 ----------------------
app = Flask(__name__)
logger = logging.getLogger('ZPF-Bot') # 使用已有的 logger

class OkayPay:
    def __init__(self, id, token, api_url_base='https://api.okaypay.me/shop/'):
        self.id = id
        self.token = token
        self.api_url_payLink = api_url_base + 'payLink'
        self.api_url_transfer = api_url_base + 'transfer'
        self.api_url_TransactionHistory = api_url_base + 'TransactionHistory'

    def pay_link(self, amount, return_url=None):
        payment_data = {
            'name': '机器人赞助',
            'amount': amount,
            'coin': 'USDT',
            'return_url': return_url or CONFIG["CALLBACK_URL"]
        }
        signed_data = self._sign(payment_data)
        return self._post(self.api_url_payLink, signed_data)

    def _sign(self, data):
        data['id'] = self.id
        filtered_data = {k: v for k, v in data.items() if v is not None and v != ''}
        sorted_data = sorted(filtered_data.items())
        query_str = urllib.parse.urlencode(sorted_data, quote_via=urllib.parse.quote)
        decoded_str = urllib.parse.unquote(query_str)
        sign_str = decoded_str + '&token=' + self.token
        signature = hashlib.md5(sign_str.encode('utf-8')).hexdigest().upper()
        signed_data = dict(sorted_data)
        signed_data['sign'] = signature
        return signed_data

    def _post(self, url, data):
        try:
            response = requests.post(
                url,
                data=data,
                headers={'User-Agent': 'HTTP CLIENT'},
                timeout=10,
                verify=True # 在生产环境中建议开启 SSL 验证
            )
            logger.info(f"OKPay API请求: URL={url}, 数据={data}, 响应={response.text}")
            return response.json()
        except Exception as e:
            logger.error(f"OKPay API请求错误: {e}")
            return {'error': str(e), 'status': 'request_failed'}

# 实例化 OKPay 客户端
okpay_client = OkayPay(id=CONFIG["OKPAY_ID"], token=CONFIG["OKPAY_TOKEN"])

@app.route('/okpay', methods=['POST'])
def handle_okpay_callback():
    try:
        callback_data = request.get_json()
        logger.info(f"收到OKPay回调: {callback_data}")

        if 'data' in callback_data and isinstance(callback_data['data'], dict):
            payment_info = callback_data['data']
        else:
            payment_info = callback_data

        order_id = payment_info.get('order_id')
        amount_str = payment_info.get('amount')
        coin = payment_info.get('coin')

        if not all([order_id, amount_str, coin]):
            logger.warning(f"从回调中未能解析出完整订单信息: {callback_data}")
            return jsonify({'status': 'success'}), 200
        
        amount = float(amount_str)
        now = int(time.time())

        with db_lock:
            conn = get_db_connection()
            try:
                c = conn.cursor()
                # 检查订单状态
                c.execute("SELECT user_id, status FROM okpay_orders WHERE order_id = ?", (order_id,))
                order = c.fetchone()

                if not order or order['status'] == 'paid':
                    status = "已支付" if (order and order['status'] == 'paid') else "不存在"
                    logger.warning(f"订单 {order_id} 状态为“{status}”，忽略回调。")
                    return jsonify({'status': 'success'}), 200
                
                user_id = order['user_id']
                
                # 使用事务一次性完成所有数据库写入
                c.execute("BEGIN TRANSACTION;")
                c.execute("UPDATE okpay_orders SET status = 'paid' WHERE order_id = ?", (order_id,))
                c.execute(
                    "INSERT INTO sponsorships (user_id, amount_usdt, order_id, timestamp) VALUES (?, ?, ?, ?)",
                    (user_id, amount, order_id, now)
                )
                c.execute(
                    """
                    INSERT INTO sponsors (user_id, total_amount_usdt, last_sponsored_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(user_id) DO UPDATE SET
                        total_amount_usdt = total_amount_usdt + excluded.total_amount_usdt,
                        last_sponsored_at = excluded.last_sponsored_at
                    """,
                    (user_id, amount, now)
                )
                c.execute("COMMIT;")
                conn.commit()

            except sqlite3.Error as e:
                conn.rollback() # 如果出错则回滚
                logger.error(f"数据库操作失败: {e}")
                raise e
            finally:
                if conn:
                    conn.close()

        # 数据库操作完成后，再发送通知
        try:
            bot.send_message(
                user_id,
                f"✅ *{escape_markdown('赞助成功！')}*\n\n"
                f"{escape_markdown('非常感谢您的支持，您的每一份赞助都是我们前进的动力！')}\n\n"
                f"*{escape_markdown('订单号:')}* `{escape_for_code(order_id)}`\n"
                f"*{escape_markdown('金额:')}* `{escape_for_code(str(amount))} {escape_for_code(coin)}`",
                parse_mode="MarkdownV2"
            )
            logger.info(f"已通知用户 {user_id} 订单 {order_id} 支付成功")
        except Exception as e:
            logger.error(f"通知用户 {user_id} 赞助成功失败: {e}")

        return jsonify({'status': 'success'}), 200

    except Exception as e:
        logger.exception(f"回调处理异常: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT user_id, amount, status FROM okpay_orders WHERE order_id = ?", (order_id,))
            order = c.fetchone()

            if not order:
                logger.warning(f"订单不存在或已处理: {order_id}")
                conn.close()
                return jsonify({'status': 'success'}), 200 # 告诉 OKPay 我们已收到，防止重试

            if order['status'] == 'paid':
                logger.info(f"订单 {order_id} 已是支付状态，忽略重复回调。")
                conn.close()
                return jsonify({'status': 'success'}), 200

            user_id = order['user_id']
            now = int(time.time())
            
            # 1. 更新订单状态
            c.execute("UPDATE okpay_orders SET status = 'paid' WHERE order_id = ?", (order_id,))
            
            # 2. 记录赞助详情
            c.execute(
                "INSERT INTO sponsorships (user_id, amount_usdt, order_id, timestamp) VALUES (?, ?, ?, ?)",
                (user_id, float(amount), order_id, now)
            )
            
            # 3. 更新赞助者总金额
            c.execute(
                """
                INSERT INTO sponsors (user_id, total_amount_usdt, last_sponsored_at)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    total_amount_usdt = total_amount_usdt + excluded.total_amount_usdt,
                    last_sponsored_at = excluded.last_sponsored_at
                """,
                (user_id, float(amount), now)
            )
            conn.commit()
            conn.close()

        # 4. 通知用户
        try:
            bot.send_message(
                user_id, 
                f"✅ *{escape_markdown('赞助成功！')}*\n\n"
                f"{escape_markdown('非常感谢您的支持，您的每一份赞助都是我们前进的动力！')}\n\n"
                f"*{escape_markdown('订单号:')}* `{escape_for_code(order_id)}`\n"
                f"*{escape_markdown('金额:')}* `{escape_for_code(str(amount))} {escape_for_code(coin)}`",
                parse_mode="MarkdownV2"
            )
            logger.info(f"已通知用户 {user_id} 订单 {order_id} 支付成功")
        except Exception as e:
            logger.error(f"通知用户 {user_id} 赞助成功失败: {e}")
        
        return jsonify({'status': 'success'}), 200
    
    except Exception as e:
        logger.exception(f"回调处理异常: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

def run_server():
    logger.info(f"回调服务器正在启动，监听 0.0.0.0:{CONFIG['WEBHOOK_PORT']}...")
    try:
        log = logging.getLogger('werkzeug')
        log.setLevel(logging.ERROR) # 屏蔽 Flask 的常规日志输出
        app.run(host='0.0.0.0', port=CONFIG["WEBHOOK_PORT"], debug=False, use_reloader=False)
    except Exception as e:
        logger.exception("回调服务器运行错误")
# ---------------------- 新增结束 ----------------------


# ---------------------- 文本处理与序列化函数 ----------------------
def escape_markdown(text: str) -> str:
    """Escapes characters for general MarkdownV2 text."""
    if not isinstance(text, str):
        text = str(text)
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

def escape_for_code(text: str) -> str:
    """Escapes characters for use inside a MarkdownV2 code block ('`' and '\')."""
    if not isinstance(text, str):
        text = str(text)
    return text.replace('\\', '\\\\').replace('`', '\\`')


def format_inline_code(text: str) -> str:
    """Wrap the provided text in MarkdownV2 inline code fencing."""
    return f"`{escape_for_code(text)}`"


def get_configured_webapp_url() -> str:
    """Return the operator-configured WebApp URL, reading overrides when available."""
    global _webapp_url_loaded, _webapp_url_override, _webapp_url_explicit

    if not _webapp_url_loaded:
        _webapp_url_loaded = True
        _webapp_url_override = None
        _webapp_url_explicit = False
        try:
            if WEBAPP_CONFIG_PATH.exists():
                data = json.loads(WEBAPP_CONFIG_PATH.read_text(encoding='utf-8'))
                stored = data.get('webapp_url')
                if stored is None:
                    stored = ''
                if isinstance(stored, str):
                    _webapp_url_override = stored.strip()
                    _webapp_url_explicit = True
        except Exception as exc:
            print(f"⚠️ 无法读取 WebApp 配置文件 {WEBAPP_CONFIG_PATH}: {exc}")

    base_url = (CONFIG.get("WEBAPP_URL") or "").strip()
    if _webapp_url_explicit:
        return _webapp_url_override or ""
    return base_url


def set_configured_webapp_url(raw_value: str):
    """Update the persisted WebApp URL configuration.

    Returns a dictionary describing the result with keys:
        success (bool)
        state   ('custom' | 'disabled' | 'default')
        url     (str | None)
        error   (str) present only when success is False
    """

    global _webapp_url_loaded, _webapp_url_override, _webapp_url_explicit

    target_url, hint = normalize_webapp_url_input(raw_value)

    if target_url not in {None, ""} and not is_valid_url(target_url):
        return {
            'success': False,
            'error': 'invalid_url',
            'url': target_url
        }

    try:
        if target_url is None:
            if WEBAPP_CONFIG_PATH.exists():
                WEBAPP_CONFIG_PATH.unlink()
            _webapp_url_override = None
            _webapp_url_explicit = False
            state = 'default'
        else:
            payload = {'webapp_url': target_url}
            ensure_parent_dir(WEBAPP_CONFIG_PATH)
            WEBAPP_CONFIG_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
            _webapp_url_override = target_url.strip()
            _webapp_url_explicit = True
            state = 'disabled' if not _webapp_url_override else 'custom'

        _webapp_url_loaded = True
        return {
            'success': True,
            'state': state,
            'url': _webapp_url_override if _webapp_url_explicit else None,
            'normalization_hint': hint
        }
    except Exception as exc:
        return {
            'success': False,
            'error': str(exc),
            'url': target_url
        }


# ---------------------- URL 校验工具 ----------------------
def is_valid_url(url: str) -> bool:
    if not url:
        return False
    try:
        parsed = urllib.parse.urlparse(url)
    except ValueError:
        return False
    return bool(parsed.scheme) and bool(parsed.netloc)


HOST_LABEL_RE = re.compile(r"^(?!-)[A-Za-z0-9-]{1,63}(?<!-)$")


def is_probable_ip(host: str) -> bool:
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        return False


def is_probable_domain(host: str) -> bool:
    if not host or len(host) > 253 or host.endswith('.'):
        return False
    labels = host.split('.')
    if len(labels) < 2:
        return False
    return all(HOST_LABEL_RE.match(label) for label in labels)


def normalize_webapp_url_input(raw_value: str):
    """Normalize administrator input for the WebApp URL command.

    Returns a tuple ``(normalized_value, hint)`` where ``hint`` describes any
    automatic transformation that was applied. ``normalized_value`` may be
    ``None`` (reset to default) or an empty string (disable).
    """

    if raw_value is None:
        raw_value = ""

    value = raw_value.strip()
    lowered = value.lower()

    if lowered in {"default", "reset"}:
        return None, "default"
    if lowered in {"", "disable", "none", "off"}:
        return "", "disabled"

    if lowered in {"auto", "https-auto", "https"}:
        https_url = f"https://{CONFIG['SERVER_PUBLIC_IP']}/webapp"
        return https_url, "auto_https"
    if lowered in {"http-auto"}:
        http_url = f"http://{CONFIG['SERVER_PUBLIC_IP']}:{CONFIG['WEBHOOK_PORT']}/webapp"
        return http_url, "auto_http"

    parsed = urllib.parse.urlparse(value)
    if parsed.scheme:
        return value, None

    candidate = value.lstrip('/')
    guessed = f"https://{candidate}"
    parsed_guess = urllib.parse.urlparse(guessed)
    host = parsed_guess.hostname or ""
    if host and (is_probable_ip(host) or is_probable_domain(host)):
        return guessed, "guessed_https"

    return value, None


def is_secure_webapp_url(url: str) -> bool:
    if not url:
        return False
    try:
        parsed = urllib.parse.urlparse(url)
    except ValueError:
        return False
    return parsed.scheme.lower() == "https" and bool(parsed.netloc)


def build_webapp_buttons(raw_url: str):
    """Prepare safe inline keyboard buttons and notice for the web application."""
    webapp_url = (raw_url or "").strip()
    if not webapp_url:
        return [], None

    if not is_valid_url(webapp_url):
        return [], "⚠️ 配置的网页地址无效，请联系管理员更新。"

    buttons = []
    notice = None

    if is_secure_webapp_url(webapp_url):
        buttons.append(types.InlineKeyboardButton("🌐 网页版", web_app=types.WebAppInfo(url=webapp_url)))
        buttons.append(types.InlineKeyboardButton("🔗 浏览器打开", url=webapp_url))
    else:
        buttons.append(types.InlineKeyboardButton("🔗 浏览器打开", url=webapp_url))
        notice = "⚠️ 当前 Web 版仅支持浏览器打开，需配置 HTTPS 才能在 Telegram 内置 WebApp 中使用。"

    return buttons, notice


def build_webapp_status_report(raw_url: str) -> str:
    """Return a MarkdownV2 status message about the configured web application URL."""
    url = (raw_url or "").strip()
    lines = ["🌐 *" + escape_markdown("网页版本状态") + "*"]

    if not url:
        lines.append("")
        lines.append(escape_markdown("当前未配置 Web 版访问地址。"))
        lines.append(escape_markdown("请在配置中设置 WEBAPP_URL 或通过环境变量覆盖。"))
        return "\n".join(lines)

    if not is_valid_url(url):
        lines.append("")
        lines.append("⚠️ " + escape_markdown("配置的地址无效："))
        lines.append(f"`{escape_for_code(url)}`")
        lines.append("")
        lines.append(escape_markdown("请确认地址格式正确（例如 https://example.com/webapp ）。"))
        return "\n".join(lines)

    lines.append("")
    lines.append("*" + escape_markdown("当前地址") + "*")
    lines.append(f"`{escape_for_code(url)}`")
    lines.append("")

    if is_secure_webapp_url(url):
        lines.append("✅ " + escape_markdown("该地址符合 Telegram WebApp 要求，可直接在机器人内打开。"))
    else:
        lines.append("⚠️ " + escape_markdown("该地址不是 HTTPS，因此无法以内嵌 WebApp 打开。"))
        lines.append("")
        lines.append("*" + escape_markdown("快速指引") + "*")
        guidance_steps = [
            "准备一个指向服务器的域名，并将其解析到当前 IP。",
            "在服务器上申请有效的 TLS 证书（如使用 Nginx + Certbot 或 Caddy）。",
            "让 /webapp 路由通过 HTTPS 对外提供服务，并更新配置中的 WEBAPP_URL。",
        ]
        for step in guidance_steps:
            lines.append("• " + escape_markdown(step))
        lines.append("")
        lines.append(escape_markdown("完成上述步骤后，可重新发送 /webapp 查看检测结果。"))

    lines.append("")
    lines.append(escape_markdown("管理员可发送 /setwebapp 查看或更新网页地址。"))
    lines.append(escape_markdown("示例：/setwebapp auto 或 /setwebapp https://example.com/webapp"))
    return "\n".join(lines)


def _sanitize_for_link_text(text: str) -> str:
    """Removes characters that conflict with Markdown link syntax."""
    if not isinstance(text, str):
        text = str(text)
    return re.sub(r'[\[\]]', '', text)

def truncate_for_link_text(text: str, max_bytes: int = 60) -> str:
    text_str = str(text or '用户').strip()
    if not text_str:
        text_str = '用户'
    encoded = text_str.encode('utf-8')
    if len(encoded) <= max_bytes:
        return text_str
    truncated_encoded = encoded[:max_bytes]
    return truncated_encoded.decode('utf-8', 'ignore') + '…'

def serialize_message(message: types.Message) -> dict:
    if not message:
        return None
    
    text = None
    if message.content_type == 'text':
        text = message.html_text
    elif message.caption:
        text = message.html_caption
    
    data = {
        'content_type': message.content_type,
        'text': text,
        'parse_mode': 'HTML' if text else None
    }

    file_id = None
    if message.photo:
        file_id = message.photo[-1].file_id
    elif message.video:
        file_id = message.video.file_id
    elif message.document:
        file_id = message.document.file_id
    elif message.audio:
        file_id = message.audio.file_id
    elif message.voice:
        file_id = message.voice.file_id
    elif message.sticker:
        file_id = message.sticker.file_id
    elif message.animation:
        file_id = message.animation.file_id
    
    if file_id:
        data['file_id'] = file_id
        
    if data.get('text') or data.get('file_id'):
        return data
    return None

def send_serialized_message(chat_id: int, serialized_data: dict, business_connection_id: str = None):
    if not serialized_data or not isinstance(serialized_data, dict):
        print(f"💥 [SendSerialized] Invalid data provided. Expected dict, got {type(serialized_data)}.")
        return

    try:
        content_type = serialized_data.get('content_type')
        text = serialized_data.get('text')
        file_id = serialized_data.get('file_id')
        parse_mode = serialized_data.get('parse_mode')

        kwargs = {'chat_id': chat_id}
        if business_connection_id:
            kwargs['business_connection_id'] = business_connection_id

        if content_type == 'text':
            bot.send_message(text=text, parse_mode=parse_mode, **kwargs)
        elif content_type == 'photo':
            bot.send_photo(photo=file_id, caption=text, parse_mode=parse_mode, **kwargs)
        elif content_type == 'document':
            bot.send_document(document=file_id, caption=text, parse_mode=parse_mode, **kwargs)
        elif content_type == 'video':
            bot.send_video(video=file_id, caption=text, parse_mode=parse_mode, **kwargs)
        elif content_type == 'audio':
            bot.send_audio(audio=file_id, caption=text, parse_mode=parse_mode, **kwargs)
        elif content_type == 'voice':
            bot.send_voice(voice=file_id, caption=text, parse_mode=parse_mode, **kwargs)
        elif content_type == 'sticker':
            bot.send_sticker(sticker=file_id, **kwargs)
        elif content_type == 'animation':
            bot.send_animation(animation=file_id, caption=text, parse_mode=parse_mode, **kwargs)
        else:
            if text:
                bot.send_message(text=text, parse_mode=parse_mode, **kwargs)
        return True
    except ApiTelegramException as e:
        print(f"💥 [SendSerialized] 发送消息失败 (Chat: {chat_id}, Conn: {business_connection_id}). 错误: {e.description}")
    except Exception as e:
        print(f"💥 [SendSerialized] 发送消息时发生未知错误: {e}")
    return False

# ---------------------- 广告与一言 API ----------------------
AD_CONTACT_ADMIN = "@KINGYUN888"
AD_BUSINESS_SITE = "https://king.jike66.top/"
AD_OFFICIAL_CHANNEL = CONFIG.get('REQUIRED_CHANNEL', '@KINGHEN888')

AD_TEXT_PREFIX = (
    f"👑 作者 [{escape_markdown(AD_CONTACT_ADMIN)}](https://t.me/{AD_CONTACT_ADMIN.lstrip('@')}) "
    f"\\| 📢 频道 [{escape_markdown(AD_OFFICIAL_CHANNEL)}](https://t.me/{AD_OFFICIAL_CHANNEL.lstrip('@')}) "
    f"\\| 🌐 业务 [{escape_markdown('官网')}]({AD_BUSINESS_SITE})"
)
ADVERTISEMENT_TEXT = AD_TEXT_PREFIX


def get_advertisement_html():
    contact_link = f"https://t.me/{AD_CONTACT_ADMIN.lstrip('@')}"
    channel_link = f"https://t.me/{AD_OFFICIAL_CHANNEL.lstrip('@')}"
    business_link = AD_BUSINESS_SITE
    return (
        '<div class="ad">👑 作者 '
        f'<a href="{contact_link}" target="_blank">{html.escape(AD_CONTACT_ADMIN)}</a>'
        ' | 📢 频道 '
        f'<a href="{channel_link}" target="_blank">{html.escape(AD_OFFICIAL_CHANNEL)}</a>'
        ' | 🌐 业务 '
        f'<a href="{business_link}" target="_blank">官网</a></div>'
    )

def get_hitokoto():
    try:
        response = requests.get("https://v1.hitokoto.cn/", timeout=5)
        response.raise_for_status()
        data = response.json()
        quote = data.get('hitokoto')
        source = data.get('from', '未知来源')
        return f"_{escape_markdown(f'“{quote}”')}_\n— {escape_markdown(source)}"
    except requests.exceptions.RequestException as e:
        print(f"⚠️ 获取一言失败: {e}")
        return None


# ---------------------- 文件与数据库读写 ----------------------
def load_json_file(filename, lock):
    with lock:
        try:
            with open(filename, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

def save_json_file(filename, data, lock):
    with lock:
        try:
            ensure_parent_dir(Path(filename))
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            print(f"⚠️ 写入文件 '{filename}' 失败: {e}")
            return False

def load_reports():
    data = load_json_file(CONFIG["REPORTS_FILE"], reports_lock)
    if "pending" not in data: data["pending"] = {}
    if "verified" not in data: data["verified"] = {}
    return data

def save_reports(data):
    return save_json_file(CONFIG["REPORTS_FILE"], data, reports_lock)

def load_channels():
    with channels_lock:
        try:
            with open(CONFIG["CHANNELS_FILE"], "r", encoding="utf-8") as f:
                channels = json.load(f)
                processed = []
                for ch in channels:
                    if isinstance(ch, str):
                        if ch.startswith('@'): processed.append(ch)
                        elif ch.isdigit() or (ch.startswith('-') and ch[1:].isdigit()):
                            try: processed.append(int(ch))
                            except ValueError: pass
                        else: processed.append(f'@{ch}')
                    elif isinstance(ch, int): processed.append(ch)
                return processed
        except (FileNotFoundError, json.JSONDecodeError):
            return ['@CJGSPD', '@bhsgongshi', '@mijingzy', '@xiaohaigefanzha', '@XingLian_FZ']
        except Exception as e:
            print(f"⚠️ 加载频道列表时发生未知错误: {e}")
            return ['@CJGSPD', '@bhsgongshi']

def save_channels(channels):
    global target_channels
    with channels_lock:
        valid_channels = []
        for ch in channels:
            if isinstance(ch, str) and ch.startswith('@'): valid_channels.append(ch)
            elif isinstance(ch, int): valid_channels.append(ch)
            elif isinstance(ch, str) and (ch.isdigit() or (ch.startswith('-') and ch[1:].isdigit())):
                try: valid_channels.append(int(ch))
                except ValueError: pass
        
        unique_channels = sorted(list(set(valid_channels)), key=lambda x: str(x).lower())

        try:
            ensure_parent_dir(Path(CONFIG["CHANNELS_FILE"]))
            with open(CONFIG["CHANNELS_FILE"], "w", encoding="utf-8") as f:
                json.dump([str(ch) if isinstance(ch, int) else ch for ch in unique_channels], f, ensure_ascii=False, indent=2)
            target_channels = unique_channels
            print(f"✅ 成功保存监控频道列表: {unique_channels}")
            return True
        except Exception as e:
            print(f"⚠️ 保存监控频道列表失败: {e}")
            return False

# ---------------------- 数据库管理 ----------------------
def get_db_connection():
    conn = sqlite3.connect(str(CONFIG["DATABASE_FILE"]), timeout=15, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                bio TEXT,
                phone TEXT,
                last_seen INTEGER,
                active_usernames_json TEXT,
                business_bio TEXT,
                business_location_json TEXT,
                business_work_hours_json TEXT
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS username_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                old_username TEXT,
                new_username TEXT,
                change_date INTEGER NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS name_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                old_first_name TEXT,
                new_first_name TEXT,
                old_last_name TEXT,
                new_last_name TEXT,
                change_date INTEGER NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS bio_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                old_bio TEXT,
                new_bio TEXT,
                change_date INTEGER NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS phone_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                old_phone TEXT,
                new_phone TEXT,
                change_date INTEGER NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS message_history (
                message_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                text TEXT,
                message_date INTEGER NOT NULL,
                link TEXT,
                PRIMARY KEY (message_id, chat_id),
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS chat_info (
                chat_id INTEGER PRIMARY KEY,
                title TEXT,
                username TEXT,
                last_updated INTEGER
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS bot_interactors (
                user_id INTEGER PRIMARY KEY,
                last_interaction_date INTEGER NOT NULL
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS business_connections (
                connection_id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                is_enabled BOOLEAN DEFAULT 1,
                last_updated INTEGER NOT NULL
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS checked_contacts (
                user_id INTEGER NOT NULL,
                contact_id INTEGER NOT NULL,
                last_checked INTEGER NOT NULL,
                is_scammer BOOLEAN DEFAULT 0,
                PRIMARY KEY (user_id, contact_id)
            )
        ''')
        
        # --- Tables for Business Ledger Feature ---
        c.execute('''
            CREATE TABLE IF NOT EXISTS business_ledgers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                contact_id INTEGER NOT NULL,
                balance REAL NOT NULL DEFAULT 0.0,
                currency TEXT NOT NULL DEFAULT '$',
                auto_pin BOOLEAN NOT NULL DEFAULT 0,
                UNIQUE(user_id, contact_id)
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS ledger_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ledger_id INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                amount REAL NOT NULL,
                new_balance REAL NOT NULL,
                description TEXT,
                FOREIGN KEY (ledger_id) REFERENCES business_ledgers (id) ON DELETE CASCADE
            )
        ''')

        # --- Tables for Premium Auto-Reply Features ---
        c.execute('''
            CREATE TABLE IF NOT EXISTS offline_replies (
                user_id INTEGER PRIMARY KEY,
                is_enabled BOOLEAN DEFAULT 0,
                reply_message_json TEXT,
                last_updated INTEGER,
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS keyword_replies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                keyword TEXT NOT NULL,
                reply_message_json TEXT NOT NULL,
                last_updated INTEGER,
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
            )
        ''')
        
        # ---------------------- 新增：赞助功能数据库表 ----------------------
        c.execute('''
            CREATE TABLE IF NOT EXISTS okpay_orders (
                order_id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                timestamp INTEGER NOT NULL
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS sponsorships (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                amount_usdt REAL NOT NULL,
                order_id TEXT UNIQUE,
                timestamp INTEGER NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE SET NULL
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS sponsors (
                user_id INTEGER PRIMARY KEY,
                total_amount_usdt REAL NOT NULL DEFAULT 0.0,
                last_sponsored_at INTEGER NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
            )
        ''')
        # ---------------------- 新增结束 ----------------------


        # --- Indexes ---
        c.execute('CREATE INDEX IF NOT EXISTS idx_message_history_user_id ON message_history (user_id, message_date DESC)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_username_history_user_id ON username_history (user_id, change_date DESC)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_username_history_new_username ON username_history (new_username)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_users_username ON users (username)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_name_history_user_id ON name_history (user_id, change_date DESC)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_bio_history_user_id ON bio_history (user_id, change_date DESC)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_phone_history_user_id ON phone_history (user_id, change_date DESC)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_message_history_chat_user ON message_history (chat_id, user_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_chat_info_chat_id ON chat_info (chat_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_bot_interactors_user_id ON bot_interactors (user_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_business_connections_user_id ON business_connections (user_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_checked_contacts_user_contact ON checked_contacts (user_id, contact_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_offline_replies_user_id ON offline_replies (user_id)')
        c.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_keyword_replies_user_keyword ON keyword_replies (user_id, keyword)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_business_ledgers_user_contact ON business_ledgers (user_id, contact_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_ledger_history_ledger_id ON ledger_history (ledger_id)')

        # ---------------------- 新增：赞助功能数据库索引 ----------------------
        c.execute('CREATE INDEX IF NOT EXISTS idx_okpay_orders_user_id ON okpay_orders (user_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_sponsors_total_amount ON sponsors (total_amount_usdt DESC)')
        # ---------------------- 新增结束 ----------------------

        conn.commit()
        
        try:
            c.execute("ALTER TABLE users ADD COLUMN business_bio TEXT")
            c.execute("ALTER TABLE users ADD COLUMN business_location_json TEXT")
            c.execute("ALTER TABLE users ADD COLUMN business_work_hours_json TEXT")
            conn.commit()
            print("🗃️ 数据库 'users' 表已更新，增加了 Business 字段。")
        except sqlite3.OperationalError as e:
            if "duplicate column name" not in str(e):
                raise e
        
        conn.close()
    print(f"🗃️ 数据库初始化完成 ({BOT_VERSION.split('|')[0].strip()} Schema).")


# ---------------------- 日志发送逻辑 ----------------------
def send_log_to_channel(text):
    if CONFIG.get("LOG_CHANNEL_ID"):
        with log_buffer_lock:
            log_buffer.append(text)

def log_batcher_thread():
    while True:
        time.sleep(CONFIG["LOG_BATCH_INTERVAL"])
        
        with log_buffer_lock:
            if not log_buffer:
                continue
            logs_to_send = list(log_buffer)
            log_buffer.clear()

        if not logs_to_send:
            continue
            
        message_parts = []
        current_part = f"📝 *批量日志更新* \\| {escape_markdown(datetime.now(timezone.utc).astimezone(CHINA_TZ).strftime('%Y-%m-%d %H:%M:%S CST'))}\n\n"
        
        for log_item in logs_to_send:
            if len(current_part) + len(log_item) + 2 > CONFIG["LOG_MAX_MESSAGE_LENGTH"]:
                message_parts.append(current_part)
                current_part = f"📝 *批量日志更新 \\| 续*\n\n"
            current_part += log_item + "\n\n"
        message_parts.append(current_part)

        for part in message_parts:
            try:
                bot.send_message(CONFIG["LOG_CHANNEL_ID"], part, parse_mode="MarkdownV2", disable_web_page_preview=True)
                if len(message_parts) > 1:
                    time.sleep(1)
            except Exception as e:
                print(f"⚠️ 批量发送日志到频道失败: {e}")
                print(f"❌ 已丢弃失败的日志批次以防止循环错误。内容:\n{part[:1000]}...")
                break

# ---------------------- 用户与消息处理 (核心数据抓取逻辑) ----------------------
async def update_user_in_db(user_from_event: User):
    """
    This is the core function for synchronizing user data.
    It fetches the latest full user profile from Telegram's API
    and updates the local database, creating change history records.
    (FIXED VERSION 2)
    """
    if not user_from_event or not isinstance(user_from_event, User) or user_from_event.bot:
        return

    now = int(time.time())
    user_id = user_from_event.id
    
    canonical_user = None
    full_bio = None
    business_bio = None
    business_location_json = None
    business_work_hours_json = None

    try:
        full_user_info = await client(GetFullUserRequest(user_id))
        
        if hasattr(full_user_info, 'users') and full_user_info.users:
            found_user = next((u for u in full_user_info.users if u.id == user_id), None)
            if found_user:
                canonical_user = found_user
        
        if hasattr(full_user_info, 'full_user'):
            full_user_data = full_user_info.full_user
            full_bio = full_user_data.about
            business_bio = getattr(full_user_data, 'business_bio', None)

            if getattr(full_user_data, 'business_location', None):
                loc = full_user_data.business_location
                loc_data = {'address': loc.address}
                if isinstance(loc.geo_point, GeoPoint):
                    loc_data['geo'] = {'lat': loc.geo_point.lat, 'long': loc.geo_point.long}
                business_location_json = json.dumps(loc_data)
            
            if getattr(full_user_data, 'business_work_hours', None):
                wh = full_user_data.business_work_hours
                wh_data = {'timezone_id': wh.timezone_id, 'periods': []}
                if hasattr(wh, 'periods') and wh.periods is not None:
                    wh_data['periods'] = [{'start_minute': p.start_minute, 'end_minute': p.end_minute} for p in wh.periods]
                business_work_hours_json = json.dumps(wh_data)

    except (PeerIdInvalidError, TypeError, ValueError):
        print(f"ℹ️ [Profile-Update] 无法获取用户 {user_id} 的完整信息 (可能已删除/无效). 使用事件数据作为备用。")
    except Exception as e:
        print(f"⚠️ [Profile-Error] 获取用户 {user_id} 完整资料时发生未知错误: {e}. 使用事件数据作为备用。")
    
    if not canonical_user:
        canonical_user = user_from_event

    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        db_user = c.fetchone()
        
        api_active_usernames = set()
        if canonical_user.username:
            api_active_usernames.add(canonical_user.username)
        if hasattr(canonical_user, 'usernames') and canonical_user.usernames:
            for u_obj in canonical_user.usernames:
                if u_obj.active:
                    api_active_usernames.add(u_obj.username)
        
        new_active_usernames_json = json.dumps(sorted(list(api_active_usernames)))
        
        new_rep_username = canonical_user.username
        if new_rep_username is None and api_active_usernames:
            new_rep_username = sorted(list(api_active_usernames))[0]
        
        display_name = (canonical_user.first_name or "") + (" " + canonical_user.last_name if canonical_user.last_name else "")
        user_link_for_log = f"[{escape_markdown(truncate_for_link_text(display_name or f'User {user_id}'))}](tg://user?id={user_id})"

        if not db_user: # New user, insert everything
            c.execute('''
                INSERT INTO users (user_id, username, first_name, last_name, bio, phone, last_seen, active_usernames_json,
                                   business_bio, business_location_json, business_work_hours_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (user_id, new_rep_username, canonical_user.first_name, canonical_user.last_name, full_bio, canonical_user.phone, now, new_active_usernames_json,
                  business_bio, business_location_json, business_work_hours_json))
            
            log_entries = []
            if api_active_usernames:
                c.execute('''INSERT INTO username_history (user_id, new_username, change_date) VALUES (?, ?, ?)''', (user_id, new_rep_username, now))
                log_entries.append(f"✍️ *{escape_markdown('用户名:')}* " + ", ".join([f"`@{escape_for_code(u)}`" for u in sorted(list(api_active_usernames))]))

            if display_name.strip():
                c.execute('''INSERT INTO name_history (user_id, new_first_name, new_last_name, change_date) VALUES (?, ?, ?, ?)''', (user_id, canonical_user.first_name, canonical_user.last_name, now))
                log_entries.append(f"👤 *{escape_markdown('姓名:')}* {escape_markdown(display_name.strip())}")

            if full_bio:
                c.execute('''INSERT INTO bio_history (user_id, new_bio, change_date) VALUES (?, ?, ?)''', (user_id, full_bio, now))
            
            if canonical_user.phone:
                 c.execute('''INSERT INTO phone_history (user_id, new_phone, change_date) VALUES (?, ?, ?)''', (user_id, canonical_user.phone, now))
                 log_entries.append(f"📱 *{escape_markdown('手机:')}* `{escape_for_code(canonical_user.phone)}`")

            if log_entries:
                log_text = f"✅ *{escape_markdown('新用户入库')}*\n*{escape_markdown('用户:')}* {user_link_for_log} \\| `{user_id}`\n" + "\n".join(log_entries)
                send_log_to_channel(log_text)
        
        else: # Existing user, check for changes and update
            # Name change detection
            old_db_name = ((db_user['first_name'] or "") + " " + (db_user['last_name'] or "")).strip()
            api_name = display_name.strip()
            if api_name != old_db_name:
                c.execute('''INSERT INTO name_history (user_id, old_first_name, new_first_name, old_last_name, new_last_name, change_date) VALUES (?, ?, ?, ?, ?, ?)''',
                          (user_id, db_user['first_name'], canonical_user.first_name, db_user['last_name'], canonical_user.last_name, now))
                send_log_to_channel(f"🔄 *{escape_markdown('姓名变更')}*\n*{escape_markdown('用户:')}* {user_link_for_log}\n*{escape_markdown('旧:')}* {escape_markdown(old_db_name or '无')}\n*{escape_markdown('新:')}* {escape_markdown(api_name or '无')}")

            # Username change detection (ROBUST FIX)
            old_rep_username_from_db = db_user['username']
            if new_rep_username != old_rep_username_from_db:
                c.execute('''INSERT INTO username_history (user_id, old_username, new_username, change_date) VALUES (?, ?, ?, ?)''',
                          (user_id, old_rep_username_from_db, new_rep_username, now))
                log_msg = (f"🔄 *{escape_markdown('主用户名变更')}*\n"
                           f"*{escape_markdown('用户:')}* {user_link_for_log}\n"
                           f"*{escape_markdown('旧:')}* {escape_markdown(f'@{old_rep_username_from_db}' if old_rep_username_from_db else '无')}\n"
                           f"*{escape_markdown('新:')}* {escape_markdown(f'@{new_rep_username}' if new_rep_username else '无')}")
                send_log_to_channel(log_msg)

            # Active usernames list change detection (to avoid duplicate logs)
            old_active_usernames_set = set(json.loads(db_user['active_usernames_json'])) if db_user['active_usernames_json'] else set()
            if api_active_usernames != old_active_usernames_set and new_rep_username == old_rep_username_from_db:
                added = api_active_usernames - old_active_usernames_set
                removed = old_active_usernames_set - api_active_usernames
                if added or removed:
                    log_parts = [f"🔄 *{escape_markdown('用户名列表变更')}*\n*{escape_markdown('用户:')}* {user_link_for_log}"]
                    if added: log_parts.append(f"*{escape_markdown('增加:')}* " + ", ".join([f"`@{escape_for_code(u)}`" for u in sorted(list(added))]))
                    if removed: log_parts.append(f"*{escape_markdown('移除:')}* " + ", ".join([f"`@{escape_for_code(u)}`" for u in sorted(list(removed))]))
                    send_log_to_channel('\n'.join(log_parts))

            # Bio change detection
            if full_bio is not None and full_bio != db_user['bio']:
                 c.execute('''INSERT INTO bio_history (user_id, old_bio, new_bio, change_date) VALUES (?, ?, ?, ?)''', (user_id, db_user['bio'], full_bio, now))
                 send_log_to_channel(f"🔄 *{escape_markdown('简介变更')}*\n*{escape_markdown('用户:')}* {user_link_for_log}\n*{escape_markdown('新简介:')}* {escape_markdown(full_bio or '空')}")

            # Phone change detection
            if canonical_user.phone and canonical_user.phone != db_user['phone']:
                c.execute('''INSERT INTO phone_history (user_id, old_phone, new_phone, change_date) VALUES (?, ?, ?, ?)''', (user_id, db_user['phone'], canonical_user.phone, now))
                send_log_to_channel(f"🔄 *{escape_markdown('手机变更')}*\n*{escape_markdown('用户:')}* {user_link_for_log}\n*{escape_markdown('旧:')}* `{escape_for_code(db_user['phone'] or '无')}`\n*{escape_markdown('新:')}* `{escape_for_code(canonical_user.phone)}`")

            # Update the main users table with all new information
            c.execute('''
                UPDATE users
                SET username = ?, first_name = ?, last_name = ?, bio = ?, phone = ?, last_seen = ?, active_usernames_json = ?,
                    business_bio = ?, business_location_json = ?, business_work_hours_json = ?
                WHERE user_id = ?
            ''', (
                new_rep_username,
                canonical_user.first_name,
                canonical_user.last_name,
                full_bio,
                canonical_user.phone,
                now,
                new_active_usernames_json,
                business_bio,
                business_location_json,
                business_work_hours_json,
                user_id
            ))
        conn.commit()
        conn.close()


def _write_message_to_db_sync(message, chat_id, link):
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        try:
            if message.chat and isinstance(message.chat, (Chat, Channel)):
                all_usernames = []
                primary_username = getattr(message.chat, 'username', None)
                if primary_username:
                    all_usernames.append(primary_username)
                if hasattr(message.chat, 'usernames') and message.chat.usernames:
                    for u in message.chat.usernames:
                        if u.active and u.username not in all_usernames:
                            all_usernames.append(u.username)
                
                username_to_store = primary_username
                if not username_to_store and all_usernames:
                    username_to_store = all_usernames[0]

                c.execute('''
                    INSERT INTO chat_info (chat_id, title, username, last_updated)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(chat_id) DO UPDATE SET
                        title = excluded.title,
                        username = excluded.username,
                        last_updated = excluded.last_updated
                ''', (chat_id, getattr(message.chat, 'title', None), username_to_store, int(time.time())))

            c.execute('''
                INSERT OR IGNORE INTO message_history (message_id, chat_id, user_id, text, message_date, link)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (message.id, chat_id, message.sender_id, message.text, int(message.date.timestamp()), link))
            
            conn.commit()
        except sqlite3.IntegrityError:
            pass
        finally:
            conn.close()

async def save_message_to_db_async(message: telebot.types.Message):
    if not message or not message.sender_id or not isinstance(message.sender, User) or message.sender.bot:
        return
    
    await update_user_in_db(message.sender)
    
    chat_id = utils.get_peer_id(message.peer_id)
    link = ""

    if hasattr(message.chat, 'username') and message.chat.username:
        link = f"https://t.me/{message.chat.username}/{message.id}"
    elif hasattr(message.peer_id, 'channel_id'):
        full_channel_id = utils.get_peer_id(PeerChannel(message.peer_id.channel_id))
        if str(full_channel_id).startswith("-100"):
             short_id = str(full_channel_id)[4:]
             link = f"https://t.me/c/{short_id}/{message.id}"
    
    await asyncio.to_thread(_write_message_to_db_sync, message, chat_id, link)


def _resolve_historic_query_to_id(query: str):
    query_norm = query.lower().strip().lstrip('@')
    
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()

        if query_norm.isdigit():
            user_id_int = int(query_norm)
            c.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id_int,))
            res = c.fetchone()
            if res:
                conn.close()
                return res['user_id']
        
        c.execute("SELECT user_id FROM users WHERE LOWER(username) = ?", (query_norm,))
        res = c.fetchone()
        if res:
            conn.close()
            return res['user_id']
        
        search_pattern = f'%"{query_norm}"%'
        c.execute("SELECT user_id FROM users WHERE active_usernames_json LIKE ?", (search_pattern,))
        res = c.fetchone()
        if res:
            conn.close()
            return res['user_id']

        c.execute("SELECT user_id FROM username_history WHERE LOWER(new_username) = ? ORDER BY change_date DESC LIMIT 1", (query_norm,))
        res = c.fetchone()
        if res:
            conn.close()
            return res['user_id']
            
    return None

def _get_profile_state_at_timestamp(c: sqlite3.Cursor, user_id: int, timestamp: int) -> dict:
    c.execute("""
        SELECT new_first_name, new_last_name FROM name_history
        WHERE user_id = ? AND change_date <= ?
        ORDER BY change_date DESC, id DESC LIMIT 1
    """, (user_id, timestamp))
    name_row = c.fetchone()
    full_name = None
    if name_row:
        full_name = f"{name_row['new_first_name'] or ''} {name_row['new_last_name'] or ''}".strip()

    c.execute("""
        SELECT new_username FROM username_history
        WHERE user_id = ? AND change_date <= ?
        ORDER BY change_date DESC, id DESC LIMIT 1
    """, (user_id, timestamp))
    user_row = c.fetchone()
    username = user_row['new_username'] if user_row else None
    
    return {'name': full_name, 'username': username}


def query_user_history_from_db(user_id: int):
    history = {
        "user_id": user_id, "current_profile": None,
        "profile_history": [],
    }
    
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()

        c.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        current_profile = c.fetchone()
        if not current_profile:
            conn.close()
            return None
        history['current_profile'] = current_profile

        all_events = []
        c.execute("SELECT change_date, new_username as detail FROM username_history WHERE user_id = ?", (user_id,))
        for row in c.fetchall():
            all_events.append({'date': row['change_date'], 'detail': row['detail']})
        
        c.execute("SELECT change_date, new_first_name, new_last_name FROM name_history WHERE user_id = ?", (user_id,))
        for row in c.fetchall():
            name = f"{row['new_first_name'] or ''} {row['new_last_name'] or ''}".strip()
            all_events.append({'date': row['change_date'], 'detail': name})

        all_events.sort(key=lambda x: x['date'], reverse=True)
        unique_timestamps = sorted(list(set(event['date'] for event in all_events)), reverse=True)
        
        reconstructed_history = []
        for ts in unique_timestamps:
            snapshot = _get_profile_state_at_timestamp(c, user_id, ts)
            if not reconstructed_history or \
               snapshot['name'] != reconstructed_history[-1]['name'] or \
               snapshot['username'] != reconstructed_history[-1]['username']:
                reconstructed_history.append({
                    'timestamp': ts,
                    'name': snapshot['name'],
                    'username': snapshot['username']
                })
        
        final_display_history = []
        if reconstructed_history:
            for i, snap in enumerate(reconstructed_history):
                keep = True
                if snap['username'] is None and i > 0:
                    if i < len(reconstructed_history) - 1:
                        prev_snap = reconstructed_history[i-1]
                        next_snap = reconstructed_history[i+1]
                        if prev_snap['name'] == next_snap['name']:
                            keep = False
                
                if keep:
                    final_display_history.append(snap)

        truly_final_history = []
        if final_display_history:
            truly_final_history.append(final_display_history[0])
            for i in range(1, len(final_display_history)):
                current_snap = final_display_history[i]
                last_added_snap = truly_final_history[-1]
                if current_snap.get('name') != last_added_snap.get('name') or \
                   current_snap.get('username') != last_added_snap.get('username'):
                    truly_final_history.append(current_snap)

        history['profile_history'] = truly_final_history
        conn.close()
    return history


def query_bio_history_from_db(user_id: int):
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT new_bio, change_date FROM bio_history WHERE user_id = ? AND new_bio IS NOT NULL AND new_bio != '' ORDER BY change_date DESC", (user_id,))
        history_rows = c.fetchall()
        conn.close()

    if not history_rows:
        return []

    deduplicated_history = []
    last_bio = None
    for row in history_rows:
        current_bio = row['new_bio'].strip() if row['new_bio'] else ''
        if current_bio != last_bio:
            deduplicated_history.append({'bio': row['new_bio'], 'date': row['change_date']})
            last_bio = current_bio
            
    return deduplicated_history

def query_phone_history_from_db(user_id: int):
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT new_phone FROM phone_history WHERE user_id = ? AND new_phone IS NOT NULL ORDER BY change_date DESC", (user_id,))
        history = list(dict.fromkeys([row['new_phone'] for row in c.fetchall()]))
        conn.close()
    return history


def query_spoken_groups_from_db(user_id: int):
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT DISTINCT chat_id FROM message_history WHERE user_id = ?", (user_id,))
        spoken_in_chat_ids = {row['chat_id'] for row in c.fetchall()}
        conn.close()
    return spoken_in_chat_ids

def get_chat_info_from_db(chat_id: int):
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT title, username FROM chat_info WHERE chat_id = ?", (chat_id,))
        res = c.fetchone()
        conn.close()
        if res:
            return {'title': res['title'], 'username': res['username']}
        return None

def _get_user_name_from_db(user_id: int) -> str:
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT first_name, last_name FROM users WHERE user_id = ?", (user_id,))
        user_row = c.fetchone()
        conn.close()
        if user_row:
            name = f"{user_row['first_name'] or ''} {user_row['last_name'] or ''}".strip()
            return name if name else str(user_id)
    return str(user_id)

# ---------------------- 活跃用户追踪 ----------------------
active_users = {}
def update_active_user(user_id):
    active_users[user_id] = time.time()

def get_online_user_count():
    threshold_time = time.time() - CONFIG["ONLINE_THRESHOLD"]
    return sum(1 for last_seen in active_users.values() if last_seen > threshold_time)

# ---------------------- Telethon 客户端与核心逻辑 ----------------------
client = TelegramClient(
    CONFIG["session_name"], CONFIG["api_id"], CONFIG["api_hash"],
    system_version="4.16.30-vxCUSTOM", device_model="Pixel 7 Pro", app_version="10.10.0"
)
telethon_loop = None
target_channels = []

async def get_user_status_async(user_id: int):
    if not client.is_connected():
        return 'unknown'
    try:
        full_user = await client(GetFullUserRequest(user_id))
        if not hasattr(full_user, 'full_user') or not hasattr(full_user.full_user, 'status'):
            return 'unknown'

        status = full_user.full_user.status
        if isinstance(status, UserStatusOnline):
            return 'online'
        if isinstance(status, UserStatusOffline):
            return 'offline'
        return 'away'
    except Exception as e:
        print(f"⚠️ [Telethon-StatusCheck] 无法获取用户 {user_id} 的状态: {e}")
        return 'unknown'


# ---------------------- 监控频道及共同群组搜索 ----------------------
async def get_common_groups_with_user(user_id: int):
    if not client.is_connected() or not user_id:
        return []

    common_groups = []
    dialog_count = 0
    checked_count = 0
    try:
        async for dialog in client.iter_dialogs():
            dialog_count += 1
            if not dialog.entity or not isinstance(dialog.entity, (Chat, Channel)):
                continue
            if dialog.is_group or (dialog.is_channel and getattr(dialog.entity, 'megagroup', False)):
                checked_count += 1
                try:
                    await client.get_permissions(dialog.entity, user_id)
                    
                    about_text = None
                    all_usernames = []
                    
                    primary_username = dialog.entity.username if hasattr(dialog.entity, 'username') else None
                    if primary_username:
                        all_usernames.append(primary_username)

                    try:
                        full_entity_request = None
                        if isinstance(dialog.entity, Channel):
                            full_entity_request = GetFullChannelRequest(dialog.entity.id)
                        elif isinstance(dialog.entity, Chat):
                            full_entity_request = GetFullChatRequest(dialog.entity.id)

                        if full_entity_request:
                            full_entity = await client(full_entity_request)
                            
                            if hasattr(full_entity, 'full_chat') and hasattr(full_entity.full_chat, 'about'):
                                about_text = full_entity.full_chat.about

                            chat_obj = None
                            if hasattr(full_entity, 'chats') and full_entity.chats:
                                chat_obj = full_entity.chats[0]

                            if chat_obj and hasattr(chat_obj, 'usernames') and chat_obj.usernames:
                                for u in chat_obj.usernames:
                                    if u.active and u.username not in all_usernames:
                                        all_usernames.append(u.username)
                    except Exception:
                        pass
                    
                    common_groups.append({
                        'title': getattr(dialog, 'title', 'Unknown Title'),
                        'id': dialog.id,
                        'about': about_text,
                        'usernames': all_usernames
                    })
                except UserNotParticipantError:
                    continue
                except (ChatAdminRequiredError, ChannelPrivateError, ValueError, PeerIdInvalidError):
                    continue
                except Exception as e:
                    error_str = str(e).lower()
                    safe_title = getattr(dialog, 'title', f"ID:{getattr(dialog, 'id', 'Unknown')}")
                    if "bot can't be participant" not in error_str and "chat not found" not in error_str:
                         print(f"⚠️ [Common-Group-Scan] Skipped '{safe_title}' due to a permission or access error: {type(e).__name__} - {e}")
                    continue
    except Exception as e:
        print(f"💥 [Common-Group-Scan] Major error during dialog iteration for user {user_id}: {e}")
    
    print(f"✅ [Common-Group-Scan] Scan complete. Checked {checked_count}/{dialog_count} chats. Found {len(common_groups)} common groups with user {user_id}.")
    return common_groups

async def search_monitored_channels_for_user(user_id: int = None, raw_query: str = None):
    if not client.is_connected() or (not user_id and not raw_query):
        return []

    search_queries = set()
    
    if user_id:
        search_queries.add(str(user_id))
        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT username FROM users WHERE user_id = ?", (user_id,))
            user = c.fetchone()
            if user and user['username']:
                search_queries.add(user['username'])
            
            c.execute("SELECT new_username FROM username_history WHERE user_id = ?", (user_id,))
            for row in c.fetchall():
                if row['new_username']:
                    search_queries.add(row['new_username'])
            conn.close()
        
        try:
            live_user = await client.get_entity(user_id)
            if live_user and live_user.username:
                search_queries.add(live_user.username)
        except Exception:
            pass

    if raw_query:
        search_queries.add(raw_query.strip().lstrip('@'))

    search_queries = list(filter(None, search_queries))
    if not search_queries:
        return []

    found_messages = []
    processed_links = set()
    channels_to_scan = target_channels
    
    log_terms = ", ".join(search_queries)
    log_target = f"user {user_id}" if user_id else f"raw query '{raw_query}'"
    print(f"📡 [Scam-Scan] 开始为 {log_target} 在 {len(channels_to_scan)} 个监控频道中搜索 (关键词: {log_terms})...")

    for channel_ref in channels_to_scan:
        try:
            channel_entity = await client.get_entity(channel_ref)
            
            for query in search_queries:
                async for message in client.iter_messages(
                    channel_entity,
                    limit=CONFIG["SCAM_CHANNEL_SEARCH_LIMIT"],
                    search=query
                ):
                    link = ""
                    if hasattr(message.chat, 'username') and message.chat.username:
                        link = f"https://t.me/{message.chat.username}/{message.id}"
                    elif hasattr(message.peer_id, 'channel_id'):
                        full_channel_id = utils.get_peer_id(PeerChannel(message.peer_id.channel_id))
                        short_id = str(full_channel_id)[4:] if str(full_channel_id).startswith("-100") else str(full_channel_id)
                        link = f"https://t.me/c/{short_id}/{message.id}"
                    
                    if link and link not in processed_links:
                        found_messages.append({
                            'link': link,
                            'text': message.text,
                            'chat_title': getattr(channel_entity, 'title', str(channel_ref))
                        })
                        processed_links.add(link)

            await asyncio.sleep(0.5)

        except (ChatAdminRequiredError):
            print(f"ℹ️ [Scam-Scan] 频道 '{channel_ref}' 需要管理员权限进行搜索。跳过...")
        except (ValueError, PeerIdInvalidError, ChannelPrivateError) as e:
            print(f"⚠️ [Scam-Scan] 无法访问频道 '{channel_ref}': {type(e).__name__}。跳过...")
        except Exception as e:
            print(f"💥 [Scam-Scan] 搜索频道 '{channel_ref}' 时发生未知错误: {type(e).__name__} - {e}")
    
    print(f"✅ [Scam-Scan] 完成搜索，为 {log_target} 找到 {len(found_messages)} 条风险提及。")
    return found_messages


# ---------------------- Telethon 事件监听 (数据抓取引擎) ----------------------
@client.on(events.NewMessage(incoming=True, func=lambda e: (e.is_group or e.is_channel) and not e.is_private))
async def historical_message_handler(event):
    try:
        message = event.message
        if not message or not message.sender:
            return

        if isinstance(message.sender, User) and not message.sender.bot:
            await save_message_to_db_async(message)

        if message.text:
            found_ids = set(re.findall(r'[=/\s:](\d{7,12})(?!\d)', message.text))
            
            if not found_ids:
                return

            with db_lock:
                conn = get_db_connection()
                c = conn.cursor()
                placeholders = ','.join('?' for _ in found_ids)
                c.execute(f"SELECT user_id FROM users WHERE user_id IN ({placeholders})", list(found_ids))
                existing_ids = {str(row['user_id']) for row in c.fetchall()}
                conn.close()

            new_ids_to_check = found_ids - existing_ids

            if new_ids_to_check:
                print(f"ℹ️ [Proactive-Scan] 发现 {len(new_ids_to_check)} 个新用户ID: {new_ids_to_check}")
                for user_id_str in new_ids_to_check:
                    try:
                        user_id_int = int(user_id_str)
                        await asyncio.sleep(0.5)
                        entity = await client.get_entity(user_id_int)
                        if isinstance(entity, User) and not entity.bot:
                            print(f"✅ [Proactive-Sync] 主动同步被提及的新用户 {user_id_int}")
                            await update_user_in_db(entity)
                    except (ValueError, TypeError, PeerIdInvalidError):
                        pass
                    except Exception as e:
                        print(f"⚠️ [Proactive-Sync] 检查新ID {user_id_str} 时出错: {e}")

    except Exception as e:
        print(f"💥 [historical_message_handler] 发生严重错误: {e}")
        traceback.print_exc()

@client.on(events.UserUpdate)
async def user_update_handler(event):
    if not event.user_id: return
    try:
        user_to_update = await client.get_entity(event.user_id)
        if user_to_update and isinstance(user_to_update, User) and not user_to_update.bot:
            await update_user_in_db(user_to_update)
    except (ValueError, PeerIdInvalidError):
        return
    except Exception as e:
        print(f"💥 [User Update Error] 处理用户更新失败 (ID: {event.user_id}): {e}")
        traceback.print_exc()

async def join_target_channels():
    print("🤝 正在尝试加入配置文件中的监控频道...")
    channels_to_join = load_channels()
    for channel in channels_to_join:
        try:
            await client(JoinChannelRequest(channel))
            print(f"✅ 成功加入: {channel}")
            await asyncio.sleep(5)
        except UserAlreadyParticipantError:
            print(f"ℹ️ 已在 '{channel}' 中，无需重复加入。")
        except (ValueError, PeerIdInvalidError):
            print(f"⚠️ 无法加入 '{channel}': 频道/群组不存在或链接无效。")
        except (UserNotParticipantError, ChannelPrivateError):
            print(f"ℹ️ 无法访问私有频道 '{channel}' 或非成员。")
        except ChannelsTooMuchError:
            print("❌ 无法加入更多频道：已达到 Telegram 账户的频道/群组上限。")
            break
        except FloodWaitError as e:
            print(f"⏳ 加入频道遭遇 FloodWait，将等待 {e.seconds} 秒...")
            await asyncio.sleep(e.seconds)
        except Exception as e:
            print(f"💥 加入频道 '{channel}' 时发生未知错误: {type(e).__name__} - {e}")
    print("✅ 频道加入流程完成。")


async def _start_telethon_async():
    global target_channels
    print("🚀 正在启动 Telethon 客户端...")
    
    try:
        await client.start(bot_token=CONFIG["BOT_TOKEN"] if not CONFIG.get("api_hash") else None)
    except FloodWaitError as e:
        print(f"❌ Telethon启动时遭遇FloodWait: {e.seconds}s. 请稍后重试。")
        return
    except (ApiIdInvalidError, AuthKeyDuplicatedError) as e:
        print(f"❌ CRITICAL Telethon ERROR: {type(e).__name__}. 请检查配置或删除 .session 文件。")
        return

    print("✅ Telethon 客户端已成功启动。")
    me = await client.get_me()
    print(f"🤖 登录账号: @{me.username} (ID: {me.id})")
    
    target_channels = load_channels()
    await join_target_channels()

    print(f"👂 Telethon 开始在所有已加入的群组和频道中进行实时数据抓取...")
    await client.run_until_disconnected()

def start_telethon():
    global telethon_loop
    loop = asyncio.new_event_loop()
    telethon_loop = loop
    asyncio.set_event_loop(loop)

    while True:
        try:
            print("🚀 [Telethon] 正在启动或尝试重新连接...")
            loop.run_until_complete(_start_telethon_async())

        except (KeyboardInterrupt, asyncio.CancelledError):
            print("🛑 [Telethon] 循环被用户中断。")
            break
        
        except Exception as e:
            print(f"💥 [Telethon] 运行期间发生严重错误: {e}")
            traceback.print_exc()
            if client.is_connected():
                try:
                    loop.run_until_complete(client.disconnect())
                except Exception as disconnect_e:
                    print(f"⚠️ [Telethon] 尝试断开连接时也发生错误: {disconnect_e}")

        print("ℹ️ [Telethon] 客户端已断开。将在 30 秒后尝试重启以确保服务持续...")
        time.sleep(30)

    print("🚪 Telethon 线程已完全停止。")
    if client.is_connected():
        loop.run_until_complete(client.disconnect())
    loop.stop()
    loop.close()
    print("✅ Telethon 资源已释放。")

# ---------------------- Telebot 初始化与成员检查 ----------------------
bot = telebot.TeleBot(CONFIG["BOT_TOKEN"])
bot_name_cache = None

def get_bot_name():
    global bot_name_cache
    if bot_name_cache is None:
        try:
            me = bot.get_me()
            bot_name_cache = me.first_name or "BOT"
        except Exception:
            bot_name_cache = "BOT"
    return bot_name_cache


membership_cache = {}
MEMBERSHIP_CACHE_DURATION = 5
user_submission_state = {}
admin_broadcast_state = {}

def check_membership(func):
    @functools.wraps(func)
    def wrapper(message_or_call, *args, **kwargs):
        user = message_or_call.from_user
        
        if user.id in user_settings_state:
            del user_settings_state[user.id]
        if user.id in user_sponsorship_state: # 新增：清理赞助状态
            del user_sponsorship_state[user.id]

        if not user.is_bot:
            if telethon_loop and telethon_loop.is_running():
                telethon_user = User(
                    id=user.id, first_name=user.first_name, last_name=user.last_name,
                    username=user.username, bot=user.is_bot, access_hash=0,
                )
                asyncio.run_coroutine_threadsafe(update_user_in_db(telethon_user), telethon_loop)
            
            with db_lock:
                conn = get_db_connection()
                try:
                    c = conn.cursor()
                    now = int(time.time())
                    c.execute('''
                        INSERT INTO bot_interactors (user_id, last_interaction_date) VALUES (?, ?)
                        ON CONFLICT(user_id) DO UPDATE SET last_interaction_date = ?
                    ''', (user.id, now, now))
                    conn.commit()
                finally:
                    conn.close()

        if user.id == CONFIG["ADMIN_ID"]:
            return func(message_or_call, *args, **kwargs)

        required_channel = CONFIG.get("REQUIRED_CHANNEL")
        if not required_channel or not required_channel.startswith('@'):
            return func(message_or_call, *args, **kwargs)

        now = time.time()
        
        cache_entry = membership_cache.get(user.id)
        if cache_entry and (now - cache_entry[0] < MEMBERSHIP_CACHE_DURATION):
            if cache_entry[1]:
                return func(message_or_call, *args, **kwargs)
        
        is_member = False
        try:
            chat_member = bot.get_chat_member(required_channel, user.id)
            is_member = chat_member.status in ['member', 'administrator', 'creator']
            membership_cache[user.id] = (now, is_member)
        except ApiTelegramException as e:
            if e.result_json and 'description' in e.result_json and 'user not found' in e.result_json['description']:
                is_member = False
            else:
                print(f"⚠️ 检查成员资格失败 (User: {user.id}): {e.description}")
            membership_cache[user.id] = (now, is_member)
        
        if is_member:
            return func(message_or_call, *args, **kwargs)
        else:
            join_text = (
                f"🚫 *{escape_markdown('访问受限')}*\n\n"
                f"{escape_markdown('请先加入我们的官方频道才能使用此功能：')}\n"
                f"➡️ {escape_markdown(required_channel)}\n\n"
                f"{escape_markdown('感谢您的支持！')}"
            )
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("➡️ 点击加入", url=f"https://t.me/{required_channel.lstrip('@')}"))
            try:
                chat_id = message_or_call.message.chat.id if isinstance(message_or_call, types.CallbackQuery) else message_or_call.chat.id
                bot.send_message(chat_id, join_text, reply_markup=markup, disable_web_page_preview=True, parse_mode="MarkdownV2")
                if isinstance(message_or_call, types.CallbackQuery):
                    bot.answer_callback_query(message_or_call.id)
            except Exception as e:
                print(f"💥 发送加群提示失败: {e}")
    return wrapper

def premium_only(func):
    @functools.wraps(func)
    def wrapper(message_or_call, *args, **kwargs):
        user = message_or_call.from_user
        if not getattr(user, 'is_premium', False):
            try:
                chat_id = message_or_call.message.chat.id if isinstance(message_or_call, types.CallbackQuery) else message_or_call.chat.id
                bot.send_message(chat_id, f"💎 *{escape_markdown('高级功能专属')}*\n\n{escape_markdown('抱歉，此功能仅向尊贵的 Telegram Premium 大会员用户开放。')}", parse_mode="MarkdownV2")
                if isinstance(message_or_call, types.CallbackQuery):
                    bot.answer_callback_query(message_or_call.id)
            except Exception as e:
                print(f"💥 发送 Premium 提示失败: {e}")
            return
        return func(message_or_call, *args, **kwargs)
    return wrapper

# ---------------------- Bot 命令处理 (UI 美化) ----------------------
@bot.message_handler(commands=['start'])
@check_membership
def handle_start(message, is_edit=False):
    webapp_buttons, webapp_notice = build_webapp_buttons(get_configured_webapp_url())
    update_active_user(message.from_user.id)
    
    command_parts = message.text.split(maxsplit=1)
    if len(command_parts) > 1 and command_parts[1].startswith('bizChat'):
        payload = command_parts[1]
        target_id_str = payload.replace('bizChat', '').strip()
        
        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT 1 FROM business_connections WHERE user_id = ? AND is_enabled = 1", (message.from_user.id,))
            is_business_user = c.fetchone()
            conn.close()

        if is_business_user and target_id_str.isdigit():
            print(f"✅ [Business Query] Triggered by user {message.from_user.id} for contact {target_id_str} via deep link.")
            try:
                bot.send_message(message.chat.id, f"ℹ️ *{escape_markdown('正在为您自动查询联系人...')}*\n*{escape_markdown('目标ID:')}* `{target_id_str}`", parse_mode="MarkdownV2")
            except Exception as e:
                print(f"⚠️ [Business Query] Failed to send pre-query notification: {e}")
            
            trigger_query_flow(
                message=message,
                query=target_id_str
            )
            return

    hitokoto_quote = get_hitokoto()
    
    welcome_text = [
        f"🛡️ *猎诈卫士* `{escape_for_code(BOT_VERSION.split('|')[0].strip())}`",
        f"你好，{escape_markdown(message.from_user.first_name)}\\! 我是您的电报安全助手。",
    ]
    
    if hitokoto_quote:
        welcome_text.append(f"\n*{escape_markdown('每日一言')}*\n{hitokoto_quote}")

    welcome_text.extend([
        f"\n*{escape_markdown('——— 功能导航 ———')}*",
        f"{escape_markdown('您可以直接使用下方按钮，或发送相应命令：')}",
        f"`/tougao` {escape_markdown('• 投稿诈骗者信息')}",
        f"`/sponsor` {escape_markdown('• 赞助支持我们')}",
        f"`/leaderboard` {escape_markdown('• 查看赞助排行')}",
        f"`/webapp` {escape_markdown('• 查看网页状态与配置指引')}",
        f"_/Tip: 直接转发用户消息、发送其用户名或ID，即可快速查询\\./_",
    ])
    
    markup = types.InlineKeyboardMarkup(row_width=3) # 改为3列
    markup.add(
        types.InlineKeyboardButton("🔍 查询记录", callback_data="query"),
        types.InlineKeyboardButton("✍️ 投稿骗子", callback_data="tougao"),
        types.InlineKeyboardButton("❤️ 赞助我们", callback_data="sponsor"),
        types.InlineKeyboardButton("💎 高级功能", callback_data="premium:main"),
        types.InlineKeyboardButton("📊 运行状态", callback_data="stats"),
        types.InlineKeyboardButton("🏆 赞助排行", callback_data="leaderboard")
    )
    if webapp_buttons:
        markup.add(*webapp_buttons)

    if webapp_notice:
        welcome_text.append(f"\n{escape_markdown(webapp_notice)}")
    final_text = "\n".join(welcome_text) + f"\n\n{ADVERTISEMENT_TEXT}"
    
    if is_edit:
        try:
            bot.edit_message_text(final_text, message.chat.id, message.message_id, reply_markup=markup, disable_web_page_preview=True, parse_mode="MarkdownV2")
        except ApiTelegramException:
            pass
    else:
        bot.reply_to(message, final_text, reply_markup=markup, disable_web_page_preview=True, parse_mode="MarkdownV2")

# ---------------------- 新增：赞助与排行榜功能 ----------------------
@bot.message_handler(commands=['sponsor'])
@check_membership
def handle_sponsor(message):
    user_id = message.from_user.id
    user_sponsorship_state[user_id] = True # 标记用户进入赞助流程
    prompt_text = (
        f"❤️ *{escape_markdown('赞助支持')}*\n\n"
        f"{escape_markdown('感谢您对本项目的关注与支持！您的每一份赞助都将用于服务器维护和功能开发，帮助我们为更多人提供服务。只可以okpay钱包支付')}\n\n"
        f"*{escape_markdown('这个是okpay钱包，请输入您希望赞助的金额 (USDT)，例如: 0.01')}*"
    )
    bot.reply_to(message, prompt_text, parse_mode="MarkdownV2")
    bot.register_next_step_handler(message, process_sponsor_amount)


@bot.message_handler(commands=['webapp'])
@check_membership
def handle_webapp_status(message):
    update_active_user(message.from_user.id)
    status_text = build_webapp_status_report(get_configured_webapp_url())
    bot.reply_to(message, status_text, parse_mode="MarkdownV2", disable_web_page_preview=True)


@bot.message_handler(commands=['setwebapp'])
@check_membership
def handle_set_webapp(message):
    update_active_user(message.from_user.id)

    if message.from_user.id != CONFIG.get("ADMIN_ID"):
        bot.reply_to(
            message,
            escape_markdown("🚫 仅管理员可以修改网页版本配置。"),
            parse_mode="MarkdownV2"
        )
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) == 1:
        status_text = build_webapp_status_report(get_configured_webapp_url())
        usage_lines = [
            "ℹ️ *" + escape_markdown("配置指引") + "*",
            "`/setwebapp https://example.com/webapp`",
            "`/setwebapp auto` - " + escape_markdown("使用服务器公网 IP 生成 HTTPS 地址"),
            "`/setwebapp http-auto` - " + escape_markdown("生成 HTTP 地址 (不推荐)"),
            "`/setwebapp disable` - " + escape_markdown("暂时关闭按钮"),
            "`/setwebapp default` - " + escape_markdown("恢复为默认地址"),
        ]
        reply_text = status_text + "\n\n" + "\n".join(usage_lines)
        bot.reply_to(message, reply_text, parse_mode="MarkdownV2", disable_web_page_preview=True)
        return

    result = set_configured_webapp_url(parts[1])
    if not result.get('success'):
        error = result.get('error')
        if error == 'invalid_url':
            msg = escape_markdown("❌ URL 无效，请提供以 http:// 或 https:// 开头的完整地址。")
        else:
            msg = escape_markdown(f"❌ 保存失败: {error}")
        bot.reply_to(message, msg, parse_mode="MarkdownV2")
        return

    state = result.get('state')
    if state == 'custom':
        notice = f"✅ *{escape_markdown('已更新 WebApp 地址。')}*"
    elif state == 'disabled':
        notice = f"✅ *{escape_markdown('已禁用 WebApp 按钮。')}*"
    else:
        notice = f"✅ *{escape_markdown('已恢复默认配置。')}*"

    hint_messages = {
        'auto_https': escape_markdown("已根据服务器公网 IP 自动生成 HTTPS 链接。"),
        'auto_http': escape_markdown("已根据服务器公网 IP 自动生成 HTTP 链接。"),
        'guessed_https': escape_markdown("检测到缺少协议，已自动补全为 HTTPS。"),
    }

    hint = result.get('normalization_hint')
    if hint in hint_messages:
        notice += "\n" + hint_messages[hint]

    status_text = build_webapp_status_report(get_configured_webapp_url())
    reply_text = notice + "\n\n" + status_text
    bot.reply_to(message, reply_text, parse_mode="MarkdownV2", disable_web_page_preview=True)


def create_okpay_order_for_user(user_id: int, amount: float):
    if amount <= 0:
        raise ValueError('金额必须为正数')

    response = okpay_client.pay_link(amount)

    if not response or 'data' not in response or not response['data']:
        error_msg = None
        if isinstance(response, dict):
            error_msg = response.get('error') or response.get('msg')
        raise RuntimeError(error_msg or '创建订单失败')

    order_id = response['data'].get('order_id')
    pay_url = response['data'].get('pay_url')

    if not order_id or not pay_url:
        raise RuntimeError('支付服务返回的数据不完整')

    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        try:
            c.execute(
                "INSERT INTO okpay_orders (order_id, user_id, amount, status, timestamp) VALUES (?, ?, ?, ?, ?)",
                (order_id, user_id, amount, 'pending', int(time.time()))
            )
            conn.commit()
        finally:
            conn.close()

    logger.info(f"用户 {user_id} 订单创建成功，订单号: {order_id}")
    return {'order_id': order_id, 'pay_url': pay_url, 'amount': amount}

def process_sponsor_amount(message):
    user_id = message.from_user.id
    if user_id not in user_sponsorship_state:
        # 如果用户未处于赞助流程，则正常处理消息
        handle_all_other_messages(message)
        return
    
    del user_sponsorship_state[user_id] # 清理状态
    
    try:
        clean_text = re.sub(r'[^\d.]', '', message.text)
        amount = float(clean_text)
        if amount <= 0.0:
            raise ValueError("金额必须为正数")
        
        bot.reply_to(message, escape_markdown("⏳ 正在为您创建支付订单，请稍候..."), parse_mode="MarkdownV2")
        
        response = okpay_client.pay_link(amount)
        
        if not response or 'data' not in response or not response['data']:
            error_msg = response.get('error') or response.get('msg', '未知错误') if isinstance(response, dict) else '无响应'
            bot.send_message(user_id, f"❌ 创建订单失败: {escape_markdown(str(error_msg))}", parse_mode="MarkdownV2")
            logger.error(f"为用户 {user_id} 创建订单失败: {response}")
            return
        
        order_id = response['data']['order_id']
        pay_url = response['data']['pay_url']

        # 将订单信息存入数据库
        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute(
                "INSERT INTO okpay_orders (order_id, user_id, amount, status, timestamp) VALUES (?, ?, ?, ?, ?)",
                (order_id, user_id, amount, 'pending', int(time.time()))
            )
            conn.commit()
            conn.close()

        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🔗 点击支付 (USDT)", url=pay_url))

        bot.send_message(
            user_id,
            f"🛒 *{escape_markdown('订单创建成功!')}*\n\n"
            f"*{escape_markdown('订单号:')}* `{escape_for_code(order_id)}`\n"
            f"*{escape_markdown('金额:')}* `{amount:.2f} USDT`\n"
            f"*{escape_markdown('有效期:')}* {escape_markdown('10分钟')}\n\n"
            f"{escape_markdown('请点击下方按钮，在OKPay页面中完成支付:')}",
            parse_mode='MarkdownV2',
            reply_markup=markup
        )
        logger.info(f"用户 {user_id} 订单创建成功，订单号: {order_id}")

    except (ValueError, TypeError):
        bot.reply_to(message, "⚠️ *{escape_markdown('金额无效')}*\n{escape_markdown('请输入一个有效的数字 (例如: 10 或 10.5)。')}", parse_mode="MarkdownV2")
    except RuntimeError as e:
        bot.send_message(user_id, f"❌ 创建订单失败: {escape_markdown(str(e))}", parse_mode="MarkdownV2")
        logger.error(f"为用户 {user_id} 创建订单失败: {e}")
    except Exception as e:
        logger.exception(f"处理赞助金额时发生错误: {e}")
        bot.reply_to(message, f"❌ {escape_markdown('处理请求时发生错误，请稍后重试。')}", parse_mode="MarkdownV2")


def get_top_sponsors(limit: int = 10):
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute(
            """
            SELECT s.user_id, s.total_amount_usdt, u.first_name, u.last_name
            FROM sponsors s
            LEFT JOIN users u ON s.user_id = u.user_id
            ORDER BY s.total_amount_usdt DESC
            LIMIT ?
            """,
            (limit,)
        )
        rows = c.fetchall()
        conn.close()

    sponsors = []
    for row in rows:
        display_name = f"{row['first_name'] or ''} {row['last_name'] or ''}".strip()
        sponsors.append({
            'user_id': row['user_id'],
            'total_amount_usdt': row['total_amount_usdt'],
            'display_name': display_name or None
        })
    return sponsors


def collect_bot_stats():
    online_count = get_online_user_count()

    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM users")
        total_users = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM bot_interactors")
        interacted_users = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM message_history")
        total_messages = c.fetchone()[0]
        c.execute("SELECT COUNT(id) FROM username_history")
        total_username_changes = c.fetchone()[0]
        c.execute("SELECT COUNT(id) FROM name_history")
        total_name_changes = c.fetchone()[0]
        conn.close()

    reports = load_reports()
    verified_count = len(reports.get('verified', {}))
    channel_count = len(load_channels())
    telethon_connected = bool(telethon_loop and client.is_connected())

    return {
        'online_count': online_count,
        'total_users': total_users,
        'interacted_users': interacted_users,
        'total_messages': total_messages,
        'identity_changes': total_username_changes + total_name_changes,
        'verified_reports': verified_count,
        'monitored_channels': channel_count,
        'telethon_connected': telethon_connected
    }


@bot.message_handler(commands=['leaderboard'])
@check_membership
def handle_leaderboard(message):
    top_sponsors = get_top_sponsors()

    if not top_sponsors:
        text = f"🏆 *{escape_markdown('赞助排行榜')}*\n\n{escape_markdown('目前还没有赞助记录，期待您的支持！')}\n\n{ADVERTISEMENT_TEXT}"
        bot.reply_to(message, text, parse_mode="MarkdownV2", disable_web_page_preview=True)
        return

    leaderboard_parts = [f"🏆 *{escape_markdown('赞助排行榜 Top 10')}*"]
    medals = ["🥇", "🥈", "🥉"]
    
    for i, sponsor in enumerate(top_sponsors):
        user_name = f"{sponsor['first_name'] or ''} {sponsor['last_name'] or ''}".strip()
        if not user_name:
            user_name = f"用户 {sponsor['user_id']}"
        
        rank_icon = medals[i] if i < 3 else f"*{i + 1}\\.*"
        
        line = (f"{rank_icon} [{escape_markdown(truncate_for_link_text(user_name))}](tg://user?id={sponsor['user_id']}) "
                f"\\- `{sponsor['total_amount_usdt']:.2f} USDT`")
        leaderboard_parts.append(line)
        
    final_text = "\n\n".join(leaderboard_parts) + f"\n\n{ADVERTISEMENT_TEXT}"
    bot.reply_to(message, final_text, parse_mode="MarkdownV2", disable_web_page_preview=True)
# ---------------------- 新增结束 ----------------------


@bot.message_handler(commands=['premium_features'])
@check_membership
@premium_only
def handle_premium_info_command(message):
    handle_premium_main_menu(message)

def handle_premium_main_menu(message_or_call):
    is_call = isinstance(message_or_call, types.CallbackQuery)
    message = message_or_call.message if is_call else message_or_call
    chat_id = message.chat.id
    message_id = message.message_id

    try:
        bot_username = bot.get_me().username
    except Exception:
        bot_username = "本机器人"

    info_text = (
        f"💎 *{escape_markdown('高级功能 · 大会员专属')}*\n"
        f"{escape_markdown('感谢您的支持！以下是为您开放的专属功能。所有功能均需连接 Telegram Business 方可使用。')}\n\n"
        f"1️⃣ *{escape_markdown('自动私聊风险检测')}*\n"
        f"_{escape_markdown('我将自动在私聊中检测联系人风险，并用您的身份发送警告。')}_\n"
        f"*› {escape_markdown('启用:')}* {escape_markdown(f'前往`设置` > `Telegram Business` > `聊天机器人`，添加 `@{bot_username}`。')}\n\n"
        f"2️⃣ *{escape_markdown('互动式记账本')}*\n"
        f"_{escape_markdown('一个基于您个人指令的快速记账工具。在与他人的私聊中使用命令即可记账。')}_\n"
        f"*› {escape_markdown('帮助:')}* {escape_markdown('/jz')}\n\n"
        f"3️⃣ *{escape_markdown('关键词自动回复')}*\n"
        f"_{escape_markdown('当私聊消息包含您设置的关键词时，自动发送预设回复。')}_\n\n"
        f"4️⃣ *{escape_markdown('离线自动回复')}*\n"
        f"_{escape_markdown('当您不在线时，自动回复收到的第一条消息，避免怠慢。')}_\n\n"
        f"👇 *{escape_markdown('选择下方按钮管理您的功能:')}*"
    )
    
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("📒 管理账本", callback_data="premium:ledger"),
        types.InlineKeyboardButton("📝 关键词回复", callback_data="premium:keyword_reply")
    )
    markup.add(
        types.InlineKeyboardButton("🔍 账本分析", callback_data="premium:analyze"),
        types.InlineKeyboardButton("📊 账本统计", callback_data="premium:stats")
    )
    markup.add(
        types.InlineKeyboardButton("🌙 离线回复", callback_data="premium:offline_reply"),
        types.InlineKeyboardButton("🔙 返回主菜单", callback_data="start_menu")
    )
    
    if is_call:
        try:
            bot.edit_message_text(info_text, chat_id, message_id, reply_markup=markup, parse_mode="MarkdownV2")
        except ApiTelegramException:
            bot.answer_callback_query(message_or_call.id)
    else:
        bot.reply_to(message_or_call, info_text, reply_markup=markup, parse_mode="MarkdownV2")


@bot.message_handler(commands=['stats'])
@check_membership
def handle_stats(message):
    update_active_user(message.from_user.id)
    stats = collect_bot_stats()
    backend_status = '✅ 在线' if stats['telethon_connected'] else '❌ 离线'
    stats_text = (
        f"📊 *{escape_markdown('机器人状态概览')}*\n"
        f"*{'─' * 20}*\n"
        f"🟢 *{escape_markdown('在线用户:')}* `{stats['online_count']}` {escape_markdown('人')}\n"
        f"📡 *{escape_markdown('可达用户:')}* `{stats['interacted_users']}` {escape_markdown('人')}\n"
        f"👥 *{escape_markdown('总收录用户:')}* `{stats['total_users']}`\n"
        f"✉️ *{escape_markdown('总记录消息:')}* `{stats['total_messages']}`\n"
        f"🔄 *{escape_markdown('身份变更:')}* `{stats['identity_changes']}` {escape_markdown('次')}\n"
        f"📝 *{escape_markdown('已验证投稿:')}* `{stats['verified_reports']}` {escape_markdown('条')}\n"
        f"📺 *{escape_markdown('监控频道数:')}* `{stats['monitored_channels']}` {escape_markdown('个')}\n"
        f"⚙️ *{escape_markdown('后台引擎:')}* {escape_markdown(backend_status)}\n"
        f"`{escape_for_code(BOT_VERSION)}`\n"
        f"*{'─' * 20}*\n"
        f"{ADVERTISEMENT_TEXT}"
    )
    bot.reply_to(message, stats_text, disable_web_page_preview=True, parse_mode="MarkdownV2")

@bot.message_handler(commands=['admin', 'addchannel', 'removechannel', 'listchannels', 'delreport', 'broadcast', 'cancel_broadcast'])
@check_membership
def handle_admin_commands(message):
    is_admin = message.from_user.id == CONFIG["ADMIN_ID"]
    command_parts = message.text.split()
    command = command_parts[0]
    
    if not is_admin and command not in ['/listchannels']:
        bot.reply_to(message, escape_markdown("🚫 *无权限*"), parse_mode="MarkdownV2")
        return
        
    if command == '/admin':
        admin_text = (
            f"🛠️ *{escape_markdown('管理员控制面板')}*\n"
            f"*{'─' * 20}*\n"
            f"*{escape_markdown('频道管理')}*\n"
            f"`/addchannel <@频道/ID>`\n"
            f"`/removechannel <@频道/ID>`\n"
            f"`/listchannels`\n"
            f"_{escape_markdown('注: 添加后机器人会自动尝试加入')}_\n\n"
            f"*{escape_markdown('投稿管理')}*\n"
            f"`/delreport <用户ID/名>`\n\n"
            f"*{escape_markdown('广播功能')}*\n"
            f"`/broadcast`\n"
            f"`/cancel_broadcast`"
        )
        bot.reply_to(message, admin_text, parse_mode="MarkdownV2")

    elif command in ['/addchannel', '/removechannel']:
        if len(command_parts) < 2:
            bot.reply_to(message, escape_markdown("⚠️ 格式错误，请提供频道用户名或ID。"), parse_mode="MarkdownV2")
            return
        
        channel_input = command_parts[1].strip()
        target = None
        if channel_input.startswith('@'):
            target = channel_input
        elif channel_input.isdigit() or (channel_input.startswith('-') and channel_input[1:].isdigit()):
            try: target = int(channel_input)
            except ValueError:
                bot.reply_to(message, escape_markdown("⚠️ ID 格式错误。"), parse_mode="MarkdownV2")
                return
        else:
            bot.reply_to(message, escape_markdown("⚠️ 格式错误，频道应以@开头或为数字ID。"), parse_mode="MarkdownV2")
            return

        current_channels = load_channels()
        if command == '/addchannel':
            if any(str(c).lower() == str(target).lower() for c in current_channels):
                reply_text = f"{escape_markdown('ℹ️ 频道 ')}{format_inline_code(str(target))}{escape_markdown(' 已存在。')}"
                bot.reply_to(message, reply_text, parse_mode="MarkdownV2")
                return
            current_channels.append(target)
            if save_channels(current_channels):
                reply_text = f"✅ {escape_markdown('成功添加 ')}{format_inline_code(str(target))}{escape_markdown(' 到监控列表。')}"
                bot.reply_to(message, reply_text, parse_mode="MarkdownV2")
            else:
                bot.reply_to(message, escape_markdown(f"❌ 添加失败，无法写入文件。"), parse_mode="MarkdownV2")
        
        elif command == '/removechannel':
            original_len = len(current_channels)
            new_channels = [c for c in current_channels if str(c).lower() != str(target).lower()]
            if len(new_channels) < original_len:
                if save_channels(new_channels):
                    reply_text = f"✅ {escape_markdown('成功移除 ')}{format_inline_code(str(target))}{escape_markdown('。')}"
                    bot.reply_to(message, reply_text, parse_mode="MarkdownV2")
                else:
                    bot.reply_to(message, escape_markdown(f"❌ 移除失败，无法写入文件。"), parse_mode="MarkdownV2")
            else:
                reply_text = f"⚠️ {escape_markdown('未在列表中找到 ')}{format_inline_code(str(target))}{escape_markdown('。')}"
                bot.reply_to(message, reply_text, parse_mode="MarkdownV2")
    
    elif command == '/listchannels':
        current_channels = load_channels()
        if not current_channels:
            response_text = escape_markdown("ℹ️ 当前没有设置任何监控频道。")
        else:
            channels_text = "\n".join([f"📺 {format_inline_code(str(ch))}" for ch in current_channels])
            response_text = f"📝 *{escape_markdown('当前监控的频道/群组列表:')}*\n\n{channels_text}"
        bot.reply_to(message, response_text + f"\n\n{ADVERTISEMENT_TEXT}", parse_mode="MarkdownV2")

    elif command == '/delreport':
        if len(command_parts) < 2:
            bot.reply_to(message, escape_markdown("格式错误。用法: `/delreport <用户ID或用户名>`"), parse_mode="MarkdownV2")
            return

        query = command_parts[1].strip().lower().lstrip('@')
        reports = load_reports()
        key_to_delete = None

        if query.isdigit() and query in reports['verified']:
            key_to_delete = query
        else:
            for key, record in reports['verified'].items():
                if record.get('usernames') and query in [u.lower() for u in record.get('usernames', [])]:
                    key_to_delete = key
                    break
        
        if key_to_delete:
            del reports['verified'][key_to_delete]
            save_reports(reports)
            reply_text = f"✅ {escape_markdown('成功删除关于 ')}{format_inline_code(query)}{escape_markdown(' 的已验证报告。')}"
            bot.reply_to(message, reply_text, parse_mode="MarkdownV2")
        else:
            reply_text = f"⚠️ {escape_markdown('未在已验证报告中找到 ')}{format_inline_code(query)}{escape_markdown('。')}"
            bot.reply_to(message, reply_text, parse_mode="MarkdownV2")

    elif command == '/broadcast':
        handle_broadcast(message)
    elif command == '/cancel_broadcast':
        handle_cancel_broadcast(message)

# ---------------------- Broadcast Flow ----------------------
def handle_broadcast(message):
    admin_id = message.from_user.id
    if admin_id in admin_broadcast_state:
        bot.reply_to(message, escape_markdown("您当前有一个正在进行的广播任务。请先完成或使用 /cancel_broadcast 取消。"), parse_mode="MarkdownV2")
        return

    admin_broadcast_state[admin_id] = {"step": "awaiting_content"}
    prompt_text = (
        f"📢 *{escape_markdown('开始广播流程')}*\n\n"
        f"*{escape_markdown('第 1/2 步: 请发送您想要广播的完整消息（可以是文本、图片、视频等）。')}*\n\n"
        f"*{escape_markdown('随时可以发送')}* `/cancel_broadcast` *{escape_markdown('来中止。')}*"
    )
    bot.reply_to(message, prompt_text, parse_mode="MarkdownV2")
    bot.register_next_step_handler(message, process_broadcast_content)

def process_broadcast_content(message):
    admin_id = message.from_user.id
    if admin_id not in admin_broadcast_state:
        return

    if message.text and message.text.strip().lower() == '/cancel_broadcast':
        del admin_broadcast_state[admin_id]
        bot.reply_to(message, escape_markdown("✅ 广播已取消。"), parse_mode="MarkdownV2")
        return

    admin_broadcast_state[admin_id]['message_to_send'] = message
    admin_broadcast_state[admin_id]['step'] = 'awaiting_confirmation'

    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT COUNT(user_id) FROM bot_interactors")
        total_users = c.fetchone()[0]
        conn.close()

    confirmation_text = (
        f"❓ *{escape_markdown('第 2/2 步: 请确认广播')}*\n\n"
        f"{escape_markdown(f'此消息将被发送给 {total_users} 位曾与机器人私聊的用户。')}\n\n"
        f"*{escape_markdown('预览如下:')}*"
    )
    bot.send_message(admin_id, confirmation_text, parse_mode="MarkdownV2")
    bot.copy_message(chat_id=admin_id, from_chat_id=message.chat.id, message_id=message.message_id)

    final_prompt = (
        f"*{escape_markdown('如果确认无误，请输入')}* `CONFIRM` *{escape_markdown('来立即开始广播。')}*\n"
        f"{escape_markdown('输入任何其他内容或发送 /cancel_broadcast 将取消。')}"
    )
    bot.send_message(admin_id, final_prompt, parse_mode="MarkdownV2")
    bot.register_next_step_handler(message, execute_broadcast)

def execute_broadcast(message):
    admin_id = message.from_user.id
    if admin_id not in admin_broadcast_state or admin_broadcast_state[admin_id].get('step') != 'awaiting_confirmation':
        return

    if not message.text or message.text.strip() != 'CONFIRM':
        del admin_broadcast_state[admin_id]
        bot.reply_to(message, escape_markdown("❌ 操作已取消。广播未发送。"), parse_mode="MarkdownV2")
        return

    broadcast_data = admin_broadcast_state.pop(admin_id)
    message_to_send = broadcast_data['message_to_send']

    bot.reply_to(message, escape_markdown("✅ 确认成功！广播任务已在后台开始，完成后会通知您。"), parse_mode="MarkdownV2")

    def broadcast_thread_func():
        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT user_id FROM bot_interactors")
            all_user_ids = [row['user_id'] for row in c.fetchall()]
            conn.close()

        success_count = 0
        fail_count = 0
        start_time = time.time()

        for user_id in all_user_ids:
            try:
                bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=message_to_send.chat.id,
                    message_id=message_to_send.message_id
                )
                success_count += 1
            except ApiTelegramException as e:
                fail_count += 1
                if e.error_code not in [400, 403]:
                    print(f"[Broadcast] Failed to send to {user_id}: {e.description}")
            except Exception as e:
                fail_count += 1
                print(f"[Broadcast] Unknown error for {user_id}: {e}")
            time.sleep(0.1)

        end_time = time.time()
        duration = round(end_time - start_time, 2)
        
        summary_text = (
            f"🏁 *{escape_markdown('广播完成！')}*\n\n"
            f"⏱️ *{escape_markdown('总耗时:')}* `{duration}` {escape_markdown('秒')}\n"
            f"✅ *{escape_markdown('发送成功:')}* `{success_count}`\n"
            f"❌ *{escape_markdown('发送失败:')}* `{fail_count}`\n"
            f"👥 *{escape_markdown('总目标数:')}* `{len(all_user_ids)}`"
        )
        bot.send_message(admin_id, summary_text, parse_mode="MarkdownV2")

    threading.Thread(target=broadcast_thread_func, daemon=True).start()

def handle_cancel_broadcast(message):
    admin_id = message.from_user.id
    if admin_id == CONFIG['ADMIN_ID'] and admin_id in admin_broadcast_state:
        del admin_broadcast_state[admin_id]
        bot.reply_to(message, escape_markdown("✅ 当前的广播会话已成功取消。"), parse_mode="MarkdownV2")
    else:
        bot.reply_to(message, escape_markdown("ℹ️ 当前没有正在进行的广播任务。"), parse_mode="MarkdownV2")


# ---------------------- Query Flow (REFACTORED) ----------------------
@bot.message_handler(commands=['cxzbf'])
@check_membership
def handle_text_query(message):
    query = message.text[len('/cxzbf'):].strip()
    if not query:
        reply_text = f"⚠️ *{escape_markdown('请输入查询关键词')}*\n{escape_markdown('可以直接发送用户名或ID，或者使用命令：')}`/cxzbf @username`"
        bot.reply_to(message, reply_text, parse_mode="MarkdownV2")
        return
    trigger_query_flow(message, query)

@bot.message_handler(func=lambda m: m.chat.type == 'private' and m.text and not m.text.startswith('/') and (m.text.strip().isdigit() or (m.text.strip().startswith('@') and len(m.text.strip()) > 1)))
@check_membership
def handle_direct_query(message):
    query = message.text.strip()
    trigger_query_flow(message, query)

@bot.message_handler(func=lambda m: m.forward_from is not None, content_types=ALL_CONTENT_TYPES)
@check_membership
def handle_forwarded_query(message):
    sender = message.forward_from
    if sender.is_bot:
        bot.reply_to(message, escape_markdown("ℹ️ 无法查询机器人账户。"), parse_mode="MarkdownV2")
        return
    
    query_term = str(sender.id)
    trigger_query_flow(message, query_term)

@bot.message_handler(func=lambda m: m.forward_sender_name is not None, content_types=ALL_CONTENT_TYPES)
@check_membership
def handle_hidden_forward(message):
    update_active_user(message.from_user.id)
    response_text = (
        f"👤 *{escape_markdown('用户信息 | 隐藏转发')}*\n\n"
        f"{escape_markdown('此用户已隐藏其转发身份，无法获取其 Telegram ID，因此无法提供查询服务。')}"
    )
    bot.reply_to(message, response_text + f"\n\n{ADVERTISEMENT_TEXT}", parse_mode="MarkdownV2")


def build_query_report_summary(resolved_id, db_history, verified_info, scam_channel_hits, common_groups,
                               spoken_in_group_ids, phone_history, bio_history):
    summary = {
        'resolved_id': resolved_id,
        'risk_source': '官方验证投稿' if verified_info else ('反诈频道曝光' if scam_channel_hits else None),
        'profile': None,
        'business': {},
        'history': [],
        'scam_hits': [],
        'common_groups': [],
        'bio_history': [],
        'phone_history': [],
        'has_verified_report': bool(verified_info)
    }

    if db_history and db_history.get('current_profile'):
        profile = db_history['current_profile']
        display_name = (f"{profile.get('first_name') or ''} {profile.get('last_name') or ''}").strip()
        active_usernames = []
        if profile.get('active_usernames_json'):
            try:
                active_usernames = json.loads(profile['active_usernames_json'])
            except Exception:
                active_usernames = []

        summary['profile'] = {
            'user_id': db_history.get('user_id', resolved_id),
            'display_name': display_name or None,
            'usernames': active_usernames,
            'phone': profile.get('phone'),
            'bio': profile.get('bio'),
            'limited': False
        }

        business_info = {}
        business_bio = profile.get('business_bio')
        if business_bio:
            business_info['bio'] = business_bio

        if profile.get('business_location_json'):
            try:
                loc_data = json.loads(profile['business_location_json'])
                business_info['location'] = loc_data.get('address')
            except Exception:
                pass

        if profile.get('business_work_hours_json'):
            try:
                wh_data = json.loads(profile['business_work_hours_json'])
                periods = []
                for period in wh_data.get('periods', []):
                    start_hour, start_minute = divmod(period.get('start_minute', 0), 60)
                    end_hour, end_minute = divmod(period.get('end_minute', 0), 60)
                    periods.append(f"{start_hour:02d}:{start_minute:02d}-{end_hour:02d}:{end_minute:02d}")
                if periods:
                    business_info['work_hours'] = periods
                if wh_data.get('timezone_id'):
                    business_info['timezone'] = wh_data['timezone_id']
            except Exception:
                pass

        summary['business'] = business_info
    else:
        summary['profile'] = {
            'user_id': resolved_id,
            'limited': True
        }
        summary['business'] = {}

    profile_history = db_history.get('profile_history', []) if db_history else []
    for entry in profile_history:
        timestamp = entry.get('timestamp')
        display_time = None
        if timestamp:
            try:
                display_time = datetime.fromtimestamp(timestamp, tz=CHINA_TZ).strftime('%Y-%m-%d %H:%M')
            except Exception:
                display_time = None
        summary['history'].append({
            'timestamp': int(timestamp) if timestamp else None,
            'display_time': display_time,
            'name': entry.get('name'),
            'username': entry.get('username')
        })

    for hit in scam_channel_hits or []:
        if isinstance(hit, dict):
            summary['scam_hits'].append({
                'chat_title': hit.get('chat_title'),
                'link': hit.get('link')
            })

    groups_map = {}
    for group in common_groups or []:
        if not isinstance(group, dict):
            continue
        gid = group.get('id')
        if gid is None:
            continue
        usernames = [u for u in group.get('usernames', []) if u]
        groups_map[gid] = {
            'id': gid,
            'title': group.get('title'),
            'usernames': usernames
        }

    for chat_id in spoken_in_group_ids or []:
        if chat_id in groups_map:
            continue
        db_info = get_chat_info_from_db(chat_id)
        if db_info:
            usernames = [db_info['username']] if db_info.get('username') else []
            groups_map[chat_id] = {
                'id': chat_id,
                'title': db_info.get('title'),
                'usernames': usernames
            }

    common_groups_list = list(groups_map.values())
    summary['common_groups'] = sorted(common_groups_list, key=lambda g: (str(g.get('title') or '')).lower())
    for group in summary['common_groups']:
        group['is_private'] = not bool(group.get('usernames'))

    for entry in bio_history or []:
        if not isinstance(entry, dict):
            continue
        timestamp = entry.get('date') or entry.get('timestamp')
        display_date = None
        if timestamp:
            try:
                display_date = datetime.fromtimestamp(timestamp, tz=CHINA_TZ).strftime('%Y-%m-%d')
            except Exception:
                display_date = None
        summary['bio_history'].append({
            'timestamp': int(timestamp) if timestamp else None,
            'display_date': display_date,
            'bio': entry.get('bio')
        })

    summary['phone_history'] = [str(phone) for phone in (phone_history or []) if phone]

    return summary


def build_query_report_markdown(summary):
    parts = []
    risk_source = summary.get('risk_source')
    if risk_source:
        parts.append(
            f"🚨 *{escape_markdown('高风险警报')}* 🚨\n*{escape_markdown('风险来源:')}* {escape_markdown(risk_source)}"
        )

    profile = summary.get('profile') or {}
    if profile.get('limited'):
        header = f"👤 *{escape_markdown('用户资料 (信息受限)')}*"
        id_line = f"› *ID:* `{summary['resolved_id']}`"
        note_line = f"_{escape_markdown('注意：无法获取此用户的实时详细资料，可能因其隐私设置或已注销。')}_"
        parts.append(f"{header}\n{id_line}\n{note_line}")
    else:
        user_summary = [f"👤 *{escape_markdown('用户资料')}*"]
        user_summary.append(f"› *ID:* `{summary['resolved_id']}`")
        if profile.get('display_name'):
            user_summary.append(f"› *Name:* {escape_markdown(profile['display_name'])}")
        usernames = profile.get('usernames') or []
        if usernames:
            user_summary.append(
                f"› *Username:* {', '.join([f'@{escape_markdown(u)}' for u in usernames])}"
            )
        if profile.get('phone'):
            user_summary.append(f"› *Phone:* `{escape_for_code(profile['phone'])}`")
        if profile.get('bio'):
            user_summary.append(f"› *Bio:* {escape_markdown(profile['bio'])}")
        parts.append("\n".join(user_summary))

        business = summary.get('business') or {}
        business_parts = []
        if business.get('bio'):
            business_parts.append(f"› *简介:* {escape_markdown(business['bio'])}")
        if business.get('location'):
            business_parts.append(f"› *位置:* {escape_markdown(business['location'])}")
        if business.get('work_hours'):
            hours_text = ', '.join([escape_markdown(h) for h in business['work_hours']])
            if business.get('timezone'):
                hours_text += f" ({escape_markdown(business['timezone'])})"
            business_parts.append(f"› *时间:* {hours_text}")
        if business_parts:
            parts.append(f"🏢 *{escape_markdown('营业信息')}*\n" + "\n".join(business_parts))

    scam_hits = summary.get('scam_hits') or []
    if scam_hits:
        count_text = escape_markdown(f"({len(scam_hits)} 条)")
        risk_header = f"🔍 *{escape_markdown('风险记录')} {count_text}*"
        risk_parts = [risk_header]
        for hit in scam_hits:
            title = _sanitize_for_link_text(hit.get('chat_title') or '未知频道')
            link = hit.get('link') or ''
            risk_parts.append(f"› [{escape_markdown(title)}]({link})")
        parts.append("\n".join(risk_parts))

    history = summary.get('history') or []
    if len(history) > 1:
        history_count = escape_markdown(f"({len(history)} 条)")
        history_header = f"📜 *{escape_markdown('历史变动')} {history_count}*"
        event_blocks = []
        for event in history:
            formatted_time = escape_for_code(event.get('display_time') or '未知')
            name_str = escape_for_code(event.get('name') or '无')
            username = event.get('username')
            username_part = f"@{username}" if username else '无'
            username_str = escape_markdown(username_part)
            event_blocks.append(f"`{formatted_time}`\n › N: `{name_str}`\n › U: {username_str}")
        parts.append(history_header + "\n" + "\n\n".join(event_blocks))

    common_groups = summary.get('common_groups') or []
    if common_groups:
        group_count = escape_markdown(f"({len(common_groups)} 个)")
        group_header = f"👥 *{escape_markdown('共同群组')} {group_count}*"
        group_lines = []
        for group in common_groups:
            usernames = group.get('usernames') or []
            if usernames:
                username_text = " ".join([f"@{escape_markdown(u)}" for u in usernames])
            else:
                username_text = escape_markdown('[私密]')
            title = escape_markdown(group.get('title') or f"群组ID: {group.get('id')}")
            group_lines.append(f"› {username_text} - {title}")
        parts.append(group_header + "\n" + "\n".join(group_lines))

    bio_history = summary.get('bio_history') or []
    if bio_history:
        bio_count = escape_markdown(f"({len(bio_history)} 条)")
        bio_header = f"📝 *Bio {escape_markdown('历史')} {bio_count}*"
        lines = []
        for entry in bio_history:
            date_str = escape_for_code(entry.get('display_date') or '未知')
            bio_text = escape_for_code((entry.get('bio') or '').strip() or '空')
            lines.append(f"› `{date_str}`\n  `{bio_text}`")
        parts.append(bio_header + "\n" + "\n\n".join(lines))

    phone_history = summary.get('phone_history') or []
    if phone_history:
        phone_count = escape_markdown(f"({len(phone_history)} 个)")
        phone_header = f"📱 *{escape_markdown('绑定号码')} {phone_count}*"
        phone_lines = [f"› `{escape_for_code(phone)}`" for phone in phone_history]
        parts.append(phone_header + "\n" + "\n".join(phone_lines))

    return "\n\n".join(filter(None, parts))


def build_query_report_html(summary):
    escape = html.escape
    sections = ["<div class=\"report\">"]

    risk_source = summary.get('risk_source')
    if risk_source:
        sections.append(
            f"<section class=\"block risk\"><h3>高风险警报</h3><p>风险来源：{escape(risk_source)}</p></section>"
        )

    profile = summary.get('profile') or {}
    if profile.get('limited'):
        sections.append(
            f"<section class=\"block\"><h3>用户资料 (信息受限)</h3>"
            f"<p><strong>ID：</strong>{escape(str(summary['resolved_id']))}</p>"
            "<p class=\"muted\">注意：无法获取此用户的实时详细资料，可能因其隐私设置或已注销。</p></section>"
        )
    else:
        profile_lines = [
            f"<p><strong>ID：</strong>{escape(str(summary['resolved_id']))}</p>"
        ]
        if profile.get('display_name'):
            profile_lines.append(f"<p><strong>名称：</strong>{escape(profile['display_name'])}</p>")
        usernames = profile.get('usernames') or []
        if usernames:
            profile_lines.append(
                f"<p><strong>用户名：</strong>{'、'.join([escape('@' + u) for u in usernames])}</p>"
            )
        if profile.get('phone'):
            profile_lines.append(f"<p><strong>电话：</strong>{escape(profile['phone'])}</p>")
        if profile.get('bio'):
            profile_lines.append(f"<p><strong>签名：</strong>{escape(profile['bio'])}</p>")

        sections.append(
            "<section class=\"block\"><h3>用户资料</h3>" + "".join(profile_lines) + "</section>"
        )

        business = summary.get('business') or {}
        business_lines = []
        if business.get('bio'):
            business_lines.append(f"<p><strong>营业简介：</strong>{escape(business['bio'])}</p>")
        if business.get('location'):
            business_lines.append(f"<p><strong>营业地址：</strong>{escape(business['location'])}</p>")
        if business.get('work_hours'):
            hours_text = '、'.join([escape(h) for h in business['work_hours']])
            if business.get('timezone'):
                hours_text += f"（{escape(business['timezone'])}）"
            business_lines.append(f"<p><strong>营业时间：</strong>{hours_text}</p>")
        if business_lines:
            sections.append("<section class=\"block\"><h3>营业信息</h3>" + "".join(business_lines) + "</section>")

    scam_hits = summary.get('scam_hits') or []
    if scam_hits:
        hit_items = []
        for hit in scam_hits:
            title = escape(hit.get('chat_title') or '未知频道')
            link = hit.get('link') or '#'
            hit_items.append(f"<li><a href=\"{escape(link)}\" target=\"_blank\">{title}</a></li>")
        sections.append(
            "<section class=\"block\"><h3>风险记录</h3><ul>" + "".join(hit_items) + "</ul></section>"
        )

    history = summary.get('history') or []
    if history:
        rows = []
        for event in history:
            time_label = escape(event.get('display_time') or '未知')
            name_label = escape(event.get('name') or '无')
            username = event.get('username')
            username_label = escape('@' + username) if username else '无'
            rows.append(
                f"<tr><td>{time_label}</td><td>{name_label}</td><td>{username_label}</td></tr>"
            )
        sections.append(
            "<section class=\"block\"><h3>历史变动</h3>"
            "<table><thead><tr><th>时间</th><th>名称</th><th>用户名</th></tr></thead><tbody>"
            + "".join(rows) + "</tbody></table></section>"
        )

    common_groups = summary.get('common_groups') or []
    if common_groups:
        group_items = []
        for group in common_groups:
            usernames = group.get('usernames') or []
            if usernames:
                username_text = '、'.join([escape('@' + u) for u in usernames])
            else:
                username_text = '私密'
            title = escape(group.get('title') or f"群组ID: {group.get('id')}")
            group_items.append(f"<li><span>{username_text}</span> - {title}</li>")
        sections.append(
            "<section class=\"block\"><h3>共同群组</h3><ul>" + "".join(group_items) + "</ul></section>"
        )

    bio_history = summary.get('bio_history') or []
    if bio_history:
        bio_items = []
        for entry in bio_history:
            date_label = escape(entry.get('display_date') or '未知')
            bio_label = escape((entry.get('bio') or '').strip() or '空')
            bio_items.append(f"<li><strong>{date_label}</strong><br>{bio_label}</li>")
        sections.append(
            "<section class=\"block\"><h3>Bio 历史</h3><ul>" + "".join(bio_items) + "</ul></section>"
        )

    phone_history = summary.get('phone_history') or []
    if phone_history:
        phone_items = [f"<li>{escape(phone)}</li>" for phone in phone_history]
        sections.append(
            "<section class=\"block\"><h3>绑定号码</h3><ul>" + "".join(phone_items) + "</ul></section>"
        )

    sections.append(f"<section class=\"block ad-wrapper\">{get_advertisement_html()}</section>")
    sections.append("</div>")
    return "".join(sections)


def build_webapp_html():
    ad_html = get_advertisement_html()
    title = "猎诈卫士 · 安全情报控制台"
    version = html.escape(BOT_VERSION)
    current_year = datetime.now(CHINA_TZ).year
    return f"""<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(title)}</title>
<style>
:root {{ color-scheme: light dark; }}
body {{ margin:0; font-family:'Segoe UI','PingFang SC',sans-serif; background:#0f172a; color:#e2e8f0; }}
a {{ color:#38bdf8; text-decoration:none; }}
a:hover {{ text-decoration:underline; }}
.app {{ max-width:960px; margin:0 auto; padding:24px 16px 48px; }}
header {{ text-align:center; margin-bottom:32px; }}
.hero-title {{ font-size:2.2rem; margin-bottom:8px; font-weight:700; }}
.tagline {{ color:#94a3b8; margin:0; }}
.sections {{ display:grid; gap:24px; }}
.card {{ background:rgba(15,23,42,0.75); border:1px solid rgba(148,163,184,0.18); border-radius:18px; padding:20px; box-shadow:0 24px 60px rgba(15,23,42,0.55); backdrop-filter:blur(18px); }}
.card h2 {{ margin-top:0; font-size:1.3rem; font-weight:600; }}
.card p {{ margin-bottom:12px; line-height:1.6; }}
form {{ display:flex; flex-direction:column; gap:12px; }}
input {{ padding:12px 16px; border-radius:14px; border:1px solid rgba(148,163,184,0.25); background:rgba(15,23,42,0.35); color:inherit; font-size:1rem; box-shadow:inset 0 1px 0 rgba(255,255,255,0.05); }}
button {{ padding:12px 16px; border-radius:14px; border:none; background:linear-gradient(135deg,#38bdf8,#6366f1); color:#0f172a; font-weight:600; font-size:1rem; cursor:pointer; transition:transform 0.2s ease, box-shadow 0.2s ease; }}
button:hover {{ transform:translateY(-1px); box-shadow:0 14px 30px rgba(99,102,241,0.45); }}
.notice {{ margin-top:12px; padding:12px 16px; border-radius:14px; background:rgba(30,41,59,0.75); border:1px solid rgba(248,113,113,0.35); color:#fca5a5; display:none; }}
.notice.success {{ border-color:rgba(74,222,128,0.4); color:#86efac; }}
#queryResult {{ margin-top:18px; display:none; }}
.table {{ width:100%; border-collapse:collapse; margin-top:12px; font-size:0.95rem; }}
.table th, .table td {{ padding:10px 12px; border-bottom:1px solid rgba(148,163,184,0.15); text-align:left; }}
.list {{ list-style:none; padding-left:0; margin:0; }}
.list li {{ margin-bottom:8px; line-height:1.6; }}
.badge {{ display:inline-block; padding:4px 10px; border-radius:999px; background:rgba(56,189,248,0.15); color:#bae6fd; font-size:0.75rem; margin-right:8px; }}
.ad-section {{ margin-top:32px; text-align:center; }}
footer {{ margin-top:36px; text-align:center; color:#94a3b8; font-size:0.9rem; }}
@media (min-width:768px) {{ .sections {{ grid-template-columns:repeat(2, minmax(0, 1fr)); }} }}
</style>
</head>
<body>
<div class="app">
<header class="card">
    <h1 class="hero-title">{html.escape(title)}</h1>
    <p class="tagline">实时掌握诈骗风险情报，支持 Telegram 与网页双端体验。</p>
    <p class="tagline">当前版本：{version}</p>
</header>
<div class="sections">
    <section class="card">
        <h2>快速查询</h2>
        <p>输入 Telegram 用户名、ID 或粘贴目标信息，立即获取风险情报报告。</p>
        <form id="queryForm">
            <input id="queryInput" placeholder="例如：@username 或 123456789" required />
            <button type="submit">开始查询</button>
        </form>
        <div id="queryNotice" class="notice"></div>
        <div id="queryResult" class="card"></div>
    </section>
    <section class="card" id="statsCard">
        <h2>运行状态</h2>
        <div id="statsContent">正在加载统计数据...</div>
    </section>
    <section class="card" id="leaderboardCard">
        <h2>赞助排行榜</h2>
        <ol id="leaderboardList" class="list"></ol>
    </section>
    <section class="card">
        <h2>在线赞助</h2>
        <p>填写您的 Telegram 数字 ID 与赞助金额 (USDT)，即可生成 OKPay 支付链接。</p>
        <form id="sponsorForm">
            <input id="sponsorId" placeholder="Telegram 数字 ID" required />
            <input id="sponsorAmount" placeholder="赞助金额 (USDT)" required />
            <button type="submit">创建赞助订单</button>
        </form>
        <div id="sponsorNotice" class="notice"></div>
    </section>
</div>
<div class="card">
    <h2>使用提示</h2>
    <ul class="list">
        <li>📥 在 Telegram 内发送 <code>/start</code> 或 <code>/cxzbf</code> 依旧可使用机器人完整功能。</li>
        <li>🛡️ 网页版仅提供安全浏览与下单体验，敏感数据仍由机器人端安全处理。</li>
        <li>💬 如需人工协助，请通过下方联系方式联系我们。</li>
    </ul>
</div>
<div class="ad-section">{ad_html}</div>
<footer>© {current_year} 猎诈卫士 · 守护每一次安全对话。</footer>
</div>
<script>
const queryForm = document.getElementById('queryForm');
const queryInput = document.getElementById('queryInput');
const queryNotice = document.getElementById('queryNotice');
const queryResult = document.getElementById('queryResult');
const statsContent = document.getElementById('statsContent');
const leaderboardList = document.getElementById('leaderboardList');
const sponsorForm = document.getElementById('sponsorForm');
const sponsorNotice = document.getElementById('sponsorNotice');

function showNotice(element, message, isSuccess = false) {{
    element.textContent = message;
    element.className = isSuccess ? 'notice success' : 'notice';
    element.style.display = 'block';
}}

queryForm.addEventListener('submit', async (event) => {{
    event.preventDefault();
    const keyword = queryInput.value.trim();
    if (!keyword) return;
    showNotice(queryNotice, '正在检索，请稍候...');
    queryResult.style.display = 'none';
    try {{
        const response = await fetch('/api/query', {{
            method: 'POST',
            headers: {{ 'Content-Type': 'application/json' }},
            body: JSON.stringify({{ query: keyword }})
        }});
        const data = await response.json();
        if (!response.ok) {{
            throw new Error(data.message || '查询失败');
        }}
        if (data.status === 'success') {{
            queryResult.innerHTML = data.report_html;
            queryResult.style.display = 'block';
            showNotice(queryNotice, '查询成功，以下为详细报告。', true);
        }} else if (data.status === 'no_data') {{
            queryResult.style.display = 'none';
            showNotice(queryNotice, data.message || '已识别用户，但暂无相关记录。');
        }} else if (data.status === 'partial') {{
            const items = (data.partial_hits || []).map(hit => {{
                const link = hit.link ? `<a href="${{hit.link}}" target="_blank">${{hit.chat_title || '未知频道'}}</a>` : (hit.chat_title || '未知频道');
                return `<li>${{link}}</li>`;
            }}).join('');
            queryResult.innerHTML = `<div class="card"><h3>部分匹配结果</h3><p>未能直接识别该用户，以下为监控频道中的相关提及：</p><ul class="list">${{items}}</ul></div>`;
            queryResult.style.display = 'block';
            showNotice(queryNotice, '已找到相关线索，请确认是否为同一用户。');
        }} else if (data.status === 'not_found') {{
            queryResult.style.display = 'none';
            showNotice(queryNotice, data.message || '未找到任何相关记录。');
        }} else if (data.status === 'unavailable') {{
            queryResult.style.display = 'none';
            showNotice(queryNotice, data.message || '后台服务暂不可用，请稍后再试。');
        }} else {{
            queryResult.style.display = 'none';
            showNotice(queryNotice, data.message || '查询失败，请稍后重试。');
        }}
    }} catch (error) {{
        queryResult.style.display = 'none';
        showNotice(queryNotice, error.message || '查询失败，请稍后重试。');
    }}
}});

async function loadStats() {{
    try {{
        const response = await fetch('/api/stats');
        const data = await response.json();
        if (response.ok && data.status === 'success') {{
            const s = data.data;
            const engine = s.telethon_connected ? '✅ 在线' : '❌ 离线';
            statsContent.innerHTML = `
                <ul class="list">
                    <li>🟢 在线用户：<strong>${{s.online_count}}</strong></li>
                    <li>📡 可达用户：<strong>${{s.interacted_users}}</strong></li>
                    <li>👥 总收录用户：<strong>${{s.total_users}}</strong></li>
                    <li>✉️ 累计消息：<strong>${{s.total_messages}}</strong></li>
                    <li>🔄 身份变更记录：<strong>${{s.identity_changes}}</strong></li>
                    <li>📝 已验证投稿：<strong>${{s.verified_reports}}</strong></li>
                    <li>📺 监控频道数：<strong>${{s.monitored_channels}}</strong></li>
                    <li>⚙️ 后台状态：<strong>${{engine}}</strong></li>
                </ul>`;
        }} else {{
            statsContent.textContent = data.message || '暂时无法获取统计数据。';
        }}
    }} catch (error) {{
        statsContent.textContent = '统计数据加载失败，请稍后重试。';
    }}
}}

async function loadLeaderboard() {{
    try {{
        const response = await fetch('/api/leaderboard');
        const data = await response.json();
        if (response.ok && data.status === 'success') {{
            if (!data.sponsors || !data.sponsors.length) {{
                leaderboardList.innerHTML = '<li>暂无赞助记录，期待您的支持！</li>';
                return;
            }}
            leaderboardList.innerHTML = data.sponsors.map((item, index) => {{
                const rankIcon = index === 0 ? '🥇' : index === 1 ? '🥈' : index === 2 ? '🥉' : `${{index + 1}}.`;
                const name = item.display_name || `用户 ${{item.user_id}}`;
                return `<li>${{rankIcon}} ${name} <span class="badge">${{item.total_amount_usdt.toFixed(2)}} USDT</span></li>`;
            }}).join('');
        }} else {{
            leaderboardList.innerHTML = '<li>赞助数据暂不可用。</li>';
        }}
    }} catch (error) {{
        leaderboardList.innerHTML = '<li>排行榜加载失败。</li>';
    }}
}}

sponsorForm.addEventListener('submit', async (event) => {{
    event.preventDefault();
    const idValue = document.getElementById('sponsorId').value.trim();
    const amountValue = document.getElementById('sponsorAmount').value.trim();
    showNotice(sponsorNotice, '正在创建赞助订单，请稍候...');
    try {{
        const response = await fetch('/api/sponsor/order', {{
            method: 'POST',
            headers: {{ 'Content-Type': 'application/json' }},
            body: JSON.stringify({{ telegram_id: idValue, amount: amountValue }})
        }});
        const data = await response.json();
        if (response.ok && data.status === 'success') {{
            sponsorNotice.innerHTML = `✅ 订单创建成功：<strong>${{data.order_id}}</strong><br>请在 <a href="${{data.pay_url}}" target="_blank">OKPay 页面</a> 完成支付。`;
            sponsorNotice.className = 'notice success';
        }} else {{
            sponsorNotice.className = 'notice';
            sponsorNotice.textContent = data.message || '赞助订单创建失败，请稍后再试。';
        }}
    }} catch (error) {{
        sponsorNotice.className = 'notice';
        sponsorNotice.textContent = '赞助订单创建失败，请稍后重试。';
    }}
}});

loadStats();
loadLeaderboard();
</script>
</body>
</html>
"""


def collect_query_insights(query: str):
    try:
        query_cleaned = query.strip().lstrip('@')
        resolved_id = None
        user_to_sync = None

        if query_cleaned.isdigit():
            try:
                resolved_id = int(query_cleaned)
                print(f"ℹ️ [ID-Resolve] Query is numeric. Tentative ID: {resolved_id}")
            except (ValueError, TypeError):
                resolved_id = None

        try:
            entity_query = int(query_cleaned) if query_cleaned.isdigit() else query_cleaned
            future = asyncio.run_coroutine_threadsafe(client.get_entity(entity_query), telethon_loop)
            live_user = future.result(timeout=CONFIG["TELETHON_TIMEOUT"])
            if live_user and isinstance(live_user, User) and not live_user.bot:
                user_to_sync = live_user
                resolved_id = live_user.id
                print(f"✅ [ID-Resolve] API resolved '{query}' to ID: {resolved_id}")
        except (FuturesTimeoutError, TelethonTimeoutError):
            print(f"⚠️ [ID-Resolve] API lookup for '{query}' timed out.")
        except (ValueError, TypeError, UsernameInvalidError, PeerIdInvalidError):
            print(f"ℹ️ [ID-Resolve] API could not find user '{query}'.")
        except Exception as e:
            print(f"💥 [ID-Resolve] Unexpected error for '{query}': {e}")

        if not resolved_id:
            resolved_id = _resolve_historic_query_to_id(query)
            if resolved_id:
                print(f"✅ [ID-Resolve] Found ID {resolved_id} for '{query}' in historical DB.")

        if resolved_id:
            try:
                if not user_to_sync:
                    entity_future = asyncio.run_coroutine_threadsafe(client.get_entity(resolved_id), telethon_loop)
                    user_to_sync = entity_future.result(timeout=CONFIG["TELETHON_TIMEOUT"])
                if user_to_sync and isinstance(user_to_sync, User):
                    update_future = asyncio.run_coroutine_threadsafe(update_user_in_db(user_to_sync), telethon_loop)
                    update_future.result(timeout=CONFIG["TELETHON_TIMEOUT"])
                    print(f"✅ [Sync-Complete] DB synchronized for user {resolved_id}.")
            except Exception as e:
                print(f"⚠️ [Sync-Error] Sync failed for user {resolved_id}: {e}. Report will use existing/scanned data.")

            scam_channel_hits = []
            try:
                search_future = asyncio.run_coroutine_threadsafe(
                    search_monitored_channels_for_user(user_id=resolved_id), telethon_loop
                )
                scam_channel_hits = search_future.result(timeout=CONFIG["SCAM_CHANNEL_SEARCH_TIMEOUT"])
            except Exception as e:
                print(f"💥 [Scam-Scan] Error searching channels for user {resolved_id}: {type(e).__name__}")

            common_groups = []
            try:
                groups_future = asyncio.run_coroutine_threadsafe(
                    get_common_groups_with_user(resolved_id), telethon_loop
                )
                common_groups = groups_future.result(timeout=CONFIG["COMMON_GROUPS_TIMEOUT"])
            except Exception as e:
                print(f"💥 [Common-Groups] Error getting common groups for user {resolved_id}: {type(e).__name__}")

            db_history = query_user_history_from_db(resolved_id)
            phone_history = query_phone_history_from_db(resolved_id)
            bio_history = query_bio_history_from_db(resolved_id)
            spoken_in_group_ids = query_spoken_groups_from_db(resolved_id)
            reports = load_reports()
            verified_report = reports.get('verified', {}).get(str(resolved_id))

            if db_history or scam_channel_hits or verified_report:
                summary = build_query_report_summary(
                    resolved_id, db_history, verified_report, scam_channel_hits,
                    common_groups, spoken_in_group_ids, phone_history, bio_history
                )
                report_markdown = build_query_report_markdown(summary)
                return {
                    'status': 'full',
                    'resolved_id': resolved_id,
                    'summary': summary,
                    'report_markdown': report_markdown,
                    'verified_info': verified_report
                }

            return {
                'status': 'resolved_no_data',
                'resolved_id': resolved_id
            }

        partial_hits = []
        try:
            search_future = asyncio.run_coroutine_threadsafe(
                search_monitored_channels_for_user(raw_query=query), telethon_loop
            )
            partial_hits = search_future.result(timeout=CONFIG["SCAM_CHANNEL_SEARCH_TIMEOUT"])
        except Exception as e:
            print(f"💥 [Fallback-Scan] Channel search failed: {e}")

        sanitized_hits = [
            {'chat_title': hit.get('chat_title'), 'link': hit.get('link')}
            for hit in (partial_hits or []) if isinstance(hit, dict)
        ]
        if sanitized_hits:
            return {'status': 'partial_hits', 'partial_hits': sanitized_hits}

        return {'status': 'not_found'}

    except Exception as e:
        traceback.print_exc()
        return {
            'status': 'error',
            'error': str(e),
            'error_type': type(e).__name__
        }



def trigger_query_flow(message, query):
    update_active_user(message.from_user.id)

    if telethon_loop is None or not client.is_connected():
        bot.reply_to(message, f"⏳ *{escape_markdown('后台服务正在初始化...')}*\n{escape_markdown('请稍候几秒再试。')}", parse_mode="MarkdownV2")
        return

    waiting_message = None
    try:
        should_reply = not (message.text and message.text.startswith('/start bizChat'))
        reply_params = ReplyParameters(message_id=message.message_id, allow_sending_without_reply=True) if should_reply else None
        waiting_message = bot.send_message(
            message.chat.id,
            escape_markdown("⏳ 正在数据库中检索并同步最新资料... ⚡️"),
            reply_parameters=reply_params,
            parse_mode="MarkdownV2"
        )
    except Exception as e:
        print(f"⚠️ 发送等待消息失败: {e}")

    def perform_query_and_send_results():
        result = collect_query_insights(query)

        if waiting_message:
            try:
                bot.delete_message(waiting_message.chat.id, waiting_message.message_id)
            except Exception:
                pass

        status = result.get('status')

        if status == 'full':
            send_query_result(
                message=message,
                resolved_id=result['resolved_id'],
                report_markdown=result['report_markdown'],
                verified_info=result.get('verified_info')
            )
            return

        if status == 'resolved_no_data':
            reply_text = (
                f"📭 {escape_markdown('已识别用户ID ')}{format_inline_code(str(result['resolved_id']))}"
                f"{escape_markdown('，但未在其历史记录、官方投稿或监控频道中发现任何相关信息。')}"
            )
            bot.reply_to(message, reply_text, parse_mode="MarkdownV2")
            return


        if status == 'partial_hits':
            partial_hits = result.get('partial_hits', [])
            header = (
                f"⚠️ *{escape_markdown('部分匹配结果')}*\n"
                f"{escape_markdown('无法直接识别用户 ')}{format_inline_code(query)}"
                f"{escape_markdown('，可能因为对方隐私设置严格或已注销。')}\n\n"
                f"{escape_markdown('但是，我们在监控频道中找到了包含此ID或用户名的提及记录:')}"
            )
            partial_count = escape_markdown(f"({len(partial_hits)} 条)")
            risk_header = f"🔍 *{escape_markdown('风险记录')} {partial_count}*"
            risk_parts = [risk_header]
            for hit in partial_hits:
                title = _sanitize_for_link_text(hit.get('chat_title') or '未知频道')
                link = hit.get('link') or ''
                risk_parts.append(f"› [{escape_markdown(title)}]({link})")
            final_text = header + "\n\n" + "\n".join(risk_parts) + f"\n\n{ADVERTISEMENT_TEXT}"
            bot.reply_to(message, final_text, parse_mode="MarkdownV2", disable_web_page_preview=True)
            return


        if status == 'not_found':
            reply_text = (
                f"📭 {escape_markdown('未在数据库中找到与 ')}{format_inline_code(query)}"
                f"{escape_markdown(' 相关的任何用户记录，各监控频道中也无相关内容。此用户可能不存在或与诈骗无关。')}"
            )
            bot.reply_to(message, reply_text, parse_mode="MarkdownV2")
            return

        error_type = result.get('error_type') or 'Error'
        error_msg = result.get('error') or '未知错误'
        error_text = (
            f"❌ {escape_markdown('查询失败，请稍后重试或联系管理员。')}"
            f"\n`{escape_for_code(error_type)}`: {escape_markdown(error_msg)}"
        )
        bot.reply_to(message, error_text, parse_mode="MarkdownV2")

    threading.Thread(target=perform_query_and_send_results, daemon=True).start()





def send_query_result(message, resolved_id, report_markdown, verified_info):
    chat_id = message.chat.id
    main_text = (report_markdown or '').strip()
    if main_text:
        full_message = (
            f"{main_text}\n\n"
            "*────────────────────*\n"
            f"{ADVERTISEMENT_TEXT}"
        )
    else:
        full_message = ADVERTISEMENT_TEXT

    try:
        should_reply = not (message.text and message.text.startswith('/start bizChat'))
        reply_params = ReplyParameters(message_id=message.message_id, allow_sending_without_reply=True) if should_reply else None
        bot.send_message(
            chat_id,
            full_message,
            reply_parameters=reply_params,
            disable_web_page_preview=True,
            parse_mode="MarkdownV2"
        )
    except ApiTelegramException as e:
        if "message is too long" in str(e).lower():
            try:
                safe_main_text = re.sub(r'[_*`\\]', '', main_text)
                file_content = (
                    f"--- User Report for {resolved_id} ---\n\n"
                    f"{safe_main_text}"
                )
                file_to_send = io.BytesIO(file_content.encode('utf-8'))
                file_to_send.name = f"report_{resolved_id}.txt"
                summary_text = (
                    f"⚠️ *{escape_markdown('报告过长')}*\n"
                    f"{escape_markdown('详细报告已生成文件发送。')}\n\n"
                    f"{ADVERTISEMENT_TEXT}"
                )
                bot.send_message(chat_id, summary_text, parse_mode="MarkdownV2", disable_web_page_preview=True)
                bot.send_document(chat_id, file_to_send)
            except Exception as file_e:
                print(f"💥 创建或发送文件报告失败: {file_e}")
                error_text = f"❌ {escape_markdown('报告过长且无法生成文件。')}"
                bot.send_message(chat_id, error_text, parse_mode="MarkdownV2")
        elif "message is not modified" not in str(e).lower():
            safe_text = re.sub(r'[_*\[\]()~`>#+\-=|{}.!\]', '', full_message)
            fallback_message = (
                "⚠️ 报告包含特殊字符，无法以格式化形式发送。以下是纯文本版本：\n\n"
                f"{safe_text}"
            )[:4096]
            bot.send_message(chat_id, fallback_message, disable_web_page_preview=True, parse_mode=None)

    if verified_info and verified_info.get('evidence_messages'):
        notice_text = f"*{escape_markdown('以下是官方验证投稿的证据：')}*"
        bot.send_message(chat_id, notice_text, parse_mode="MarkdownV2")
        for ev in verified_info['evidence_messages']:
            try:
                bot.forward_message(chat_id, ev['chat_id'], ev['message_id'])
                time.sleep(0.5)
            except Exception as e:
                print(f"❌ 转发证据失败: {e}")
                bot.send_message(chat_id, f"_{escape_markdown('一份证据无法转发(可能已被删除)。')}_", parse_mode="MarkdownV2")

def handle_tougao(message):
    update_active_user(message.from_user.id)
    user_id = message.from_user.id
    if user_id in user_submission_state:
        bot.reply_to(message, escape_markdown("您当前有一个正在进行的投稿，请先完成或取消。"), parse_mode="MarkdownV2")
        return
    
    user_submission_state[user_id] = {"step": "awaiting_ids", "evidence": []}
    prompt_text = (
        f"✍️ *{escape_markdown('开始投稿流程')}*\n\n"
        f"*{escape_markdown('第 1/3 步: 请发送诈骗犯的 Telegram User ID。')}*\n"
        f"{escape_markdown('如果知道多个ID，请每行输入一个。如果不知道，请发送“无”。')}"
    )
    bot.reply_to(message, prompt_text, parse_mode="MarkdownV2")
    bot.register_next_step_handler(message, process_scammer_ids)

def process_scammer_ids(message):
    user_id = message.from_user.id
    if user_id not in user_submission_state: return

    ids_text = message.text.strip()
    if ids_text.lower() in ['无', 'none', 'null', '']:
        user_submission_state[user_id]['ids'] = []
    else:
        raw_ids = ids_text.splitlines()
        valid_ids = [line.strip() for line in raw_ids if line.strip().isdigit()]
        if len(valid_ids) != len(raw_ids):
            bot.reply_to(message, escape_markdown("⚠️ 输入包含了无效的ID格式，请只输入数字ID，每行一个。请重新开始 /tougao。"), parse_mode="MarkdownV2")
            del user_submission_state[user_id]
            return
        user_submission_state[user_id]['ids'] = valid_ids

    user_submission_state[user_id]['step'] = "awaiting_usernames"
    prompt_text = (
        f"✅ *{escape_markdown('ID 已收到。')}*\n\n"
        f"*{escape_markdown('第 2/3 步: 请发送诈骗犯的 Telegram 用户名 (不带@)。')}*\n"
        f"{escape_markdown('如果知道多个，请每行输入一个。如果不知道，请发送“无”。')}"
    )
    bot.reply_to(message, prompt_text, parse_mode="MarkdownV2")
    bot.register_next_step_handler(message, process_scammer_usernames)

def process_scammer_usernames(message):
    user_id = message.from_user.id
    if user_id not in user_submission_state: return

    usernames_text = message.text.strip()
    if usernames_text.lower() in ['无', 'none', 'null', '']:
        user_submission_state[user_id]['usernames'] = []
    else:
        user_submission_state[user_id]['usernames'] = [u.strip().lstrip('@') for u in usernames_text.splitlines()]

    if not user_submission_state[user_id]['ids'] and not user_submission_state[user_id]['usernames']:
        bot.reply_to(message, escape_markdown("❌ 错误：您必须至少提供一个ID或用户名。请使用 /tougao 重新开始。"), parse_mode="MarkdownV2")
        del user_submission_state[user_id]
        return
        
    user_submission_state[user_id]['step'] = "awaiting_evidence"
    prompt_text = (
        f"✅ *{escape_markdown('用户名已收到。')}*\n\n"
        f"*{escape_markdown('第 3/3 步: 请发送所有相关证据。')}*\n"
        f"{escape_markdown('这可以是文字说明、截图、聊天记录文件等。')}\n\n"
        f"*{escape_markdown('发送完所有证据后，请务必发送')}* `{escape_markdown(DONE_SUBMISSION_COMMAND)}` *{escape_markdown('来完成投稿。')}*"
    )
    bot.reply_to(message, prompt_text, parse_mode="MarkdownV2")
    bot.register_next_step_handler(message, process_evidence)

def process_evidence(message):
    user_id = message.from_user.id
    if user_id not in user_submission_state or user_submission_state[user_id]['step'] != "awaiting_evidence":
        handle_all_other_messages(message)
        return

    if message.text and message.text.strip().lower() == DONE_SUBMISSION_COMMAND:
        finalize_submission(message)
        return

    user_submission_state[user_id]['evidence'].append(message)
    bot.register_next_step_handler(message, process_evidence)

def finalize_submission(message):
    user_id = message.from_user.id
    if user_id not in user_submission_state: return
    submission_data = user_submission_state[user_id]
    if not submission_data['evidence']:
        bot.reply_to(message, escape_markdown("⚠️ 您没有提交任何证据。投稿至少需要一条证据。请发送证据后再发送 /done。"), parse_mode="MarkdownV2")
        bot.register_next_step_handler(message, process_evidence)
        return

    submission_id = str(uuid.uuid4())
    
    serialized_data = [serialize_message(msg) for msg in submission_data['evidence']]
    
    pending_submission = {
        "submitter_id": user_id, "submitter_username": message.from_user.username,
        "submission_time": time.time(), "user_ids": submission_data['ids'],
        "usernames": submission_data['usernames'],
        "evidence_messages": [{"chat_id": msg.chat.id, "message_id": msg.message_id} for msg in submission_data['evidence']],
        "evidence_data": [ev for ev in serialized_data if ev]
    }
    
    reports = load_reports()
    reports['pending'][submission_id] = pending_submission
    save_reports(reports)
    del user_submission_state[user_id]

    bot.reply_to(message, escape_markdown("✅ 您的投稿已成功提交，正在等待管理员审核。感谢您的贡献！"), parse_mode="MarkdownV2")

    ids_str = ", ".join(submission_data['ids']) or "无"
    users_str = ", ".join([f"@{u}" for u in submission_data['usernames']]) or "无"
    submitter_name = (message.from_user.first_name or "") + (" " + (message.from_user.last_name or "") if message.from_user.last_name else "")
    
    submitter_name_trunc = truncate_for_link_text(submitter_name or '用户')
    sanitized_submitter_name = _sanitize_for_link_text(submitter_name_trunc)
    escaped_submitter_name = escape_markdown(sanitized_submitter_name)
    
    admin_text = (
        f"📢 *{escape_markdown('新的诈骗者投稿待审核')}*\n\n"
        f"› *{escape_markdown('投稿人:')}* [{escaped_submitter_name}](tg://user?id={user_id}) \\| `{user_id}`\n"
        f"› *{escape_markdown('诈骗犯ID:')}* `{escape_for_code(ids_str)}`\n"
        f"› *{escape_markdown('诈骗犯用户:')}* `{escape_for_code(users_str)}`\n\n"
        f"*{escape_markdown('提交的证据如下:')}*"
    )
    
    try:
        bot.send_message(CONFIG["ADMIN_ID"], admin_text, parse_mode="MarkdownV2")
        for msg in submission_data['evidence']:
            bot.forward_message(CONFIG["ADMIN_ID"], msg.chat.id, msg.message_id)
        
        markup = types.InlineKeyboardMarkup()
        approve_btn = types.InlineKeyboardButton("✅ 批准", callback_data=f"approve_sub:{submission_id}")
        reject_btn = types.InlineKeyboardButton("❌ 拒绝", callback_data=f"reject_sub:{submission_id}")
        markup.add(approve_btn, reject_btn)
        bot.send_message(CONFIG["ADMIN_ID"], "请审核以上投稿：", reply_markup=markup, parse_mode=None)
    except Exception as e:
        print(f"💥 发送投稿给管理员失败: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith('approve_sub:') or call.data.startswith('reject_sub:'))
def handle_submission_review(call):
    if call.from_user.id != CONFIG["ADMIN_ID"]:
        bot.answer_callback_query(call.id, "无权限操作")
        return

    action, submission_id = call.data.split(':', 1)
    reports = load_reports()
    
    if submission_id not in reports['pending']:
        bot.answer_callback_query(call.id, "此投稿已被处理或不存在。")
        bot.edit_message_text("此投稿已被处理。", call.message.chat.id, call.message.message_id, reply_markup=None, parse_mode=None)
        return

    submission = reports['pending'][submission_id]
    submitter_id = submission['submitter_id']

    if action == 'approve_sub':
        all_ids = submission.get('user_ids', [])
        all_users = submission.get('usernames', [])
        
        if not all_ids and not all_users:
            bot.edit_message_text("❌ 批准失败：投稿缺少用户ID和用户名。", call.message.chat.id, call.message.message_id, parse_mode=None)
            bot.answer_callback_query(call.id, "批准失败")
            return

        primary_key = str(all_ids[0]) if all_ids else all_users[0].lower()
        
        verified_entry = {
            "user_ids": all_ids, "usernames": all_users,
            "evidence_messages": submission.get('evidence_messages', []),
            "evidence_data": submission.get('evidence_data', []),
            "submitter_id": submitter_id,
            "approval_time": time.time(), "approved_by": call.from_user.id
        }
        reports['verified'][primary_key] = verified_entry
        del reports['pending'][submission_id]
        save_reports(reports)
        
        edit_text = f"✅ {escape_markdown('已批准投稿 ')}{format_inline_code(primary_key)}{escape_markdown('。')}"
        bot.edit_message_text(edit_text, call.message.chat.id, call.message.message_id, reply_markup=None, parse_mode="MarkdownV2")
        bot.answer_callback_query(call.id, "已批准")
        try:
            notify_text = f"🎉 *{escape_markdown('投稿已批准')}*\n{escape_markdown('好消息！您提交的关于')} {format_inline_code(primary_key)} {escape_markdown('的投稿已被管理员批准。感谢您的贡献！')}"
            bot.send_message(submitter_id, notify_text, parse_mode="MarkdownV2")
        except Exception as e:
            print(f"通知用户 {submitter_id} 批准失败: {e}")

    elif action == 'reject_sub':
        bot.answer_callback_query(call.id)
        prompt_text = (
            f"❗️ *{escape_markdown('您已选择拒绝此投稿。')}*\n\n"
            f"*{escape_markdown('请直接回复此消息，输入拒绝的理由。')}*\n"
            f"*{escape_markdown('如果不想提供理由，请发送')}* `/skip`"
        )
        markup = types.ForceReply(selective=False, input_field_placeholder="输入拒绝理由...")
        prompt_message = bot.send_message(call.message.chat.id, prompt_text, reply_markup=markup, parse_mode="MarkdownV2")
        
        bot.register_next_step_handler(prompt_message, process_rejection_reason, submission_id, call.message.message_id)
        
        try:
            bot.edit_message_text("⏳ 等待输入拒绝理由...", call.message.chat.id, call.message.message_id, reply_markup=None, parse_mode="MarkdownV2")
        except Exception:
            pass

def process_rejection_reason(message, submission_id, original_review_message_id):
    reason = message.text.strip()
    reports = load_reports()
    
    if submission_id not in reports['pending']:
        bot.reply_to(message, "此投稿似乎已被其他操作处理。", parse_mode=None)
        return

    submission = reports['pending'][submission_id]
    submitter_id = submission['submitter_id']

    del reports['pending'][submission_id]
    save_reports(reports)

    rejection_text_for_admin = f"❌ {escape_markdown('已拒绝投稿 | ID: ')}`{escape_for_code(f'{submission_id[:8]}...')}`"
    user_notification = f"很遗憾，您提交的投稿已被管理员拒绝。"
    
    if reason.lower() != '/skip':
        rejection_text_for_admin += f"\n*{escape_markdown('原因:')}* {escape_markdown(reason)}"
        user_notification += f"\n\n*{escape_markdown('管理员留言:')}* {escape_markdown(reason)}"
        
    bot.reply_to(message, "操作完成。", parse_mode=None)
    
    try:
        bot.edit_message_text(
            rejection_text_for_admin,
            message.chat.id,
            original_review_message_id,
            parse_mode="MarkdownV2"
        )
    except Exception as e:
        print(f"编辑最终拒绝消息失败: {e}")
        
    try:
        bot.send_message(submitter_id, user_notification, parse_mode="MarkdownV2")
    except Exception as e:
        print(f"通知用户 {submitter_id} 拒绝失败: {e}")

# ====================================================================
# START OF BUSINESS LEDGER LOGIC
# ====================================================================

def get_or_create_ledger(user_id: int, contact_id: int):
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT * FROM business_ledgers WHERE user_id = ? AND contact_id = ?", (user_id, contact_id))
        ledger = c.fetchone()
        if not ledger:
            c.execute("SELECT currency, auto_pin FROM business_ledgers WHERE user_id = ? AND contact_id = 0", (user_id,))
            global_settings = c.fetchone()
            
            if not global_settings:
                c.execute("INSERT INTO business_ledgers (user_id, contact_id) VALUES (?, 0)", (user_id,))
                conn.commit()
                global_currency = '$'
                global_auto_pin = 0
            else:
                global_currency = global_settings['currency']
                global_auto_pin = global_settings['auto_pin']

            c.execute("INSERT INTO business_ledgers (user_id, contact_id, currency, auto_pin) VALUES (?, ?, ?, ?)", (user_id, contact_id, global_currency, global_auto_pin))
            conn.commit()
            c.execute("SELECT * FROM business_ledgers WHERE user_id = ? AND contact_id = ?", (user_id, contact_id))
            ledger = c.fetchone()
        conn.close()
    return ledger

def handle_ledger_command(message: types.Message):
    user_id = message.from_user.id
    contact_id = message.chat.id
    text = message.text.strip()
    
    ledger = get_or_create_ledger(user_id, contact_id)
    currency = ledger['currency']
    
    if text.startswith(('+', '-')):
        match = re.match(r'([+\-])\s*([\d,.]+)\s*(.*)', text, re.DOTALL)
        if not match: return 
        
        sign, amount_str, description_text = match.groups()
        
        description = None
        if message.reply_to_message and (message.reply_to_message.text or message.reply_to_message.caption):
             description = (message.reply_to_message.text or message.reply_to_message.caption).strip()
        else:
             description = description_text.strip() or None
        
        try:
            amount = float(amount_str.replace(',', ''))
            if amount < 0: return
        except ValueError: return
            
        if sign == '-': amount = -amount

        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("UPDATE business_ledgers SET balance = balance + ? WHERE id = ?", (amount, ledger['id']))
            conn.commit()
            
            c.execute("SELECT balance FROM business_ledgers WHERE id = ?", (ledger['id'],))
            new_balance = c.fetchone()['balance']
            
            c.execute(
                "INSERT INTO ledger_history (ledger_id, timestamp, amount, new_balance, description) VALUES (?, ?, ?, ?, ?)",
                (ledger['id'], int(time.time()), amount, new_balance, description)
            )
            conn.commit()
            conn.close()

        action_text = "入金" if amount > 0 else "出金"
        response_text = f"{action_text} {currency} {abs(amount):.2f}, 剩余 {currency} {new_balance:.2f}"
        
        if description:
            response_text += f"\n备注: {description}"

        sent_message = bot.send_message(
            chat_id=contact_id,
            text=response_text,
            business_connection_id=message.business_connection_id,
            reply_to_message_id=message.message_id,
            parse_mode=None
        )

        if ledger['auto_pin']:
            try:
                bot.pin_chat_message(
                    chat_id=contact_id,
                    message_id=sent_message.message_id,
                    disable_notification=True,
                    business_connection_id=message.business_connection_id
                )
            except ApiTelegramException as e:
                print(f"💥 [Ledger-AutoPin] Failed to auto-pin message: {e.description}")

    elif text == '//':
        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT balance FROM business_ledgers WHERE id = ?", (ledger['id'],))
            old_balance = c.fetchone()['balance']
            
            c.execute("UPDATE business_ledgers SET balance = 0.0 WHERE id = ?", (ledger['id'],))
            c.execute(
                "INSERT INTO ledger_history (ledger_id, timestamp, amount, new_balance, description) VALUES (?, ?, ?, ?, ?)",
                (ledger['id'], int(time.time()), -old_balance, 0.0, "清账成功")
            )
            conn.commit()
            conn.close()

        response_text = f"清账成功, 剩余 {currency} 0.00"
        bot.send_message(
            chat_id=contact_id,
            text=response_text,
            business_connection_id=message.business_connection_id,
            reply_to_message_id=message.message_id,
            parse_mode=None
        )

    elif text.lower().startswith('/l'):
        parts = text.lower().replace('/l', '').strip().split(':')
        limit = 5
        offset = 0
        try:
            if len(parts) == 1 and parts[0]: limit = int(parts[0])
            elif len(parts) == 2:
                limit = int(parts[0]) if parts[0] else 5
                offset = int(parts[1]) if parts[1] else 0
        except (ValueError, IndexError): pass

        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute(
                "SELECT * FROM ledger_history WHERE ledger_id = ? ORDER BY timestamp DESC LIMIT ? OFFSET ?",
                (ledger['id'], limit, offset)
            )
            history_rows = c.fetchall()
            conn.close()
        
        if not history_rows:
            bot.send_message(contact_id, "没有找到记账记录。", business_connection_id=message.business_connection_id, parse_mode=None)
            return

        history_text_parts = [f"最近 {len(history_rows)} 条记录:"]
        for row in history_rows:
            date_str = datetime.fromtimestamp(row['timestamp'], tz=CHINA_TZ).strftime('%m-%d %H:%M')
            action = "入金" if row['amount'] > 0 else "出金"
            desc_part = f" \\- {escape_markdown(row['description'] or '')}" if row['description'] else ""
            entry = f"`{escape_for_code(date_str)}` {action} `{currency} {abs(row['amount']):.2f}`{desc_part}\n  *剩余:* `{currency} {row['new_balance']:.2f}`"
            history_text_parts.append(entry)
            
        bot.send_message(
            contact_id,
            "\n".join(history_text_parts),
            business_connection_id=message.business_connection_id,
            parse_mode="MarkdownV2"
        )

    elif text.lower() in ['/$', '/¥', '/usdt', '/default']:
        new_currency = '$'
        if text.lower() == '/¥': new_currency = '¥'
        if text.lower() == '/usdt': new_currency = 'USDT'
        
        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("UPDATE business_ledgers SET currency = ? WHERE id = ?", (new_currency, ledger['id']))
            conn.commit()
            conn.close()
        
        bot.send_message(
            chat_id=contact_id,
            text=f"此对话货币单位已切换为: {new_currency}",
            business_connection_id=message.business_connection_id,
            parse_mode=None
        )

@bot.message_handler(commands=['jz'])
@check_membership
@premium_only
def show_ledger_settings_command(message: types.Message):
    show_ledger_settings(message)

def show_ledger_settings(message_or_call):
    is_call = isinstance(message_or_call, types.CallbackQuery)
    message = message_or_call.message if is_call else message_or_call
    user_id = message_or_call.from_user.id
    chat_id = message.chat.id
    message_id = message.message_id
    
    if chat_id < 0 and not is_call:
        bot.reply_to(message, "请在与我的私聊中使用此命令打开设置面板。", parse_mode=None)
        return

    ledger = get_or_create_ledger(user_id, 0)
    
    settings_text = (
        f"*{escape_markdown('您正在设置记录账本')}*\n\n"
        f"> 📝 *{escape_markdown('命令说明')}*\n"
        f"> `{escape_for_code('±金额[备注]')}`\n"
        f"> {escape_markdown('金额前使用’+’表示入金,’-’表示出金;如需备注,请在金额后加一个空格并附上说明文字。')}\n"
        f"> \n"
        f"> `{escape_for_code('/l[显示条数]:[起始位置]')}`\n"
        f"> {escape_markdown('默认不加数字时显示最近5条记录;可通过指定数字调整显示的条数和起始位置,例如/l10:3表示从第3条记录之后开始,显示10条记录')}\n"
        f"> \n"
        f"> `{escape_for_code('//')}`\n"
        f"> {escape_markdown('金额归零。')}\n"
        f"> \n"
        f"> `{escape_for_code('/货币[*]')}`\n"
        f"> {escape_markdown('切换对话中的货币单位,支持的选项包括:/usdt、/$、/¥,使用/default可恢复默认货币单位。')}\n\n"
        f"💴 *{escape_markdown('货币单位')}*\n"
        f"> `{escape_for_code(ledger['currency'])}`\n\n"
        f"⚙️ *{escape_markdown('高级选项')}*\n"
        f"> {'🟢' if ledger['auto_pin'] else '🔴'} {escape_markdown('自动置顶')}\n"
        f"> {escape_markdown('开启后, 出入金时会自动置顶消息。')}"
    )

    markup = types.InlineKeyboardMarkup(row_width=3)
    
    auto_pin_status_text = f"{'🟢 已开启' if ledger['auto_pin'] else '🔴 已关闭'}"
    markup.add(
        types.InlineKeyboardButton(auto_pin_status_text, callback_data="ledger_set:toggle_pin"),
        types.InlineKeyboardButton("自动置顶", callback_data="ledger_set:toggle_pin")
    )
    
    currency_btns = []
    for c in ['$', '¥', 'USDT']:
        text = f"⦿ {c}" if ledger['currency'] == c else c
        currency_btns.append(types.InlineKeyboardButton(text, callback_data=f"ledger_set:currency:{c}"))

    markup.row(*currency_btns)
    
    markup.add(types.InlineKeyboardButton("↩️ 返回", callback_data="premium:main"))
    
    if is_call:
        try:
            bot.edit_message_text(settings_text, chat_id, message_id, reply_markup=markup, parse_mode="MarkdownV2")
        except ApiTelegramException as e:
            if "message is not modified" not in str(e).lower(): print(f"Error editing ledger settings: {e}")
    else:
        bot.reply_to(message, settings_text, reply_markup=markup, parse_mode="MarkdownV2")


@bot.callback_query_handler(func=lambda call: call.data.startswith('ledger_set:'))
@check_membership
@premium_only
def handle_ledger_settings_callback(call: types.CallbackQuery):
    user_id = call.from_user.id
    parts = call.data.split(':')
    action = parts[1]
    
    bot.answer_callback_query(call.id)

    if action == "dummy":
        return

    ledger = get_or_create_ledger(user_id, 0)
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        if action == "toggle_pin":
            new_status = not ledger['auto_pin']
            c.execute("UPDATE business_ledgers SET auto_pin = ? WHERE user_id = ?", (new_status, user_id))
        elif action == "currency":
            new_currency = parts[2]
            c.execute("UPDATE business_ledgers SET currency = ? WHERE user_id = ?", (new_currency, user_id))
        conn.commit()
        conn.close()

    show_ledger_settings(call)

# ====================================================================
# END OF BUSINESS LEDGER LOGIC
# ====================================================================

# ====================================================================
# START OF LEDGER ANALYSIS AND STATS LOGIC
# ====================================================================

def query_ledger_stats(user_id: int, contact_id: int = None, start_time: int = None):
    stats = {'income': 0.0, 'outcome': 0.0, 'count': 0}
    
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        
        base_query = """
            SELECT 
                SUM(CASE WHEN lh.amount > 0 THEN lh.amount ELSE 0 END) as income,
                SUM(CASE WHEN lh.amount < 0 THEN lh.amount ELSE 0 END) as outcome,
                COUNT(lh.id) as count
            FROM ledger_history lh
            JOIN business_ledgers bl ON lh.ledger_id = bl.id
            WHERE bl.user_id = ?
        """
        params = [user_id]

        if contact_id is not None:
            base_query += " AND bl.contact_id = ?"
            params.append(contact_id)
        else:
            base_query += " AND bl.contact_id != 0"

        if start_time is not None:
            base_query += " AND lh.timestamp >= ?"
            params.append(start_time)

        c.execute(base_query, tuple(params))
        result = c.fetchone()
        conn.close()

        if result and result['count'] > 0:
            stats['income'] = result['income'] or 0.0
            stats['outcome'] = abs(result['outcome'] or 0.0)
            stats['count'] = result['count']

    return stats

def show_ledger_stats_menu(call: types.CallbackQuery, page=0):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    message_id = call.message.message_id
    
    overall_stats = query_ledger_stats(user_id)
    net = overall_stats['income'] - overall_stats['outcome']
    
    text_parts = [
        f"📊 *{escape_markdown('账本统计总览')}*",
        f"*{'─'*20}*",
        f"🟢 *{escape_markdown('总收入:')}* `{overall_stats['income']:.2f}`",
        f"🔴 *{escape_markdown('总支出:')}* `{overall_stats['outcome']:.2f}`",
        f"🔵 *{escape_markdown('净利润:')}* `{'%.2f' % net}`",
        f"🔄 *{escape_markdown('总笔数:')}* `{overall_stats['count']}`",
        f"*{'─'*20}*",
        f"{escape_markdown('以下是按聊天伙伴的独立统计:')}"
    ]
    
    markup = types.InlineKeyboardMarkup(row_width=1)
    
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT contact_id, balance, currency FROM business_ledgers WHERE user_id = ? AND contact_id != 0 ORDER BY id DESC", (user_id,))
        ledgers = c.fetchall()
        conn.close()

    if not ledgers:
        text_parts.append(f"\n_{escape_markdown('暂无与任何人的独立账本记录。')}_")
    
    page_size = 5
    start = page * page_size
    end = start + page_size
    
    for ledger in ledgers[start:end]:
        contact_name = _get_user_name_from_db(ledger['contact_id'])
        sanitized_name = escape_markdown(truncate_for_link_text(contact_name, 30))
        btn_text = f"{sanitized_name} ({ledger['currency']} {ledger['balance']:.2f})"
        markup.add(types.InlineKeyboardButton(btn_text, callback_data=f"ledger_stats_contact:{ledger['contact_id']}"))

    nav_buttons = []
    if page > 0:
        nav_buttons.append(types.InlineKeyboardButton("⬅️ 上一页", callback_data=f"ledger_stats_page:{page-1}"))
    if end < len(ledgers):
        nav_buttons.append(types.InlineKeyboardButton("下一页 ➡️", callback_data=f"ledger_stats_page:{page+1}"))
    if nav_buttons:
        markup.row(*nav_buttons)

    markup.add(types.InlineKeyboardButton("🔙 返回高级功能", callback_data="premium:main"))
    
    try:
        bot.edit_message_text("\n".join(text_parts), chat_id, message_id, reply_markup=markup, parse_mode="MarkdownV2")
    except ApiTelegramException as e:
        if "message is not modified" not in str(e).lower():
            print(f"Error in show_ledger_stats_menu: {e}")

def show_ledger_analysis_menu(call: types.CallbackQuery):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    message_id = call.message.message_id
    
    now = datetime.now(CHINA_TZ)
    
    seven_days_ago = int((now - timedelta(days=7)).timestamp())
    stats_7d = query_ledger_stats(user_id, start_time=seven_days_ago)
    
    thirty_days_ago = int((now - timedelta(days=30)).timestamp())
    stats_30d = query_ledger_stats(user_id, start_time=thirty_days_ago)
    
    start_of_month = int(now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).timestamp())
    stats_month = query_ledger_stats(user_id, start_time=start_of_month)

    text = (
        f"🔍 *{escape_markdown('账本数据分析(所有聊天汇总)')}*\n\n"
        f"*{escape_markdown('近7日:')}*\n"
        f"> 🟢 *{escape_markdown('收入:')}* `{stats_7d['income']:.2f}`\n"
        f"> 🔴 *{escape_markdown('支出:')}* `{stats_7d['outcome']:.2f}`\n"
        f"> 🔄 *{escape_markdown('笔数:')}* `{stats_7d['count']}`\n\n"
        f"*{escape_markdown('近30日:')}*\n"
        f"> 🟢 *{escape_markdown('收入:')}* `{stats_30d['income']:.2f}`\n"
        f"> 🔴 *{escape_markdown('支出:')}* `{stats_30d['outcome']:.2f}`\n"
        f"> 🔄 *{escape_markdown('笔数:')}* `{stats_30d['count']}`\n\n"
        f"*{escape_markdown('本月 (' + str(now.month) + '月):')}*\n"
        f"> 🟢 *{escape_markdown('收入:')}* `{stats_month['income']:.2f}`\n"
        f"> 🔴 *{escape_markdown('支出:')}* `{stats_month['outcome']:.2f}`\n"
        f"> 🔄 *{escape_markdown('笔数:')}* `{stats_month['count']}`"
    )

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🔄 刷新数据", callback_data="premium:analyze"))
    markup.add(types.InlineKeyboardButton("🔙 返回高级功能", callback_data="premium:main"))
    
    try:
        bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode="MarkdownV2")
    except ApiTelegramException as e:
        if "message is not modified" not in str(e).lower():
            print(f"Error in show_ledger_analysis_menu: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith('ledger_stats_'))
@check_membership
@premium_only
def handle_ledger_stats_callbacks(call: types.CallbackQuery):
    user_id = call.from_user.id
    action, payload = call.data.split(':', 1)

    if action == "ledger_stats_page":
        page = int(payload)
        show_ledger_stats_menu(call, page=page)
    elif action == "ledger_stats_contact":
        contact_id = int(payload)
        stats = query_ledger_stats(user_id, contact_id)
        contact_name = _get_user_name_from_db(contact_id)
        
        net = stats['income'] - stats['outcome']
        
        text = (
            f"📊 *{escape_markdown('与 ' + truncate_for_link_text(contact_name) + ' 的账本统计')}*\n"
            f"*{'─'*20}*\n"
            f"🟢 *{escape_markdown('总收入:')}* `{stats['income']:.2f}`\n"
            f"🔴 *{escape_markdown('总支出:')}* `{stats['outcome']:.2f}`\n"
            f"🔵 *{escape_markdown('净利润:')}* `{'%.2f' % net}`\n"
            f"🔄 *{escape_markdown('总笔数:')}* `{stats['count']}`"
        )
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🔙 返回统计列表", callback_data="premium:stats"))
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="MarkdownV2")

# ====================================================================
# END OF LEDGER ANALYSIS AND STATS LOGIC
# ====================================================================

# ---------------------- Business 业务连接与消息处理 ----------------------

def perform_background_scam_check(business_connection_id: str, chat_id: int, business_user_id: int, contact_user: types.User):
    if not telethon_loop or not client.is_connected() or not contact_user or contact_user.is_bot:
        return

    contact_id = contact_user.id
    now = int(time.time())

    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute(
            "SELECT last_checked FROM checked_contacts WHERE user_id = ? AND contact_id = ?",
            (business_user_id, contact_id)
        )
        last_check = c.fetchone()
        conn.close()

    if last_check and (now - last_check['last_checked'] < CONFIG.get("BUSINESS_SCAN_COOLDOWN", 86400)):
        return

    print(f"🕵️ [Business-Scan] 开始为用户 {business_user_id} 自动检测联系人 {contact_id} (@{contact_user.username})...")

    is_scammer = False
    warning_reason = ""
    evidence_data_to_send = []

    reports = load_reports()
    verified_report = reports.get('verified', {}).get(str(contact_id))
    if not verified_report and contact_user.username:
        for record in reports.get('verified', {}).values():
            if record.get('usernames') and contact_user.username.lower() in [u.lower() for u in record.get('usernames', [])]:
                verified_report = record
                break
    
    if verified_report:
        is_scammer = True
        warning_reason = "此联系人已被【官方验证】为诈骗者。"
        
        evidence_data_from_report = verified_report.get('evidence_data', [])
        evidence_data_to_send = evidence_data_from_report if isinstance(evidence_data_from_report, list) else [evidence_data_from_report]


    scam_channel_hits = []
    if not is_scammer:
        try:
            future = asyncio.run_coroutine_threadsafe(search_monitored_channels_for_user(user_id=contact_id), telethon_loop)
            scam_channel_hits = future.result(timeout=CONFIG.get("SCAM_CHANNEL_SEARCH_TIMEOUT", 40))
        except Exception as e:
            print(f"💥 [Business-Scan] 自动检测时搜索频道失败 (联系人: {contact_id}): {type(e).__name__} - {e}")

        if scam_channel_hits:
            is_scammer = True
            reason_parts = ["此联系人在【反诈频道】中有以下曝光记录："]
            links = [f"› [{escape_markdown(truncate_for_link_text(hit['chat_title']))}]({hit['link']})" for hit in scam_channel_hits[:3]]
            reason_parts.extend(links)
            warning_reason = "\n".join(reason_parts)

    if is_scammer:
        contact_name = (contact_user.first_name or "") + (" " + (contact_user.last_name or "") if contact_user.last_name else "")
        contact_name = contact_name.strip() or f"User ID {contact_id}"
        
        username_mention = (
            escape_markdown(f"@{contact_user.username}") if contact_user.username else escape_markdown('N/A')
        )
        left_paren = escape_markdown('(')
        right_paren = escape_markdown(')')
        pipe_symbol = escape_markdown('|')

        warning_message_md = (
            f"🚨 *{escape_markdown('安全警报 (自动检测)')}* 🚨\n\n"
            f"{escape_markdown('联系人')} *{escape_markdown(contact_name)}* "
            f"{left_paren}{username_mention} {pipe_symbol} {format_inline_code(str(contact_id))}{right_paren} "
            f"{escape_markdown('存在高风险记录。')}\n\n"
            f"*{escape_markdown('原因:')}* {warning_reason}\n\n"
            f"*{escape_markdown('请谨慎交易，注意防范风险。')}*"
        )
        try:
            bot.send_message(
                chat_id=chat_id,
                text=warning_message_md,
                business_connection_id=business_connection_id,
                parse_mode="MarkdownV2",
                disable_web_page_preview=True
            )

            if evidence_data_to_send:
                for evidence in evidence_data_to_send:
                    if evidence:
                        send_serialized_message(chat_id, evidence, business_connection_id=business_connection_id)
                        time.sleep(0.5)
            
            print(f"✅ [Business-Scan] 成功在聊天 {chat_id} 中发送关于 {contact_id} 的警告和证据。")

        except Exception as e:
            print(f"💥 [Business-Scan] 无法在聊天 {chat_id} 中发送主警告: {e}")
            try:
                fallback_text = f"⚠️ *{escape_markdown('自动安全检测失败')}*\n\n{escape_markdown('尝试在与联系人的聊天中发送警告时出错，请在私聊中查看此警告。')}\n\n" + warning_message_md
                bot.send_message(business_user_id, fallback_text, parse_mode="MarkdownV2", disable_web_page_preview=True)
            except Exception as e2:
                print(f"💥 [Business-Scan] 回退私聊警告也发送失败 (用户: {business_user_id}): {e2}")

    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('''
            INSERT INTO checked_contacts (user_id, contact_id, last_checked, is_scammer) VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id, contact_id) DO UPDATE SET
                last_checked = excluded.last_checked,
                is_scammer = excluded.is_scammer
        ''', (business_user_id, contact_id, now, is_scammer))
        conn.commit()
        conn.close()

@bot.business_connection_handler(func=lambda conn: True)
def handle_business_connection(connection: types.BusinessConnection):
    user_id = connection.user_chat_id
    conn_id = connection.id
    is_enabled = connection.is_enabled
    now = int(time.time())

    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('''
            INSERT INTO business_connections (connection_id, user_id, is_enabled, last_updated) VALUES (?, ?, ?, ?)
            ON CONFLICT(connection_id) DO UPDATE SET
                is_enabled = excluded.is_enabled,
                last_updated = excluded.last_updated
        ''', (conn_id, user_id, is_enabled, now))
        conn.commit()
        conn.close()

    status_text = "启用" if is_enabled else "禁用"
    log_msg = f"🤝 *{escape_markdown('业务连接更新')}*\n" \
              f"› *{escape_markdown('用户:')}* [{escape_markdown(str(user_id))}](tg://user?id={user_id})\n" \
              f"› *{escape_markdown('状态:')}* {escape_markdown(status_text)}"
    send_log_to_channel(log_msg)
    
    if is_enabled:
        bot.send_message(user_id, f"✅ *{escape_markdown('连接成功！')}*\n{escape_markdown('猎诈卫士已成为您的私人安全助理。您现在可以前往')} {escape_markdown('/premium_features')} {escape_markdown('设置自动回复等高级功能。')}", parse_mode="MarkdownV2")
    else:
        bot.send_message(user_id, f"ℹ️ *{escape_markdown('连接已禁用')}*\n{escape_markdown('所有 Business 相关功能（如自动回复、记账）已暂停。')}", parse_mode="MarkdownV2")


@bot.business_message_handler(func=lambda msg: True)
def handle_business_message(message: types.Message):
    if not message.business_connection_id or not message.chat:
        return
        
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT user_id FROM business_connections WHERE connection_id = ? AND is_enabled = 1", (message.business_connection_id,))
        res = c.fetchone()
        conn.close()

    if not res: return
    
    business_user_id = res['user_id']
    sender_is_business_user = message.from_user and message.from_user.id == business_user_id
    
    if sender_is_business_user and message.text:
        handle_ledger_command(message)
        return

    else:
        contact_user_object = message.from_user
        if not contact_user_object or contact_user_object.is_bot:
            return

        if message.text:
            with db_lock:
                conn = get_db_connection()
                c = conn.cursor()
                c.execute("SELECT keyword, reply_message_json FROM keyword_replies WHERE user_id = ?", (business_user_id,))
                keywords = c.fetchall()
                conn.close()
            
            for row in keywords:
                keyword = row['keyword']
                if keyword.lower() in message.text.lower():
                    print(f"✅ [Keyword-Reply] Triggered for user {business_user_id} by keyword '{keyword}'")
                    reply_data_from_db = json.loads(row['reply_message_json'])
                    reply_data_list = reply_data_from_db if isinstance(reply_data_from_db, list) else [reply_data_from_db]

                    for i, reply_data in enumerate(reply_data_list):
                        send_serialized_message(message.chat.id, reply_data, business_connection_id=message.business_connection_id)
                        if i < len(reply_data_list) - 1:
                            time.sleep(0.5)
                    return

        with db_lock:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT is_enabled, reply_message_json FROM offline_replies WHERE user_id = ?", (business_user_id,))
            offline_settings = c.fetchone()
            conn.close()
        
        if offline_settings and offline_settings['is_enabled'] and offline_settings['reply_message_json']:
            now = time.time()
            cooldown_key = (business_user_id, contact_user_object.id)
            last_sent_time = offline_reply_cooldown_cache.get(cooldown_key, 0)

            if now - last_sent_time > CONFIG.get("OFFLINE_REPLY_COOLDOWN", 300):
                try:
                    future = asyncio.run_coroutine_threadsafe(get_user_status_async(business_user_id), telethon_loop)
                    status = future.result(timeout=10)
                    
                    if status != 'online':
                        print(f"✅ [Offline-Reply] Triggered for user {business_user_id} (status: {status})")
                        reply_data_from_db = json.loads(offline_settings['reply_message_json'])
                        reply_data_list = reply_data_from_db if isinstance(reply_data_from_db, list) else [reply_data_from_db]

                        all_sent = True
                        for i, reply_data in enumerate(reply_data_list):
                            if not send_serialized_message(message.chat.id, reply_data, business_connection_id=message.business_connection_id):
                                all_sent = False
                                break
                            if i < len(reply_data_list) - 1:
                                time.sleep(0.5)

                        if all_sent:
                            offline_reply_cooldown_cache[cooldown_key] = now
                        return
                    else:
                        print(f"ℹ️ [Offline-Reply] Skipped for user {business_user_id}. Reason: User status is 'online'.")
                except (FuturesTimeoutError, Exception) as e:
                    print(f"⚠️ [Offline-Reply] Could not check status for user {business_user_id}: {e}")
            else:
                print(f"ℹ️ [Offline-Reply] Cooldown active for user {business_user_id} and contact {contact_user_object.id}. Skipping.")

        threading.Thread(
            target=perform_background_scam_check,
            args=(message.business_connection_id, message.chat.id, business_user_id, contact_user_object),
            daemon=True
        ).start()

# ---------------------- 高级功能回调与设置流程 ----------------------

@bot.callback_query_handler(func=lambda call: call.data.startswith(('premium:', 'offline:', 'keyword:')))
@check_membership
@premium_only
def handle_all_premium_callbacks(call: types.CallbackQuery):
    action_parts = call.data.split(':')
    menu = action_parts[0]
    action = action_parts[1] if len(action_parts) > 1 else 'main'
    user_id = call.from_user.id
    message = call.message
    chat_id = message.chat.id
    
    bot.answer_callback_query(call.id)

    if menu == 'premium':
        if action == 'main': handle_premium_main_menu(call)
        elif action == 'ledger': show_ledger_settings(call)
        elif action == 'offline_reply': handle_offline_reply_menu(call)
        elif action == 'keyword_reply': handle_keyword_reply_menu(call, is_main_menu=True)
        elif action == 'stats': show_ledger_stats_menu(call)
        elif action == 'analyze': show_ledger_analysis_menu(call)
    
    elif menu == 'offline':
        if action == 'toggle':
            with db_lock:
                conn = get_db_connection()
                c = conn.cursor()
                c.execute("SELECT is_enabled FROM offline_replies WHERE user_id = ?", (user_id,))
                res = c.fetchone()
                new_status = not (res['is_enabled'] if res else False)
                c.execute("INSERT INTO offline_replies (user_id, is_enabled, last_updated) VALUES (?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET is_enabled=excluded.is_enabled, last_updated=excluded.last_updated",
                          (user_id, new_status, int(time.time())))
                conn.commit()
                conn.close()
            handle_offline_reply_menu(call, toggled=True)
        elif action == 'set':
            user_settings_state[user_id] = {'flow': 'awaiting_offline_reply', 'messages': []}
            prompt_text = (
                f"📲 *{escape_markdown('设置离线回复')}*\n\n"
                f"{escape_markdown('请发送您希望作为自动回复的一条或多条消息。')}\n\n"
                f"*{escape_markdown('发送完毕后，请输入')}* `{escape_markdown(DONE_SUBMISSION_COMMAND)}` *{escape_markdown('来完成设置。')}*"
            )
            bot.send_message(chat_id, prompt_text, parse_mode="MarkdownV2")
            bot.register_next_step_handler(message, process_settings_flow)
        elif action == 'view':
            with db_lock:
                conn = get_db_connection()
                c = conn.cursor()
                c.execute("SELECT reply_message_json FROM offline_replies WHERE user_id = ?", (user_id,))
                res = c.fetchone()
                conn.close()
            if res and res['reply_message_json']:
                bot.send_message(chat_id, f"👀 *{escape_markdown('您当前的离线回复预览如下:')}*", parse_mode="MarkdownV2")
                reply_data_from_db = json.loads(res['reply_message_json'])
                reply_list = reply_data_from_db if isinstance(reply_data_from_db, list) else [reply_data_from_db]
                for reply_data in reply_list:
                    send_serialized_message(chat_id, reply_data)
                    time.sleep(0.5)
            else:
                bot.send_message(chat_id, f"ℹ️ {escape_markdown('您尚未设置离线回复内容。')}", parse_mode="MarkdownV2")

    elif menu == 'keyword':
        if action == 'add':
            user_settings_state[user_id] = {'flow': 'awaiting_keyword_keyword'}
            prompt_text = f"⌨️ {escape_markdown('请输入您要设置的关键词（不区分大小写，一行一个）。')}"
            bot.send_message(chat_id, prompt_text, parse_mode="MarkdownV2")
            bot.register_next_step_handler(message, process_settings_flow)
        elif action == 'del':
            if len(action_parts) > 2:
                keyword_id_to_del = action_parts[2]
                with db_lock:
                    conn = get_db_connection()
                    c = conn.cursor()
                    c.execute("DELETE FROM keyword_replies WHERE id = ? AND user_id = ?", (keyword_id_to_del, user_id))
                    conn.commit()
                    conn.close()
                bot.answer_callback_query(call.id, "已删除")
                handle_keyword_reply_menu(call, is_main_menu=False)
        elif action == 'view':
            if len(action_parts) > 2:
                keyword_id_to_view = action_parts[2]
                with db_lock:
                    conn = get_db_connection()
                    c = conn.cursor()
                    c.execute("SELECT keyword, reply_message_json FROM keyword_replies WHERE id = ? AND user_id = ?", (keyword_id_to_view, user_id))
                    res = c.fetchone()
                    conn.close()
                if res and res['reply_message_json']:
                    bot.send_message(chat_id, f"👀 *{escape_markdown('关键词“' + res['keyword'] + '”的回复预览如下:')}*", parse_mode="MarkdownV2")
                    reply_data_from_db = json.loads(res['reply_message_json'])
                    reply_list = reply_data_from_db if isinstance(reply_data_from_db, list) else [reply_data_from_db]
                    for reply_data in reply_list:
                        send_serialized_message(chat_id, reply_data)
                        time.sleep(0.5)
                else:
                    bot.send_message(chat_id, f"ℹ️ {escape_markdown('找不到该关键词的回复内容。')}", parse_mode="MarkdownV2")

def handle_offline_reply_menu(call, toggled=False):
    user_id = call.from_user.id
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT is_enabled FROM offline_replies WHERE user_id = ?", (user_id,))
        res = c.fetchone()
        conn.close()
    
    is_enabled = res['is_enabled'] if res else False
    status_text = "✅ 已启用" if is_enabled else "❌ 已禁用"
    toggle_text = "🌙 禁用" if is_enabled else "☀️ 启用"

    text = f"🌙 *{escape_markdown('离线自动回复管理')}*\n\n*{escape_markdown('当前状态:')}* {escape_markdown(status_text)}\n\n{escape_markdown('当您在Telegram上显示为离线或离开时，此功能会自动回复联系人发来的第一条消息。可以设置多条消息作为回复序列。')}"
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton(toggle_text, callback_data="offline:toggle"),
        types.InlineKeyboardButton("📝 设置回复内容", callback_data="offline:set"),
        types.InlineKeyboardButton("👀 查看当前回复", callback_data="offline:view"),
        types.InlineKeyboardButton("🔙 返回", callback_data="premium:main")
    )
    if toggled:
        bot.answer_callback_query(call.id, f"已{toggle_text.split(' ')[1]}")
    bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="MarkdownV2")

def handle_keyword_reply_menu(call, is_main_menu=False, deleted=False):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    message_id = call.message.message_id
    
    with db_lock:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT id, keyword FROM keyword_replies WHERE user_id = ? ORDER BY keyword", (user_id,))
        keywords = c.fetchall()
        conn.close()
    
    text_parts = [f"📝 *{escape_markdown('关键词自动回复管理')}*"]
    if not keywords:
        text_parts.append(f"\n{escape_markdown('您还没有设置任何关键词。点击下方按钮添加。')}")
    else:
        text_parts.append(f"\n{escape_markdown('以下是您当前的关键词列表。您可以查看或删除它们。')}")

    markup = types.InlineKeyboardMarkup(row_width=2)
    if keywords:
        for row in keywords:
            markup.add(
                types.InlineKeyboardButton(f"👀 {row['keyword']}", callback_data=f"keyword:view:{row['id']}"),
                types.InlineKeyboardButton(f"🗑️ 删除", callback_data=f"keyword:del:{row['id']}")
            )
    
    markup.add(
        types.InlineKeyboardButton("➕ 添加新关键词", callback_data="keyword:add"),
        types.InlineKeyboardButton("🔙 返回", callback_data="premium:main")
    )
    
    try:
        if is_main_menu or deleted:
            if deleted:
                bot.answer_callback_query(call.id, "已删除")
            bot.edit_message_text("\n".join(text_parts), chat_id, message_id, reply_markup=markup, parse_mode="MarkdownV2")
        elif not is_main_menu and call.data.startswith("keyword:del"):
             bot.edit_message_text("\n".join(text_parts), chat_id, message_id, reply_markup=markup, parse_mode="MarkdownV2")

    except ApiTelegramException as e:
        if "message is not modified" not in str(e).lower():
             print(f"Error in handle_keyword_reply_menu: {e}")

def process_settings_flow(message: types.Message):
    user_id = message.from_user.id
    if user_id not in user_settings_state: return

    state = user_settings_state[user_id]
    flow = state['flow']

    if flow == 'awaiting_offline_reply':
        if message.text and message.text.strip() == DONE_SUBMISSION_COMMAND:
            if not state.get('messages'):
                bot.reply_to(message, f"⚠️ {escape_markdown('您还没有发送任何要作为回复的消息。请发送消息或取消。')}", parse_mode="MarkdownV2")
                bot.register_next_step_handler(message, process_settings_flow)
                return

            serialized_list = [msg for msg in [serialize_message(m) for m in state['messages']] if msg is not None]
            if not serialized_list:
                bot.reply_to(message, f"❌ {escape_markdown('设置失败，未能识别您发送的任何消息格式。')}", parse_mode="MarkdownV2")
                del user_settings_state[user_id]
                return

            with db_lock:
                conn = get_db_connection()
                c = conn.cursor()
                c.execute("INSERT INTO offline_replies (user_id, reply_message_json, last_updated) VALUES (?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET reply_message_json=excluded.reply_message_json, last_updated=excluded.last_updated",
                          (user_id, json.dumps(serialized_list), int(time.time())))
                conn.commit()
                conn.close()
            
            bot.reply_to(message, f"✅ *{escape_markdown('离线回复已更新！')}*\n{escape_markdown('您可以在菜单中启用它。')}", parse_mode="MarkdownV2")
            del user_settings_state[user_id]
        else:
            state['messages'].append(message)
            bot.register_next_step_handler(message, process_settings_flow)

    elif flow == 'awaiting_keyword_keyword':
        keyword = message.text.strip()
        if not keyword or '\n' in keyword:
            bot.reply_to(message, f"❌ *{escape_markdown('关键词无效')}*\n{escape_markdown('关键词不能为空，且只能包含一行。请重新输入。')}", parse_mode="MarkdownV2")
            bot.register_next_step_handler(message, process_settings_flow)
            return
        
        state['flow'] = 'awaiting_keyword_reply'
        state['keyword'] = keyword
        state['messages'] = []
        prompt_text = (
            f"✅ *{escape_markdown('关键词已收到！')}*\n\n"
            f"{escape_markdown('现在，请发送您希望绑定到关键词“' + keyword + '”的一条或多条回复消息。')}\n\n"
            f"*{escape_markdown('发送完毕后，请输入')}* `{escape_markdown(DONE_SUBMISSION_COMMAND)}` *{escape_markdown('来完成。')}*"
        )
        bot.reply_to(message, prompt_text, parse_mode="MarkdownV2")
        bot.register_next_step_handler(message, process_settings_flow)
        
    elif flow == 'awaiting_keyword_reply':
        if message.text and message.text.strip() == DONE_SUBMISSION_COMMAND:
            if not state.get('messages'):
                bot.reply_to(message, f"⚠️ {escape_markdown('您还没有发送任何要作为回复的消息。请发送消息或取消。')}", parse_mode="MarkdownV2")
                bot.register_next_step_handler(message, process_settings_flow)
                return
            
            keyword = state['keyword']
            serialized_list = [msg for msg in [serialize_message(m) for m in state['messages']] if msg is not None]
            if not serialized_list:
                bot.reply_to(message, f"❌ {escape_markdown('设置失败，未能识别您发送的任何消息格式。')}", parse_mode="MarkdownV2")
                del user_settings_state[user_id]
                return

            with db_lock:
                conn = get_db_connection()
                c = conn.cursor()
                lower_keyword = keyword.lower()
                c.execute("DELETE FROM keyword_replies WHERE user_id = ? AND keyword = ?", (user_id, lower_keyword))
                c.execute("INSERT INTO keyword_replies (user_id, keyword, reply_message_json, last_updated) VALUES (?, ?, ?, ?)",
                          (user_id, lower_keyword, json.dumps(serialized_list), int(time.time())))
                conn.commit()
                conn.close()

            bot.reply_to(message, f"✅ *{escape_markdown('关键词回复设置成功！')}*\n*{escape_markdown('关键词:')}* `{escape_for_code(keyword)}`", parse_mode="MarkdownV2")
            del user_settings_state[user_id]
        else:
            state['messages'].append(message)
            bot.register_next_step_handler(message, process_settings_flow)

# ---------------------- General Callback & Fallback Handlers ----------------------
@bot.callback_query_handler(func=lambda call: call.data in ["query", "forward", "stats", "tougao", "start_menu", "sponsor", "leaderboard"])
@check_membership
def main_menu_callback_handler(call):
    update_active_user(call.from_user.id)
    chat_id = call.message.chat.id
    
    proxy_message = copy.copy(call.message)
    proxy_message.from_user = call.from_user

    if call.data == "start_menu":
        bot.answer_callback_query(call.id)
        handle_start(proxy_message, is_edit=True)
        return

    bot.answer_callback_query(call.id)
    prompts = {
        "query": f"⌨️ *{escape_markdown('请直接发送目标的【用户名】或【ID】')}*\n{escape_markdown('您也可以【转发】目标用户的消息来查询。')}",
        "forward": f"📤 *{escape_markdown('请转发目标用户的消息给我')}*"
    }
    
    if call.data in prompts:
        bot.send_message(chat_id, prompts[call.data], parse_mode="MarkdownV2")
    elif call.data == "stats":
        handle_stats(proxy_message)
    elif call.data == "tougao":
        handle_tougao(proxy_message)
    elif call.data == "sponsor":
        handle_sponsor(proxy_message)
    elif call.data == "leaderboard":
        handle_leaderboard(proxy_message)

    try:
        if call.data in ["query", "forward", "tougao", "sponsor"]:
             bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=None)
    except Exception: pass


@bot.message_handler(func=lambda m: m.chat.type == 'private' and m.text and m.text.strip().startswith(('+', '-', '//')) and not m.business_connection_id)
@check_membership
def handle_unconnected_ledger_command(message: types.Message):
    if getattr(message.from_user, 'is_premium', False):
        try:
            bot_username = bot.get_me().username
            part3_text = f"点击 `添加机器人` 并选择 `@{bot_username}`"
            guidance_text = (
                f"💎 *{escape_markdown('启用记账功能')}*\n\n"
                f"{escape_markdown('检测到您是尊贵的 Telegram Premium 用户！要使用记账功能，请先将我连接到您的商业版账户:')}\n\n"
                f"1\\. {escape_markdown('前往 `设置` > `Telegram Business`')}\n"
                f"2\\. {escape_markdown('选择 `聊天机器人`')}\n"
                f"3\\. {escape_markdown(part3_text)}\n\n"
                f"*{escape_markdown('连接成功后，您就可以在与他人的私聊中使用 `+`、`-` 等命令了。')}*"
            )
            bot.reply_to(message, guidance_text, parse_mode="MarkdownV2")
        except Exception as e:
            print(f"💥 Failed to send business connection guide: {e}")

@bot.message_handler(func=lambda message: True, content_types=ALL_CONTENT_TYPES)
@check_membership
def handle_all_other_messages(message):
    user_id = message.from_user.id
    if user_id in user_settings_state:
        process_settings_flow(message)
        return
    if user_id in user_submission_state and user_submission_state[user_id].get('step') == "awaiting_evidence":
        process_evidence(message)
        return
    if user_id in user_sponsorship_state:
        process_sponsor_amount(message)
        return
    if user_id == CONFIG['ADMIN_ID'] and user_id in admin_broadcast_state:
        return

    update_active_user(user_id)
    if message.chat.type == 'private' and message.text and message.text.startswith('/'):
        known_commands = [
            '/start', '/cxzbf', '/stats', '/admin', '/addchannel',
            '/removechannel', '/listchannels', '/tougao', '/delreport',
            DONE_SUBMISSION_COMMAND, '/broadcast', '/cancel_broadcast',
            '/premium_features', '/jz', '/sponsor', '/leaderboard',
            '/webapp', '/setwebapp'
        ]
        if message.text.split()[0] not in known_commands:
            bot.reply_to(message, f"🤔 *{escape_markdown('无法识别的命令。')}*\n{escape_markdown('请使用')} /start {escape_markdown('查看可用命令。')}" + f"\n\n{ADVERTISEMENT_TEXT}", parse_mode="MarkdownV2")

if __name__ == '__main__':
    # 配置日志记录
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    logger.info(f"🚀 初始化 猎诈卫士 Bot ({BOT_VERSION})...")
    
    get_bot_name()

    for fname, default_content in [
        (CONFIG["CHANNELS_FILE"], '[]'),
        (CONFIG["REPORTS_FILE"], '{"pending": {}, "verified": {}}'),
    ]:
        if not os.path.exists(fname):
            ensure_parent_dir(Path(fname))
            with open(fname, 'w', encoding='utf-8') as f: f.write(default_content)
            logger.info(f"📄 创建默认文件: {fname}")

    init_db()

    # --- 启动顺序优化 ---

    # 1. 优先启动 Flask Web 服务器，确保端口监听成功
    server_thread = threading.Thread(target=run_server, name="FlaskWebhookThread", daemon=True)
    server_thread.start()
    logger.info("✅ OKPay 回调服务器线程已启动。")
    time.sleep(3) # 等待3秒，确保 Flask 有足够时间完成初始化和端口绑定

    # 2. 启动日志批量处理器
    log_thread = threading.Thread(target=log_batcher_thread, name="LogBatcherThread", daemon=True)
    log_thread.start()
    logger.info("✅ 日志批量处理器线程已启动。")

    # 3. 启动 Telethon 客户端
    telethon_thread = threading.Thread(target=start_telethon, name="TelethonThread", daemon=True)
    telethon_thread.start()
    logger.info("✅ Telethon 客户端线程启动。等待连接...")
    time.sleep(5) # 等待 Telethon 开始连接

    # 4. 最后启动 Telebot 轮询，这是主阻塞进程
    logger.info("🤖 Telebot 准备开始轮询 (使用带自愈功能的 infinity_polling)...")
    while True:
        try:
            logger.info(f"🟢 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Telebot 轮询循环已启动/重启。")
            bot.infinity_polling(
                timeout=CONFIG.get('TELEBOT_API_TIMEOUT', 40),
                long_polling_timeout=CONFIG.get('TELEBOT_POLLING_TIMEOUT', 30),
                allowed_updates=telebot.util.update_types
            )
        except KeyboardInterrupt:
            logger.info("\n🛑 正在关闭...")
            break
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
            logger.warning(f"🔥 Telebot 网络错误: {type(e).__name__}. 轮询中断。15秒后重启...")
            time.sleep(15)
        except ApiTelegramException as e:
            logger.error(f"🔥 Telebot API 错误: {e}。15秒后重启...")
            time.sleep(15)
        except Exception as e:
            logger.exception(f"💥 Telebot 轮询出现未处理的致命错误: {e}")
            logger.info("🔁 发生严重错误，30秒后将自动重启轮询...")
            time.sleep(30)
    
    # --- 清理部分（保持不变）---
    logger.info("🧹 开始清理和关闭程序...")
    bot.stop_polling()
    if telethon_loop and telethon_loop.is_running():
        logger.info("🔌 正在断开 Telethon 连接...")
        future = asyncio.run_coroutine_threadsafe(client.disconnect(), telethon_loop)
        try:
            future.result(timeout=5)
            logger.info("✅ Telethon 已断开。")
        except Exception as e:
            logger.warning(f"⚠️ Telethon 断开连接超时或失败: {e}")
        
        if telethon_loop.is_running():
            telethon_loop.call_soon_threadsafe(telethon_loop.stop)
    
    if 'telethon_thread' in locals() and telethon_thread.is_alive():
        logger.info("⏳ 等待 Telethon 线程结束...")
        telethon_thread.join(timeout=10)
        if telethon_thread.is_alive():
            logger.warning("⚠️ Telethon 线程未能正常结束。")

    logger.info("🚪 主线程退出。程序已关闭。")
