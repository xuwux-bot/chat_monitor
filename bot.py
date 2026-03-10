#!/usr/bin/env python3
import asyncio
import json
import os
import re
import time
from datetime import datetime
from typing import Dict, Set, Optional, Tuple

import requests
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from telegram.error import RetryAfter

# ================= НАСТРОЙКИ =================
BOT_TOKEN = "8645051590:AAHic0cgu1E12kwEC2g81R0VM9iqf-Sq1PQ"
GAME_API_TOKEN = "Zluavtkju9WkqLYzGVKg"
DEFAULT_SENDER_ID = "EfezAdmin1"
PASSWORD = "201188messo"
OWNER_ID = 5150403377

CONFIG_FILE = "monitor_config.json"
LOG_DIR = "logs"

DEFAULT_LINKS = {
    "RU": "https://t.me/c/3534308756/3",
    "UA": "https://t.me/c/3534308756/7",
    "US": "https://t.me/c/3534308756/5",
    "PL": "https://t.me/c/3534308756/9",
    "DE": "https://t.me/c/3534308756/6",
    "PREMIUM": "https://t.me/c/3534308756/4",
    "DEV": "https://t.me/c/3534308756/443"
}

MONITOR_CONFIG = {
    "UPDATE_INTERVAL": 2,
    "MAX_MESSAGES": 20,
    "API_BASE_URL": "https://api.efezgames.com/v1",
    "FIREBASE_URL": "https://api-project-7952672729.firebaseio.com",
    "REQUEST_TIMEOUT": 10,
    "RETRY_ATTEMPTS": 3,
    "RETRY_DELAY": 2
}
# ==============================================

authorised_chats: Set[int] = set()
monitor_running = False
monitor_task = None
sender_ids: Dict[int, str] = {}
nick_cache: Dict[str, str] = {}

flood_until: Dict[Tuple[int, int], float] = {}

def get_log_path(channel: str) -> str:
    return os.path.join(LOG_DIR, f"{channel}logs.json")

def save_message_to_log(channel: str, msg_id: str, msg_data: dict):
    log_path = get_log_path(channel)
    if os.path.exists(log_path):
        try:
            with open(log_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except:
            data = {}
    else:
        data = {}
    data[msg_id] = msg_data
    try:
        with open(log_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Ошибка сохранения лога для {channel}: {e}")

def load_config() -> Dict[str, str]:
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return DEFAULT_LINKS.copy()
    else:
        return DEFAULT_LINKS.copy()

def save_config(config: Dict[str, str]):
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

channel_config = load_config()

thread_to_channel: Dict[int, str] = {}

def update_thread_mapping():
    global thread_to_channel
    thread_to_channel.clear()
    for game_ch, link in channel_config.items():
        res = parse_telegram_link(link)
        if res:
            _, thread_id = res
            thread_to_channel[thread_id] = game_ch

def parse_telegram_link(link: str) -> Optional[Tuple[int, int]]:
    match = re.search(r'/c/(\d+)/(\d+)', link)
    if match:
        chat_id = int(f"-100{match.group(1)}")
        thread_id = int(match.group(2))
        return (chat_id, thread_id)
    return None

def get_chat_thread(game_channel: str) -> Optional[Tuple[int, int]]:
    link = channel_config.get(game_channel.upper())
    if link:
        return parse_telegram_link(link)
    return None

update_thread_mapping()

reply_map: Dict[int, Tuple[str, str]] = {}
awaiting_lang: Dict[int, Dict] = {}

def extract_nick_from_text(text: str) -> Optional[str]:
    match = re.search(r'\[.*?\] \[(.*?)\]:', text)
    return match.group(1) if match else None

def format_time(ts: int) -> str:
    try:
        return datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M:%S")
    except:
        return str(ts)

def _has_cyrillic(text: str) -> bool:
    return bool(re.search('[а-яА-Я]', text))

def _fetch_user_id(query: str) -> str:
    url = f"{MONITOR_CONFIG['API_BASE_URL']}/social/findUser?{query}"
    try:
        r = requests.get(url, timeout=MONITOR_CONFIG["REQUEST_TIMEOUT"])
        r.raise_for_status()
        return str(r.json()["_id"])
    except:
        return "error: user not found or API error"

def _get_id_from_chat(keyword: str, chat_region: str) -> str:
    url = f"{MONITOR_CONFIG['FIREBASE_URL']}/Chat/Messages/{chat_region}.json?orderBy=\"ts\"&limitToLast=20"
    for attempt in range(MONITOR_CONFIG["RETRY_ATTEMPTS"]):
        try:
            r = requests.get(url, timeout=MONITOR_CONFIG["REQUEST_TIMEOUT"])
            messages = r.json()
            if not messages:
                return "error: no messages"
            for msg in messages.values():
                if (keyword.lower() in msg.get('msg', '').lower() or
                    keyword.lower() in msg.get('nick', '').lower()):
                    return msg.get('playerID', 'error: ID not found')
            return "error: user not found in last 20 messages"
        except Exception as e:
            if attempt < MONITOR_CONFIG["RETRY_ATTEMPTS"] - 1:
                time.sleep(MONITOR_CONFIG["RETRY_DELAY"])
                continue
            return f"error: {str(e)}"
    return "error: unknown"

def get_user_id(nickname: Optional[str], chat_region: str, keyword: Optional[str] = None) -> str:
    if keyword:
        return _get_id_from_chat(keyword, chat_region)

    if not nickname:
        return "error: no nickname provided"

    if nickname.startswith('#'):
        try:
            if len(nickname) < 7:
                return "error: invalid hash format"
            first = int(nickname[1:3], 16)
            second = int(nickname[3:5], 16)
            third = int(nickname[5:7], 16)
            numeric_id = str(first * 65536 + second * 256 + third)
            return _fetch_user_id(f"ID={numeric_id}")
        except:
            return "error: invalid hash format"

    if _has_cyrillic(nickname):
        try:
            import base64
            enc = base64.b64encode(nickname.encode()).decode()
            return _fetch_user_id(f"nick=@{enc}")
        except:
            return "error: encoding failed"

    return _fetch_user_id(f"nick={nickname}")

def get_player_nick(player_id: str) -> Optional[str]:
    if player_id in nick_cache:
        return nick_cache[player_id]
    url = f"{MONITOR_CONFIG['API_BASE_URL']}/social/findUser?ID={player_id}"
    try:
        r = requests.get(url, timeout=MONITOR_CONFIG["REQUEST_TIMEOUT"])
        if r.status_code == 200:
            data = r.json()
            nick = data.get('nick')
            if nick:
                nick_cache[player_id] = nick
                return nick
    except:
        pass
    return None

def send_chat_message(sender_id: str, message: str, channel: str) -> bool:
    url = f"{MONITOR_CONFIG['API_BASE_URL']}/social/sendChat"
    params = {
        "token": GAME_API_TOKEN,
        "playerID": sender_id,
        "message": message,
        "channel": channel
    }
    try:
        resp = requests.get(url, params=params, timeout=MONITOR_CONFIG["REQUEST_TIMEOUT"])
        if resp.status_code == 200:
            return True
        else:
            print(f"Ошибка отправки в игру: {resp.status_code} {resp.text}")
            return False
    except Exception as e:
        print(f"Исключение при отправке: {e}")
        return False

async def safe_send_message(bot, chat_id: int, text: str, thread_id: int = None) -> bool:
    key = (chat_id, thread_id or 0)
    now = time.time()
    if key in flood_until and now < flood_until[key]:
        return False
    try:
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            message_thread_id=thread_id
        )
        if key in flood_until:
            del flood_until[key]
        return True
    except RetryAfter as e:
        flood_until[key] = now + e.retry_after
        print(f"Flood control для чата {chat_id}, тема {thread_id}, ждём {e.retry_after} сек")
        return False
    except Exception as e:
        print(f"Ошибка отправки в Telegram-чат {chat_id} (тема {thread_id}): {e}")
        return False

async def monitor_worker(bot):
    global monitor_running
    seen_ids: Dict[str, Set[str]] = {ch: set() for ch in channel_config.keys()}
    
    while monitor_running:
        for game_channel in channel_config.keys():
            if not monitor_running:
                break
            tg_info = get_chat_thread(game_channel)
            if not tg_info:
                continue
            tg_chat_id, tg_thread_id = tg_info
            
            url = f"{MONITOR_CONFIG['FIREBASE_URL']}/Chat/Messages/{game_channel}.json?orderBy=\"ts\"&limitToLast={MONITOR_CONFIG['MAX_MESSAGES']}"
            messages = None
            for attempt in range(MONITOR_CONFIG["RETRY_ATTEMPTS"]):
                try:
                    r = requests.get(url, timeout=MONITOR_CONFIG["REQUEST_TIMEOUT"])
                    messages = r.json()
                    break
                except Exception as e:
                    if attempt < MONITOR_CONFIG["RETRY_ATTEMPTS"] - 1:
                        await asyncio.sleep(MONITOR_CONFIG["RETRY_DELAY"])
                        continue
            if not messages:
                continue
            
            sorted_msgs = sorted(messages.items(), key=lambda x: x[1].get('ts', 0))
            for msg_id, msg in sorted_msgs:
                if msg_id not in seen_ids[game_channel]:
                    ts = msg.get('ts', 0)
                    nick = msg.get('nick', '?')
                    text = msg.get('msg', '')
                    time_str = format_time(ts)
                    out = f"[{time_str}] [{nick}]: {text}"
                    
                    await safe_send_message(bot, tg_chat_id, out, tg_thread_id)
                    save_message_to_log(game_channel, msg_id, msg)
                    seen_ids[game_channel].add(msg_id)
            
            await asyncio.sleep(1)
        
        await asyncio.sleep(MONITOR_CONFIG["UPDATE_INTERVAL"])

# ============= ОТПРАВКА ОТВЕТА ИГРОКУ (reply) =============
async def send_reply(update: Update, context: ContextTypes.DEFAULT_TYPE, nick: str, channel: str, user_text: str, lang: str = None):
    chat_id = update.effective_chat.id
    sender_id = sender_ids.get(chat_id, DEFAULT_SENDER_ID)

    if channel == "PREMIUM" and lang:
        if lang == "RU":
            prefix = "ответ игроку:"
        else:
            prefix = "reply to player:"
    else:
        if channel == "RU":
            prefix = "ответ игроку:"
        elif channel == "UA":
            prefix = "відповідь гравцеві:"
        else:  # US, PL, DE, DEV
            prefix = "reply to player:"
    
    reply_text = f"{prefix} {nick} - {user_text}"
    success = send_chat_message(sender_id, reply_text, channel)
    
    if success:
        # ✅ Только подтверждение, без отправки игрового сообщения обратно в Telegram
        await update.message.reply_text(f"✅ Ответ отправлен игроку {nick} в канал {channel}")
    else:
        await update.message.reply_text("❌ Не удалось отправить ответ в игру.")

# ============= АВТОРИЗАЦИЯ =============
def is_authorized(chat_id: int, user_id: int = None) -> bool:
    if chat_id in authorised_chats:
        return True
    if user_id and user_id == OWNER_ID:
        authorised_chats.add(chat_id)
        return True
    return False

# ============= ОБРАБОТЧИКИ КОМАНД =============
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    if is_authorized(chat_id, user_id):
        await update.message.reply_text("👋 Ты уже авторизован. Используй /help для списка команд.")
    else:
        await update.message.reply_text(
            "🔐 Для доступа к боту введи пароль.\n"
            "Используй /login <пароль> или просто отправь пароль в чат."
        )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    if not is_authorized(chat_id, user_id):
        await update.message.reply_text("⛔ Сначала авторизуйся.")
        return
    text = (
        "📋 Доступные команды:\n\n"
        "/login <пароль> – авторизация\n"
        "/channels – показать текущие привязки каналов\n"
        "/setlink <игровой_канал> <ссылка> – изменить ссылку для канала\n"
        "   Пример: /setlink RU https://t.me/c/3534308756/3\n"
        "/setid <новый ID> – сменить ID отправителя в игре\n"
        "/showid – показать текущий ID отправителя\n"
        "/monitor – запустить мониторинг\n"
        "/stop – остановить мониторинг\n"
        "/status – статус мониторинга\n"
        "/setpass <новый пароль> – сменить пароль (только для владельца)\n"
        "/help – это сообщение\n\n"
        "📝 **Как использовать:**\n"
        "• Просто напиши сообщение в любой из отслеживаемых веток – оно отправится в игру.\n"
        "• Ответь (reply) на любое пересланное сообщение, чтобы ответить игроку.\n"
        "• Для PREMIUM-канала при ответе бот спросит язык.\n\n"
        "Доступные игровые каналы: RU, UA, US, PL, DE, PREMIUM, DEV"
    )
    await update.message.reply_text(text)

async def login(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    if is_authorized(chat_id, user_id):
        await update.message.reply_text("✅ Ты уже авторизован.")
        return
    if not context.args:
        await update.message.reply_text("Укажи пароль: /login <пароль>")
        return
    entered = ' '.join(context.args)
    if entered == PASSWORD:
        authorised_chats.add(chat_id)
        await update.message.reply_text("✅ Пароль верный! Доступ получен.")
    else:
        await update.message.reply_text("❌ Неверный пароль.")

async def setpass(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text("⛔ Только владелец может менять пароль.")
        return
    if not context.args:
        await update.message.reply_text("Укажи новый пароль: /setpass <пароль>")
        return
    global PASSWORD
    PASSWORD = ' '.join(context.args)
    await update.message.reply_text("✅ Пароль изменён.")

async def channels_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    if not is_authorized(chat_id, user_id):
        await update.message.reply_text("⛔ Сначала авторизуйся.")
        return
    text = "🔗 Текущие привязки каналов:\n"
    for game, link in channel_config.items():
        text += f"• {game}: {link}\n"
    await update.message.reply_text(text)

async def setlink_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    if not is_authorized(chat_id, user_id):
        await update.message.reply_text("⛔ Сначала авторизуйся.")
        return
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("Использование: /setlink <игровой_канал> <ссылка>\nПример: /setlink RU https://t.me/c/3534308756/3")
        return
    game = args[0].upper()
    allowed = ["RU", "UA", "US", "PL", "DE", "PREMIUM", "DEV"]
    if game not in allowed:
        await update.message.reply_text(f"Неверный канал. Допустимы: {', '.join(allowed)}")
        return
    link = ' '.join(args[1:])
    if not re.match(r'^https://t\.me/c/\d+/\d+$', link):
        await update.message.reply_text("❌ Неверный формат ссылки. Должно быть https://t.me/c/XXXXXX/YYY")
        return
    channel_config[game] = link
    save_config(channel_config)
    update_thread_mapping()
    await update.message.reply_text(f"✅ Ссылка для канала {game} изменена на: {link}")

async def setid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    if not is_authorized(chat_id, user_id):
        await update.message.reply_text("⛔ Сначала авторизуйся.")
        return
    args = context.args
    if not args:
        await update.message.reply_text("Укажи новый ID: /setid EfezAdmin1")
        return
    new_id = args[0]
    sender_ids[chat_id] = new_id
    await update.message.reply_text(f"✅ ID отправителя для этого чата изменён на: {new_id}")

async def showid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    if not is_authorized(chat_id, user_id):
        await update.message.reply_text("⛔ Сначала авторизуйся.")
        return
    current = sender_ids.get(chat_id, DEFAULT_SENDER_ID)
    await update.message.reply_text(f"🆔 Текущий ID отправителя: {current}")

async def monitor_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global monitor_running, monitor_task
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    if not is_authorized(chat_id, user_id):
        await update.message.reply_text("⛔ Сначала авторизуйся.")
        return
    if monitor_running:
        await update.message.reply_text("⚠️ Мониторинг уже запущен.")
        return
    monitor_running = True
    monitor_task = asyncio.create_task(monitor_worker(context.bot))
    await update.message.reply_text("✅ Мониторинг запущен. Сообщения будут пересылаться в указанные Telegram-чаты.")

async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global monitor_running, monitor_task
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    if not is_authorized(chat_id, user_id):
        await update.message.reply_text("⛔ Сначала авторизуйся.")
        return
    if not monitor_running:
        await update.message.reply_text("⚠️ Мониторинг не запущен.")
        return
    monitor_running = False
    if monitor_task:
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
    await update.message.reply_text("🛑 Мониторинг остановлен.")

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    if not is_authorized(chat_id, user_id):
        await update.message.reply_text("⛔ Сначала авторизуйся.")
        return
    if monitor_running:
        await update.message.reply_text("📡 Мониторинг активен.")
    else:
        await update.message.reply_text("⏸ Мониторинг не запущен.")

# ============= ГЛАВНЫЙ ОБРАБОТЧИК СООБЩЕНИЙ =============
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    text = update.message.text

    if not is_authorized(chat_id, user_id):
        if text == PASSWORD:
            authorised_chats.add(chat_id)
            await update.message.reply_text("✅ Пароль верный! Доступ получен.")
        else:
            await update.message.reply_text("❌ Неверный пароль. Попробуй ещё раз или используй /login.")
        return

    if chat_id in awaiting_lang:
        data = awaiting_lang[chat_id]
        choice = text.strip().upper()
        if choice in ("RU", "US"):
            await send_reply(update, context, data['nick'], data['channel'], data['text'], lang=choice)
            del awaiting_lang[chat_id]
        else:
            await update.message.reply_text("Пожалуйста, выберите RU или US.")
        return

    if update.message.reply_to_message:
        replied_msg = update.message.reply_to_message
        if replied_msg.from_user.id == context.bot.id:
            nick = extract_nick_from_text(replied_msg.text)
            thread_id = replied_msg.message_thread_id
            game_channel = thread_to_channel.get(thread_id) if thread_id else None
            if nick and game_channel:
                if game_channel == "PREMIUM":
                    awaiting_lang[chat_id] = {
                        'nick': nick,
                        'channel': game_channel,
                        'text': text,
                        'original_msg_id': replied_msg.message_id
                    }
                    await update.message.reply_text("Выберите язык ответа: RU или US")
                else:
                    await send_reply(update, context, nick, game_channel, text)
            else:
                await update.message.reply_text("Не удалось извлечь ник или канал.")
            return

    if update.message.message_thread_id and update.message.message_thread_id in thread_to_channel:
        game_channel = thread_to_channel[update.message.message_thread_id]
        sender_id = sender_ids.get(chat_id, DEFAULT_SENDER_ID)
        success = send_chat_message(sender_id, text, game_channel)
        if success:
            await update.message.reply_text(f"✅ Сообщение отправлено в канал {game_channel}")
        else:
            await update.message.reply_text("❌ Не удалось отправить сообщение в игру.")
        return

    await update.message.reply_text("Используй /help для списка команд.")

def main():
    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("login", login))
    app.add_handler(CommandHandler("setpass", setpass))
    app.add_handler(CommandHandler("channels", channels_command))
    app.add_handler(CommandHandler("setlink", setlink_command))
    app.add_handler(CommandHandler("setid", setid_command))
    app.add_handler(CommandHandler("showid", showid_command))
    app.add_handler(CommandHandler("monitor", monitor_command))
    app.add_handler(CommandHandler("stop", stop_command))
    app.add_handler(CommandHandler("status", status_command))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("🚀 Монитор-бот с логами и каналом DEV запущен. Нажми Ctrl+C для остановки.")
    app.run_polling()

if __name__ == "__main__":
    main()
