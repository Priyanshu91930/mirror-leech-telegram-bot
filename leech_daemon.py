import os
import sys
import time
import shutil
import asyncio
import logging
import requests
from pymongo import MongoClient
from bson.objectid import ObjectId

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("LeechDaemon")

# Add current directory to path to import config
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
try:
    import config
    BOT_TOKEN = getattr(config, 'BOT_TOKEN', '')
    API_ID = getattr(config, 'TELEGRAM_API', 0)
    API_HASH = getattr(config, 'TELEGRAM_HASH', '')
except ImportError:
    logger.error("Could not import config.py. Please run this script in the root of mirror-leech-telegram-bot.")
    sys.exit(1)

# Pyrogram / Kurigram imports
try:
    from pyrogram import Client
except ImportError:
    try:
        from kurigram import Client
    except ImportError:
        logger.error("Neither pyrogram nor kurigram is installed. Please install it inside your venv.")
        sys.exit(1)

# MongoDB Connection
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb+srv://anihubyt:Zxcvbnmm9193@cluster0.qv5tu12.mongodb.net/terabox_downloader?appName=Cluster0")

try:
    client = MongoClient(MONGODB_URI)
    db = client.get_default_database()
    if db is None or db.name == 'test':
        db = client['terabox_downloader']
    tasks_collection = db['leechtasks']
    logger.info("Connected to MongoDB successfully.")
    
    # Auto-healing: Reset any stuck 'processing' tasks to 'pending' on startup
    reset_res = tasks_collection.update_many(
        {'status': 'processing'},
        {'$set': {'status': 'pending'}}
    )
    logger.info(f"Reset {reset_res.modified_count} stuck 'processing' tasks to 'pending' on startup.")
except Exception as e:
    logger.error(f"MongoDB connection failed: {e}")
    sys.exit(1)

# Initialize Pyrogram Bot Client
bot = Client("leech_daemon_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Video extension list
VIDEO_EXTS = ('.mp4', '.mkv', '.webm', '.avi', '.mov', '.flv', '.m4v', '.3gp')

# Size formatter helper
def format_size(bytes_val):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.2f} TB"

# Helper to get free space
def get_free_disk_space():
    total, used, free = shutil.disk_usage('.')
    return free, format_size(free)

# Helper to format status text with visual progress bar, speed, ETA, and free space
def make_progress_status(status_name, filename, current, total, start_time):
    now = time.time()
    elapsed = now - start_time
    if elapsed == 0:
        elapsed = 0.1
    
    percentage = (current / total) * 100 if total > 0 else 0
    speed = current / elapsed  # bytes per second
    
    # Progress Bar (10 Blocks)
    completed_blocks = int(percentage / 10)
    remaining_blocks = 10 - completed_blocks
    progress_bar = "■" * completed_blocks + "□" * remaining_blocks
    
    speed_str = format_size(speed) + "/s"
    current_str = format_size(current)
    total_str = format_size(total)
    
    # ETA calculator
    if speed > 0 and total > 0:
        eta_seconds = (total - current) / speed
        if eta_seconds > 3600:
            eta_str = f"{int(eta_seconds//3600)}h {int((eta_seconds%3600)//60)}m"
        elif eta_seconds > 60:
            eta_str = f"{int(eta_seconds//60)}m {int(eta_seconds%60)}s"
        else:
            eta_str = f"{int(eta_seconds)}s"
    else:
        eta_str = "Calculating..."
        
    _, free_space_str = get_free_disk_space()
        
    status_text = (
        f"⏳ <b>VPS Status: {status_name}...</b>\n"
        f"📁 <b>File:</b> <code>{filename}</code>\n\n"
        f"<code>[{progress_bar}] {percentage:.1f}%</code>\n"
        f"⚖️ <b>Size:</b> {current_str} / {total_str}\n"
        f"⚡ <b>Speed:</b> {speed_str}\n"
        f"⏱️ <b>ETA:</b> {eta_str}\n"
        f"💾 <b>VPS Free Space:</b> {free_space_str}"
    )
    return status_text

# Progress tracker class to handle safe callback throttled updates
class TaskProgress:
    def __init__(self, bot, chat_id, message_id, filename, status_name):
        self.bot = bot
        self.chat_id = chat_id
        self.message_id = message_id
        self.filename = filename
        self.status_name = status_name
        self.current = 0
        self.total = 0
        self.start_time = time.time()
        self.last_update_time = 0
        self.running = True
        self.loop = asyncio.get_event_loop()

    def update(self, current, total):
        self.current = current
        self.total = total
        now = time.time()
        # Throttled at 5 seconds interval to avoid Telegram Flood limits
        if now - self.last_update_time > 5 and self.running:
            self.last_update_time = now
            asyncio.run_coroutine_threadsafe(self.send_update(), self.loop)

    async def send_update(self):
        if not self.running:
            return
        text = make_progress_status(self.status_name, self.filename, self.current, self.total, self.start_time)
        try:
            await self.bot.edit_message_text(
                chat_id=self.chat_id,
                message_id=self.message_id,
                text=text
            )
        except Exception:
            pass

    def stop(self):
        self.running = False


async def download_file(url, local_path, progress_tracker=None):
    logger.info(f"Downloading {url} to {local_path}...")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': '*/*',
        'Referer': 'https://www.terabox.com/'
    }
    
    def do_download():
        with requests.get(url, headers=headers, stream=True, timeout=60) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0))
            
            # Disk space check with safety buffer (100MB)
            free_space, free_space_str = get_free_disk_space()
            buffer_space = 100 * 1024 * 1024  # 100 MB buffer
            
            if total_size > (free_space - buffer_space):
                raise Exception(
                    f"Insufficient disk space on VPS. "
                    f"File requires {format_size(total_size)} but only {free_space_str} is free (100MB buffer required)."
                )

            downloaded = 0
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024): # 1MB chunks
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if progress_tracker:
                            progress_tracker.update(downloaded, total_size)
                            
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, do_download)
    logger.info(f"Downloaded successfully: {local_path}")

async def process_task(task):
    task_id = task['_id']
    chat_id = int(task['chatId'])
    message_id = task.get('messageId')
    dlink = task['dlink']
    filename = task['filename']

    # Initialize trackers
    download_tracker = None
    upload_tracker = None

    logger.info(f"Processing task {task_id} for chat {chat_id} - file: {filename}")
    
    # Update status to processing
    tasks_collection.update_one({'_id': task_id}, {'$set': {'status': 'processing'}})
    
    # Create progress notification message
    try:
        progress_msg = await bot.send_message(
            chat_id=chat_id, 
            text=f"⏳ <b>VPS Status: Initializing...</b>\n📁 <code>{filename}</code>",
            reply_to_message_id=message_id
        )
        progress_msg_id = progress_msg.id
    except Exception as e:
        logger.warning(f"Could not send progress message: {e}")
        progress_msg_id = None

    # Create downloads directory
    os.makedirs("downloads", exist_ok=True)
    local_path = os.path.join("downloads", filename)

    # Tracker for Download phase
    download_tracker = None
    if progress_msg_id:
        download_tracker = TaskProgress(bot, chat_id, progress_msg_id, filename, "Downloading")

    try:
        # 1. Download Phase
        await download_file(dlink, local_path, download_tracker)
        if download_tracker:
            download_tracker.stop()

        # Update message for upload phase transition
        if progress_msg_id:
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_msg_id,
                    text=f"⏳ <b>VPS Status: Uploading...</b>\n📁 <code>{filename}</code>"
                )
            except Exception:
                pass

        # Tracker for Upload phase
        upload_tracker = None
        if progress_msg_id:
            upload_tracker = TaskProgress(bot, chat_id, progress_msg_id, filename, "Uploading")

        # Pyrogram progress callback function
        async def upload_callback(current, total):
            if upload_tracker:
                upload_tracker.update(current, total)

        # 2. Upload Phase
        logger.info(f"Uploading {local_path} to Telegram...")
        is_video = filename.lower().endswith(VIDEO_EXTS)
        
        if is_video:
            await bot.send_video(
                chat_id=chat_id,
                video=local_path,
                caption=f"📂 <b>File:</b> <code>{filename}</code>\n\nUploaded via VPS Leech Queue.",
                reply_to_message_id=message_id,
                progress=upload_callback
            )
        else:
            await bot.send_document(
                chat_id=chat_id,
                document=local_path,
                caption=f"📂 <b>File:</b> <code>{filename}</code>\n\nUploaded via VPS Leech Queue.",
                reply_to_message_id=message_id,
                progress=upload_callback
            )

        if upload_tracker:
            upload_tracker.stop()

        # Update status to completed
        tasks_collection.update_one({'_id': task_id}, {'$set': {'status': 'completed'}})
        logger.info(f"Task {task_id} completed successfully.")
        
        # Delete temporary progress message
        if progress_msg_id:
            try:
                await bot.delete_messages(chat_id, progress_msg_id)
            except Exception:
                pass

    except Exception as err:
        logger.error(f"Failed to process task {task_id}: {err}")
        if download_tracker:
            download_tracker.stop()
        if upload_tracker:
            upload_tracker.stop()
            
        # Update status to failed
        tasks_collection.update_one({'_id': task_id}, {
            '$set': {
                'status': 'failed',
                'error': str(err)
            }
        })
        
        # Notify user about failure
        try:
            error_text = f"❌ <b>VPS Error:</b> Failed to leech file <code>{filename}</code>.\nError: <code>{str(err)}</code>"
            if progress_msg_id:
                await bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text=error_text)
            else:
                await bot.send_message(chat_id=chat_id, text=error_text, reply_to_message_id=message_id)
        except Exception as msg_err:
            logger.error(f"Failed to send error notification: {msg_err}")

    finally:
        # Clean up downloaded file
        if os.path.exists(local_path):
            try:
                os.remove(local_path)
                logger.info(f"Cleaned up local file: {local_path}")
            except Exception as e:
                logger.error(f"Failed to delete local file {local_path}: {e}")

async def main():
    logger.info("Starting Leech Daemon loop...")
    async with bot:
        while True:
            try:
                # FIFO logic: find one task sorted by creation date ascending (oldest first)
                task = tasks_collection.find_one({'status': 'pending'}, sort=[('createdAt', 1)])
                if task:
                    await process_task(task)
                else:
                    await asyncio.sleep(10)
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(main())
