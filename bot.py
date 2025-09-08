from __future__ import annotations
from typing import Optional, List, Dict, Any, Union
import logging
import os
import re
import json
import random
import string
import subprocess
import time
import requests
import cloudscraper
import threading
import aiohttp
import asyncio
import schedule
from datetime import datetime, timedelta
from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import quote
from tenacity import retry, stop_after_attempt, wait_exponential
from telethon import TelegramClient, events
from telethon.tl import functions
from telethon.tl.types import InputDocumentFileLocation, InputPhotoFileLocation, DocumentAttributeVideo, InputMediaPhoto, InputMediaDocument
from telethon.errors import FloodWaitError
from telethon.errors.rpcerrorlist import WebpageMediaEmptyError
from telethon.tl.custom import Button
from telethon.tl.functions.messages import EditMessageRequest
from datetime import datetime, timedelta
import pytz
from typing import List, Union
from motor.motor_asyncio import AsyncIOMotorClient
import traceback
from telethon.tl.types import PeerUser
from dotenv import load_dotenv
import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
import http.cookiejar
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn
import base64
import shutil
from telethon import types
from pathlib import Path
from telethon.tl.types import InputPhoto
from telethon.tl.functions.photos import UploadProfilePhotoRequest

THUMBNAIL_DIR = Path("./thumbnails")
THUMBNAIL_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


try:
    from pyrofork import Client as PyroClient
    from pyrofork.types import InlineKeyboardButton, InlineKeyboardMarkup
    PYROFORK_AVAILABLE = True
    logger.info("Pyrofork imported successfully for fast uploads")
except ImportError:
    PYROFORK_AVAILABLE = False
    logger.warning("Pyrofork not available. Using standard Telethon uploads.")

if not load_dotenv():
    logger.warning("No .env file found or failed to load environment variables")
env_file = Path(".env")
if not env_file.exists():
    logger.warning(f"No .env file found at {env_file.absolute()}")
elif not env_file.read_text().strip():
    logger.warning(f".env file exists but is empty at {env_file.absolute()}")

BASE_DIR = Path.cwd()
LOG_DIR = BASE_DIR / "logs"
DOWNLOAD_DIR = BASE_DIR / "anime_downloads"
THUMBNAIL_DIR = BASE_DIR / "thumbnails"
DB_NAME = "AnimePahe" 

for directory in [LOG_DIR, DOWNLOAD_DIR, THUMBNAIL_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "bot.log"
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(LOG_FILE))
    ]
)
logger = logging.getLogger(__name__)

class Config:
    DB_URL = "mongodb+srv://nitinkumardhundhara:DARKXSIDE78@cluster0.wdive.mongodb.net/?retryWrites=true&w=majority"
    DB_NAME = "AnimePahe"
    ADMINS = [7086472788] 
    OWNER_ID = ADMINS
    FSUB_LINK_EXPIRY = 3600
    FORCE_PIC = "https://imgs.search.brave.com/251VkzPIZrlvS1S8Z9Py6nTUSmF0YOANsC0BKOiBtfk/rs:fit:860:0:0:0/g:ce/aHR0cHM6Ly93YWxs/cGFwZXJjZy5jb20v/bWVkaWEvdHNfb3Jp/Zy8xMjExMC53ZWJw"

def get_env_var(key: str, default: Any = None, required: bool = True) -> Any:
    value = os.environ.get(key, default)
    
    if required and (value is None or (isinstance(value, str) and value.strip() == "")):
        env_file = Path(".env")
        if not env_file.exists():
            raise ValueError(
                f"Environment variable {key} is required but not set.\n"
                f"Please create a .env file in {env_file.absolute()}"
            )
        else:
            raise ValueError(
                f"Environment variable {key} is required but not set.\n"
                f"Please add {key}=your_value to your .env file"
            )
    
    logger.debug(f"Loaded environment variable: {key}")
    return value

logger.info("Loading essential configuration...")
try:
    API_ID = int(get_env_var("API_ID"))
    API_HASH = get_env_var("API_HASH")
    BOT_TOKEN = get_env_var("BOT_TOKEN")
    ADMIN_CHAT_ID = int(get_env_var("ADMIN_CHAT_ID"))
    MONGO_URI = get_env_var("MONGO_URI", required=False)
    PORT = int(get_env_var("PORT", "8090"))
    BOT_USERNAME = get_env_var("BOT_USERNAME", "BlakiteX9AnimeBot")
    logger.info("Successfully loaded all environment variables")
except ValueError as e:
    logger.error(f"Environment variable error: {e}")
    raise

CHANNEL_ID = get_env_var("CHANNEL_ID", required=False)
CHANNEL_USERNAME = get_env_var("CHANNEL_USERNAME", required=False)
DUMP_CHANNEL_ID = get_env_var("DUMP_CHANNEL_ID", required=False)
DUMP_CHANNEL_USERNAME = get_env_var("DUMP_CHANNEL_USERNAME", required=False)

if CHANNEL_ID:
    try:
        CHANNEL_ID = int(CHANNEL_ID)
        logger.info(f"Channel ID configured: {CHANNEL_ID}")
    except ValueError:
        logger.warning("Invalid CHANNEL_ID provided - must be a number. Falling back to username if available.")
        CHANNEL_ID = None

if CHANNEL_USERNAME:
    if not CHANNEL_USERNAME.startswith('@'):
        CHANNEL_USERNAME = f"@{CHANNEL_USERNAME}"
    logger.info(f"Channel username configured: {CHANNEL_USERNAME}")

if DUMP_CHANNEL_ID:
    try:
        DUMP_CHANNEL_ID = int(DUMP_CHANNEL_ID)
        logger.info(f"Dump Channel ID configured: {DUMP_CHANNEL_ID}")
    except ValueError:
        logger.warning("Invalid DUMP_CHANNEL_ID provided - must be a number. Falling back to username if available.")
        DUMP_CHANNEL_ID = None

if DUMP_CHANNEL_USERNAME:
    if not DUMP_CHANNEL_USERNAME.startswith('@'):
        DUMP_CHANNEL_USERNAME = f"@{DUMP_CHANNEL_USERNAME}"
    logger.info(f"Dump channel username configured: {DUMP_CHANNEL_USERNAME}")

if not CHANNEL_ID and not CHANNEL_USERNAME:
    logger.warning("No main channel ID or username configured. Files will only be sent to dump channel.")

if not DUMP_CHANNEL_ID and not DUMP_CHANNEL_USERNAME:
    logger.warning("No dump channel ID or username configured. Files will only be sent to users directly.")

FIXED_THUMBNAIL_URL = "https://i.postimg.cc/DfLv5dJP/photo-2025-08-05-16-48-30.jpg"
START_PIC_URL = "https://imgs.search.brave.com/9n5_FAipMAH3ic1LtbHx3btylHNWppO2rl4gXnRjr1g/rs:fit:860:0:0:0/g:ce/aHR0cHM6Ly93YWxs/cGFwZXJjYXZlLmNv/bS93cC93cDE4MzM1/NTIuanBn"
STICKER_ID = "CAACAgUAAyEFAASONkiwAAIqzmgkRV65h50_3UdyXQ4r0osj7Cs2AAIfAANDc8kSq8cUT3BtY9A2BA"
FORCE_PIC = "https://imgs.search.brave.com/9n5_FAipMAH3ic1LtbHx3btylHNWppO2rl4gXnRjr1g/rs:fit:860:0:0:0/g:ce/aHR0cHM6Ly93YWxs/cGFwZXJjYXZlLmNv/bS93cC93cDE4MzM1/NTIuanBn"

def download_start_pic(url: str, save_path = THUMBNAIL_DIR / "start_pic.jpg"):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        with open(save_path, "wb") as f:
            f.write(response.content)
        print(f"[INFO] Start pic downloaded and saved as '{save_path}'")
        return str(save_path)
    except Exception as e:
        print(f"[ERROR] Failed to download start pic: {e}")
        return None

def download_start_pic_if_not_exists(url: str, save_path = THUMBNAIL_DIR / "start_pic.jpg"):
    if save_path.exists():
        print(f"[INFO] Start pic already exists at '{save_path}'")
        return str(save_path)
    return download_start_pic(url, save_path)

def download_force_pic(url: str, save_path = THUMBNAIL_DIR / "force_pic.jpg"):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        with open(save_path, "wb") as f:
            f.write(response.content)
        print(f"[INFO] Force pic downloaded and saved as '{save_path}'")
        return str(save_path)
    except Exception as e:
        print(f"[ERROR] Failed to download force pic: {e}")
        return None

def download_force_pic_if_not_exists(url: str, save_path = THUMBNAIL_DIR / "force_pic.jpg"):
    if save_path.exists():
        print(f"[INFO] Force pic already exists at '{save_path}'")
        return str(save_path)
    return download_force_pic(url, save_path)

class CodeflixBots:
    def __init__(self):
        self.client = AsyncIOMotorClient(MONGO_URI)
        self.db = self.client["file_sequence_bot"]
        self.database = self.client[DB_NAME]
        self.settings = self.db["settings"]
        self.fsub_data = self.database['fsub']   
        self.rqst_fsub_data = self.database['request_forcesub']
        self.rqst_fsub_Channel_data = self.database['request_forcesub_channel']
        self.channel_data = self.database['channels']

    async def channel_exist(self, channel_id: int):
        found = await self.fsub_data.find_one({'_id': channel_id})
        return bool(found)

    async def add_channel(self, channel_id: int):
        if not await self.channel_exist(channel_id):
            await self.fsub_data.insert_one({'_id': channel_id})
            return

    async def rem_channel(self, channel_id: int):
        if await self.channel_exist(channel_id):
            await self.fsub_data.delete_one({'_id': channel_id})
            return

    async def show_channels(self):
        channel_docs = await self.fsub_data.find().to_list(length=None)
        channel_ids = [doc['_id'] for doc in channel_docs]
        return channel_ids

    async def get_channel_mode(self, channel_id: int):
        data = await self.fsub_data.find_one({'_id': channel_id})
        return data.get("mode", "off") if data else "off"

    async def set_channel_mode(self, channel_id: int, mode: str):
        await self.fsub_data.update_one(
            {'_id': channel_id},
            {'$set': {'mode': mode}},
            upsert=True
        )

    async def req_user(self, channel_id: int, user_id: int):
        try:
            result = await self.rqst_fsub_Channel_data.update_one(
                {'_id': int(channel_id)},
                {'$addToSet': {'user_ids': int(user_id)}},
                upsert=True
            )
            logger.info(f"Recorded join request in DB for channel {channel_id}, user {user_id}. UpdateResult: {{matched:{getattr(result, 'matched_count', None)}, modified:{getattr(result, 'modified_count', None)}}}")
            return True
        except Exception as e:
            logger.error(f"[DB ERROR] Failed to add user to request list: {e}")
            return False

    async def del_req_user(self, channel_id: int, user_id: int):
        await self.rqst_fsub_Channel_data.update_one(
            {'_id': channel_id}, 
            {'$pull': {'user_ids': user_id}}
        )

    async def req_user_exist(self, channel_id: int, user_id: int):
        try:
            found = await self.rqst_fsub_Channel_data.find_one({
                '_id': int(channel_id),
                'user_ids': int(user_id)
            })
            logger.debug(f"req_user_exist check for channel {channel_id}, user {user_id}: found={bool(found)}")
            return bool(found)
        except Exception as e:
            logger.error(f"[DB ERROR] Failed to check request list: {e}")
            return False  

    async def reqChannel_exist(self, channel_id: int):
        channel_ids = await self.show_channels()
        return channel_id in channel_ids

codeflixbots = CodeflixBots()

pyro_client = None
if PYROFORK_AVAILABLE:
    try:
        pyro_client = PyroClient(
            "AnimePahe",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN
        )
        logger.info("Pyrofork client initialized for fast uploads")
    except Exception as e:
        logger.error(f"Failed to initialize Pyrofork client: {e}")
        pyro_client = None

class AnimeQueue:
    def __init__(self):
        self.pending_queue = []
        self.processing_queue = []
        self.processed_episodes = set()
        self.lock = threading.Lock()
        self.queue_file = BASE_DIR / "anime_queue.json"
        self.load_queue()
    
    def load_queue(self):
        try:
            if self.queue_file.exists():
                with open(self.queue_file, 'r') as f:
                    data = json.load(f)
                    self.pending_queue = data.get('pending', [])
                    self.processed_episodes = set(data.get('processed', []))
                    logger.info(f"Loaded {len(self.pending_queue)} pending episodes from queue")
        except Exception as e:
            logger.error(f"Error loading queue: {e}")
    
    def save_queue(self):
        try:
            with self.lock:
                data = {
                    'pending': self.pending_queue,
                    'processed': list(self.processed_episodes)
                }
                with open(self.queue_file, 'w') as f:
                    json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving queue: {e}")
    
    def add_to_pending(self, anime_info):
        with self.lock:
            episode_id = f"{anime_info['title']}_{anime_info['episode']}"
            if episode_id not in [item['id'] for item in self.pending_queue]:
                anime_info['id'] = episode_id
                anime_info['added_time'] = datetime.now().isoformat()
                self.pending_queue.append(anime_info)
                self.save_queue()
                logger.info(f"Added {episode_id} to pending queue")
                return True
        return False
    
    def get_next_pending(self):
        with self.lock:
            if self.pending_queue:
                return self.pending_queue[0]
        return None
    
    def remove_from_pending(self, episode_id):
        with self.lock:
            self.pending_queue = [item for item in self.pending_queue if item['id'] != episode_id]
            self.save_queue()
    
    def mark_as_processed(self, anime_title, episode_number):
        with self.lock:
            episode_id = f"{anime_title}_{episode_number}"
            self.processed_episodes.add(episode_id)
            self.save_queue()
    
    def is_processed(self, anime_title, episode_number):
        episode_id = f"{anime_title}_{episode_number}"
        return episode_id in self.processed_episodes
    
    def clear_old_entries(self, days=7):
        with self.lock:
            cutoff_date = datetime.now() - timedelta(days=days)
            self.pending_queue = [
                item for item in self.pending_queue
                if datetime.fromisoformat(item['added_time']) > cutoff_date
            ]
            self.save_queue()

anime_queue = AnimeQueue()

class QualitySettings:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.state: Dict[str, Any] = {
            "enabled_qualities": ["360p", "720p", "1080p"],
            "download_all": True,
            "batch_mode": False
        }
        self.load_state()
        
    def load_state(self) -> None:
        try:
            if QUALITY_SETTINGS_FILE.exists():
                with open(QUALITY_SETTINGS_FILE, 'r', encoding='utf-8') as f:
                    loaded_state = json.load(f)
                    self.state.update(loaded_state)
                logger.info("Quality settings loaded successfully")
        except json.JSONDecodeError as e:
            logger.error(f"Corrupted quality settings file: {e}")
            self._backup_corrupted_state()
        except Exception as e:
            logger.error(f"Error loading quality settings: {str(e)}")
    
    def _backup_corrupted_state(self) -> None:
        try:
            if QUALITY_SETTINGS_FILE.exists():
                backup_path = QUALITY_SETTINGS_FILE.with_suffix('.json.bak')
                QUALITY_SETTINGS_FILE.rename(backup_path)
                logger.info(f"Corrupted quality settings file backed up to {backup_path}")
        except Exception as e:
            logger.error(f"Failed to backup corrupted quality settings file: {e}")
    
    def save_state(self) -> None:
        with self._lock:
            temp_file = QUALITY_SETTINGS_FILE.with_suffix('.tmp')
            try:
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(self.state, f, indent=2)
                temp_file.replace(QUALITY_SETTINGS_FILE)
                logger.info("Quality settings saved successfully")
            except Exception as e:
                logger.error(f"Error saving quality settings: {str(e)}")
                if temp_file.exists():
                    temp_file.unlink()
    
    @property
    def enabled_qualities(self) -> List[str]:
        return self.state.get("enabled_qualities", ["360p", "720p", "1080p"])
    
    @enabled_qualities.setter
    def enabled_qualities(self, qualities: List[str]) -> None:
        self.state["enabled_qualities"] = qualities
        self.save_state()
    
    @property
    def download_all(self) -> bool:
        return self.state.get("download_all", True)
    
    @download_all.setter
    def download_all(self, value: bool) -> None:
        self.state["download_all"] = bool(value)
        self.save_state()
        
    @property
    def batch_mode(self) -> bool:
        return self.state.get("batch_mode", False)
    
    @batch_mode.setter
    def batch_mode(self, value: bool) -> None:
        self.state["batch_mode"] = bool(value)
        self.save_state()

mongo_client = None
db = None
processed_episodes_collection = None
anime_banners_collection = None
anime_hashtags_collection = None
admins_collection = None
bot_settings_collection = None

if MONGO_URI:
    try:
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = mongo_client.anime_bot
        processed_episodes_collection = db.processed_episodes
        anime_banners_collection = db.anime_banners
        anime_hashtags_collection = db.anime_hashtags
        admins_collection = db.admins
        bot_settings_collection = db.bot_settings
        
        try:
            processed_episodes_collection.create_index(
                [("anime_title", pymongo.ASCENDING), ("episode_number", pymongo.ASCENDING)], 
                unique=True
            )
            anime_banners_collection.create_index(
                [("anime_title", pymongo.ASCENDING)], 
                unique=True
            )
            anime_hashtags_collection.create_index(
                [("anime_title", pymongo.ASCENDING)], 
                unique=True
            )
            admins_collection.create_index(
                [("user_id", pymongo.ASCENDING)], 
                unique=True
            )
            bot_settings_collection.create_index(
                [("setting_name", pymongo.ASCENDING)], 
                unique=True
            )
        except Exception as e:
            logger.warning(f"Index creation failed: {e}")
        
        mongo_client.admin.command('ismaster')
        logger.info("Successfully connected to MongoDB")
    except (ConnectionFailure, OperationFailure) as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        mongo_client = None
        db = None
        processed_episodes_collection = None
        anime_banners_collection = None
        anime_hashtags_collection = None
        admins_collection = None
        bot_settings_collection = None
else:
    logger.warning("MONGO_URI not provided. Using JSON file for data storage.")
    
    JSON_DATA_FILE = BASE_DIR / "anime_data.json"
    if not JSON_DATA_FILE.exists():
        with open(JSON_DATA_FILE, 'w') as f:
            json.dump({
                "processed_episodes": [], 
                "posted_banners": [],
                "anime_hashtags": [],
                "admins": [],
                "bot_settings": {}
            }, f)

AUTO_DOWNLOAD_STATE_FILE = BASE_DIR / "auto_download_state.json"
QUALITY_SETTINGS_FILE = BASE_DIR / "quality_settings.json"
SESSION_FILE = BASE_DIR / "anime_bot.session"
FFMPEG_PATH = "ffmpeg"

HEADERS = {
    'authority': 'animepahe.ru',
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'accept-language': 'en-US,en;q=0.9',
    'cookie': '__ddg2_=;',
    'dnt': '1',
    'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="124", "Chromium";v="124"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'x-requested-with': 'XMLHttpRequest',
    'referer': 'https://animepahe.ru/',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
}

YTDLP_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Referer': 'https://kwik.cx/',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0'
}

WORKER_BASE_URL = "https://pahe-babu.raviguj4554.workers.dev/?d="

FFMPEG_AVAILABLE = False
try:
    subprocess.run([FFMPEG_PATH, "-version"], check=True, capture_output=True)
    FFMPEG_AVAILABLE = True
    logger.info("FFmpeg is available")
except (subprocess.CalledProcessError, FileNotFoundError):
    logger.warning("FFmpeg is not available. Some features may not work.")

def install_ytdlp():
    try:
        subprocess.run(["pip", "install", "yt-dlp"], check=True)
        return True
    except:
        return False

try:
    import yt_dlp
except ImportError:
    if install_ytdlp():
        import yt_dlp

def install_ffmpeg():
    try:
        subprocess.run([FFMPEG_PATH, "-version"], check=True, capture_output=True)
        return True
    except:
        try:
            logger.info("Attempting to install FFmpeg with apt-get...")
            subprocess.run(["apt-get", "update"], check=True)
            subprocess.run(["apt-get", "install", "-y", "ffmpeg"], check=True)
            subprocess.run([FFMPEG_PATH, "-version"], check=True, capture_output=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            logger.error(f"Failed to install FFmpeg with apt-get: {e}")
            try:
                logger.info("Attempting to install FFmpeg with yum...")
                subprocess.run(["yum", "install", "-y", "ffmpeg"], check=True)
                subprocess.run([FFMPEG_PATH, "-version"], check=True, capture_output=True)
                return True
            except (subprocess.CalledProcessError, FileNotFoundError) as e:
                logger.error(f"Failed to install FFmpeg with yum: {e}")
                return False

if not FFMPEG_AVAILABLE and not install_ffmpeg():
    logger.error("FFmpeg is not available. Video conversion will be skipped.")

SEARCH, SELECT_ANIME, SELECT_EPISODE, SELECT_QUALITY, DOWNLOADING = range(5)
AUTO_DISABLED, AUTO_ENABLED = range(2)
ANILIST_API = "https://graphql.anilist.co"

client = TelegramClient(str(SESSION_FILE), API_ID, API_HASH)

currently_processing = False
processing_lock = asyncio.Lock()

class BotSettings:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.state: Dict[str, Any] = {
            "start_pic": START_PIC_URL,
            "thumbnail": FIXED_THUMBNAIL_URL,
            "dump_channel_id": DUMP_CHANNEL_ID,
            "dump_channel_username": DUMP_CHANNEL_USERNAME,
            "file_delete_timer": 1800
        }
        self.load_state()
        
    def load_state(self) -> None:
        if bot_settings_collection is not None:
            try:
                for setting in bot_settings_collection.find():
                    self.state[setting["setting_name"]] = setting["setting_value"]
                logger.info("Bot settings loaded successfully from MongoDB")
            except Exception as e:
                logger.error(f"Error loading bot settings from MongoDB: {e}")
        else:
            try:
                if JSON_DATA_FILE.exists():
                    with open(JSON_DATA_FILE, 'r') as f:
                        data = json.load(f)
                        if "bot_settings" in data:
                            self.state.update(data["bot_settings"])
                    logger.info("Bot settings loaded successfully from JSON")
            except Exception as e:
                logger.error(f"Error loading bot settings: {e}")
    
    def save_state(self) -> None:
        with self._lock:
            if bot_settings_collection is not None:
                try:
                    for key, value in self.state.items():
                        bot_settings_collection.update_one(
                            {"setting_name": key},
                            {"$set": {"setting_value": value}},
                            upsert=True
                        )
                    logger.info("Bot settings saved successfully to MongoDB")
                except Exception as e:
                    logger.error(f"Error saving bot settings to MongoDB: {e}")
            else:
                try:
                    data = load_json_data()
                    data["bot_settings"] = self.state
                    save_json_data(data)
                    logger.info("Bot settings saved successfully to JSON")
                except Exception as e:
                    logger.error(f"Error saving bot settings: {e}")
    
    def get(self, key: str, default=None):
        return self.state.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        self.state[key] = value
        self.save_state()

bot_settings = BotSettings()

quality_settings = QualitySettings()

async def get_fixed_thumbnail():
    thumbnail_path = os.path.join(THUMBNAIL_DIR, "fixed_thumbnail.png")
    
    if not os.path.exists(thumbnail_path):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(FIXED_THUMBNAIL_URL) as response:
                    if response.status == 200:
                        with open(thumbnail_path, 'wb') as f:
                            f.write(await response.read())
                        logger.info("Downloaded fixed thumbnail")
        except Exception as e:
            logger.error(f"Error downloading fixed thumbnail: {e}")
    
    return thumbnail_path if os.path.exists(thumbnail_path) else None

user_states = {}

class UserState:
    def __init__(self):
        self.anime_results = None
        self.anime_session = None
        self.anime_title = None
        self.total_episodes = None
        self.episodes = None
        self.current_page = 1
        self.total_pages = None
        self.episode_session = None
        self.episode_number = None
        self.download_links = None
        self.waiting_for_interval = False
        self.last_command_time = 0
        self.progress_message = None
        self.quality_setting = False
        self.rate_limited_until = 0
        self.current_batch_page = 1

def sanitize_filename(file_name: str) -> str:
    sanitized = re.sub(r'[<>:"/\\|?*]', '', file_name)
    return sanitized.strip()

def create_short_name(name: str, max_length: int = 30) -> str:
    if len(name) > max_length:
        return ''.join(word[0].upper() for word in name.split())
    return name

def format_size(size_bytes: int) -> str:
    if not isinstance(size_bytes, (int, float)):
        return "0 B"
        
    if size_bytes < 0:
        return "0 B"
        
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024**2:
        return f"{size_bytes/1024:.2f} KB"
    elif size_bytes < 1024**3:
        return f"{size_bytes/(1024**2):.2f} MB"
    else:
        return f"{size_bytes/(1024**3):.2f} GB"

def format_speed(speed_bytes):
    if not isinstance(speed_bytes, (int, float)):
        return "0 B/s"
        
    if speed_bytes < 1024:
        return f"{speed_bytes} B/s"
    elif speed_bytes < 1024**2:
        return f"{speed_bytes/1024:.2f} KB/s"
    elif speed_bytes < 1024**3:
        return f"{speed_bytes/(1024**2):.2f} MB/s"
    else:
        return f"{speed_bytes/(1024**3):.2f} GB/s"

def format_time(seconds):
    seconds = int(seconds)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"

def format_filename(anime_title, episode_number, quality, type_str):
    season_match = re.search(r'Season (\d+)', anime_title, re.IGNORECASE)
    if season_match:
        season = f"S{int(season_match.group(1)):02d}"
    else:
        season = "S01"
    
    ep_num = f"{int(episode_number):02d}"
    
    clean_title = re.sub(r'\s*\(.*?\)\s*', '', anime_title)
    clean_title = re.sub(r'\s*\[.*?\]\s*', '', clean_title)
    clean_title = clean_title.strip()
    
    return f"[{season}-{ep_num}] {clean_title} [{quality}] [{type_str}]"

async def is_subscribed(client, user_id: int) -> bool:
    channel_ids = await codeflixbots.show_channels()
    
    if not channel_ids:
        return True
    
    if user_id == Config.OWNER_ID:
        return True
    
    for cid in channel_ids:
        if not await is_sub(client, user_id, cid):
            mode = await codeflixbots.get_channel_mode(cid)
            if mode == "on":
                await asyncio.sleep(2)
                if await is_sub(client, user_id, cid):
                    continue
            return False
    return True

async def is_sub(client, user_id: int, channel_id: int) -> bool:
    try:
        member = await client.get_permissions(channel_id, user_id)
        return True
    except Exception:
        mode = await codeflixbots.get_channel_mode(channel_id)
        if mode == "on":
            exists = await codeflixbots.req_user_exist(channel_id, user_id)
            return exists
        return False

@client.on(events.Raw(types.UpdateBotChatInviteRequester))
async def handle_join_request(event):
    logger.debug(f"Received join request event: {event}")
    try:
        channel_id = None
        user_id = event.user_id

        if hasattr(event.peer, 'channel_id'):
            channel_id = event.peer.channel_id
            logger.debug(f"Extracted channel_id from event.peer.channel_id: {channel_id}")
        elif hasattr(event.peer, 'chat_id'):
            channel_id = event.peer.chat_id
            logger.debug(f"Extracted channel_id from event.peer.chat_id: {channel_id}")
        else:
            logger.warning(f"Could not determine channel ID from join request event.peer: {event.peer} (type: {type(event.peer)})")

        if channel_id is None:
            logger.error(f"Failed to extract channel ID from event: {event}")
            return

        is_fsub_channel = await codeflixbots.reqChannel_exist(channel_id)
        logger.debug(f"Is channel {channel_id} an FSUB channel? {is_fsub_channel}")

        if is_fsub_channel:
            mode = await codeflixbots.get_channel_mode(channel_id)
            logger.debug(f"FSUB mode for channel {channel_id}: {mode}")

            if mode == "on":
                logger.info(f"Recording join request: User {user_id} requested to join channel {channel_id}")
                await codeflixbots.req_user(channel_id, user_id)
            else:
                logger.debug(f"Join request received for channel {channel_id} (mode: {mode}), not recording as mode is OFF.")
        else:
             logger.debug(f"Join request for channel {channel_id}, but it's not in the FSUB list.")

    except Exception as e:
        logger.error(f"Error handling join request event: {e}", exc_info=True)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        

async def not_joined(client, event):
    logger.info(f"not_joined called for user_id: {event.sender_id}, chat_id: {event.chat_id}")
    try:
        user_name = "User"
        try:
            user = await event.get_sender()
            logger.debug(f"Sender entity: {user}")
            if hasattr(user, 'first_name') and user.first_name:
                user_name = user.first_name
        except Exception as e:
            logger.warning(f"Could not get user details in not_joined: {e}. Using default name.")

        channel_buttons = []
        all_channels = []
        try:
            all_channels = await codeflixbots.show_channels()
            logger.debug(f"Fetched force sub channels: {all_channels}")
            if not all_channels:
                 logger.info("No force sub channels configured, allowing user.")
                 return
        except Exception as e:
            logger.error(f"Error fetching force sub channels from DB: {e}", exc_info=True)
            await safe_respond(event, "A critical error occurred (FSUB DB). Please contact admin.")
            raise events.StopPropagation

        user_id = event.sender_id

        for chat_id in all_channels:
            logger.debug(f"Checking subscription for chat_id: {chat_id}")
            is_user_sub = False
            try:
                is_user_sub = await is_sub(client, user_id, chat_id)
                logger.debug(f"Subscription status for {user_id} in {chat_id}: {is_user_sub}")
            except Exception as e:
                logger.error(f"Error checking subscription for {user_id} in {chat_id}: {e}", exc_info=True)
                is_user_sub = False

            if not is_user_sub:
                try:
                    chat = await client.get_entity(chat_id)
                    logger.debug(f"Fetched chat entity: {chat}")
                    mode = await codeflixbots.get_channel_mode(chat_id)
                    logger.debug(f"Mode for {chat_id}: {mode}")

                    link = "https://t.me/darkxside78"
                    try:
                        if mode == "on" and not (hasattr(chat, 'username') and chat.username):
                            invite = await client(
                                functions.messages.ExportChatInviteRequest(
                                    peer=chat_id,
                                    request_needed=True,
                                    expire_date=datetime.now() + timedelta(seconds=Config.FSUB_LINK_EXPIRY) if Config.FSUB_LINK_EXPIRY else None
                                )
                            )
                            link = invite.link
                        else:
                            if hasattr(chat, 'username') and chat.username:
                                link = f"https://t.me/{chat.username}"
                            else:
                                invite = await client(
                                    functions.messages.ExportChatInviteRequest(
                                        peer=chat_id,
                                        request_needed=False,
                                        expire_date=datetime.now() + timedelta(seconds=Config.FSUB_LINK_EXPIRY) if Config.FSUB_LINK_EXPIRY else None
                                    )
                                )
                                link = invite.link
                    except Exception as link_error:
                        logger.error(f"Error generating invite link for {chat_id}: {link_error}", exc_info=True)
                        if hasattr(chat, 'username') and chat.username:
                             link = f"https://t.me/{chat.username}"

                    channel_buttons.append(Button.url("Jᴏɪɴ Cʜᴀɴɴᴇʟ", link))
                    logger.debug(f"Added button for {chat.title}")

                except Exception as channel_error:
                    logger.error(f"Error processing channel {chat_id} for FSUB: {channel_error}", exc_info=True)
                    await safe_respond(event, f"Error preparing channel link ({chat_id}). Please contact admin.")
                    raise events.StopPropagation

        buttons = []
        if channel_buttons:
            if len(channel_buttons) >= 2:
                buttons.append([channel_buttons[0], channel_buttons[1]])
            elif len(channel_buttons) == 1:
                buttons.append([channel_buttons[0]])

            if len(channel_buttons) >= 3:
                buttons.append([channel_buttons[2]])

            for btn in channel_buttons[3:]:
                buttons.append([btn])

            logger.info("Sending force-subscribe message with buttons.")
            buttons.append([Button.inline("Rᴇᴛʀʏ", b"check_joined")])
            
            caption_text = f"<blockquote><b>Jᴏɪɴ ᴏᴜʀ ᴄʜᴀɴɴᴇʟ(s) ᴛᴏ ᴜsᴇ ᴛʜɪs ʙᴏᴛ.</b></blockquote>"
            try:
                force_pic_path = bot_settings.get("force_pic_path", None)
                use_fallback = False
                
                if force_pic_path and os.path.exists(force_pic_path):
                    force_media = force_pic_path
                else:
                    force_media = FORCE_PIC
                    use_fallback = True
                    
                await client.send_file(
                    event.chat_id,
                    force_media,
                    caption=caption_text,
                    parse_mode='HTML',
                    buttons=buttons,
                    force_document=False
                )
                
                logger.debug("Force-subscribe message sent successfully using send_file.")
            except WebpageMediaEmptyError as wme:
                logger.warning(f"Force pic invalid or no media found: {wme}")
                try:
                    await safe_respond(event, caption_text, buttons=buttons, parse_mode='html')
                except Exception as fallback_error:
                    logger.critical(f"Fallback safe_respond also failed: {fallback_error}", exc_info=True)
            except Exception as send_error:
                logger.error(f"Error sending force-subscribe message: {send_error}", exc_info=True)
                try:
                    await safe_respond(event, caption_text, buttons=buttons, parse_mode='html')
                except Exception as fallback_error:
                    logger.critical(f"Fallback safe_respond also failed: {fallback_error}", exc_info=True)
                     
            raise events.StopPropagation
        else:
            logger.info("User is subscribed to all channels or no channels configured, allowing.")

    except events.StopPropagation:
        logger.debug("StopPropagation raised in not_joined.")
        raise
    except Exception as e:
        logger.critical(f"Unexpected error in not_joined function: {e}", exc_info=True)
        try:
            await safe_respond(event, "A critical error occurred in the force-subscribe check. Please contact the bot admin.")
        except:
            pass
        raise events.StopPropagation

async def fast_upload_file(file_path, caption, thumb_path=None, progress_callback=None):
    global pyro_client, client
    
    dump_msg_id = None
    upload_success = False
    target_channel = DUMP_CHANNEL_ID or DUMP_CHANNEL_USERNAME
    
    if not target_channel:
        logger.warning("No dump channel configured")
        return None
    
    if PYROFORK_AVAILABLE and pyro_client:
        try:
            if not pyro_client.is_connected:
                await pyro_client.start()
                logger.info("Started Pyrofork client for fast upload")
            
            logger.info(f"Uploading with Pyrofork: {os.path.basename(file_path)}")
            
            pyro_msg = await pyro_client.send_document(
                chat_id=target_channel,
                document=file_path,
                caption=caption,
                thumb=thumb_path,
                file_name=os.path.basename(file_path),
                progress=progress_callback,
                force_document=True
            )
            
            dump_msg_id = pyro_msg.id
            upload_success = True
            logger.info(f"Fast upload completed using Pyrofork: {dump_msg_id}")
            
        except Exception as e:
            logger.error(f"Pyrofork upload failed: {e}")
            upload_success = False
    
    if not upload_success:
        try:
            logger.info(f"Uploading with Telethon: {os.path.basename(file_path)}")
            
            msg = await client.send_file(
                target_channel,
                file_path,
                caption=caption,
                thumb=thumb_path,
                force_document=True,
                attributes=None,
                supports_streaming=False,
                part_size_kb=8192
            )
            
            dump_msg_id = msg.id
            upload_success = True
            logger.info(f"Upload completed using Telethon: {dump_msg_id}")
            
        except FloodWaitError as e:
            logger.error(f"Flood wait during upload: {e.seconds} seconds")
            await asyncio.sleep(e.seconds + 5)
            try:
                msg = await client.send_file(
                    target_channel,
                    file_path,
                    caption=caption,
                    thumb=thumb_path,
                    force_document=True,
                    attributes=None,
                    supports_streaming=False,
                    part_size_kb=8192 
                )
                dump_msg_id = msg.id
                upload_success = True
            except Exception as retry_error:
                logger.error(f"Upload retry failed: {retry_error}")
        except Exception as e:
            logger.error(f"Telethon upload failed: {e}")
    
    return dump_msg_id if upload_success else None

class ProgressMessage:
    def __init__(self, client, chat_id, initial_text, parse_mode='html'):
        self.client = client
        self.chat_id = chat_id
        self.message_id = None
        self.initial_text = initial_text
        self.parse_mode = parse_mode
        self.last_update_time = 0
        self.min_interval = 10
        self.flood_wait_count = 0
        self.max_flood_waits = 3
    
    async def send(self):
        try:
            msg = await self.client.send_message(
                self.chat_id, 
                self.initial_text, 
                parse_mode=self.parse_mode
            )
            self.message_id = msg.id
            self.last_update_time = time.time()
            return True
        except FloodWaitError as e:
            wait_time = e.seconds + 5
            logger.warning(f"Flood wait on initial message: {wait_time} seconds")
            await asyncio.sleep(wait_time)
            return await self.send()
        except Exception as e:
            logger.error(f"Error sending progress message: {e}")
            return False
    
    async def update(self, text):
        current_time = time.time()
        
        if current_time - self.last_update_time < self.min_interval:
            return
            
        if current_time - self.last_update_time > 30:
            self.flood_wait_count = 0
            
        if not self.message_id:
            if not await self.send():
                return
                
        try:
            await self.client.edit_message(
                self.chat_id, 
                self.message_id, 
                text, 
                parse_mode=self.parse_mode
            )
            self.last_update_time = current_time
            self.flood_wait_count = 0
        except FloodWaitError as e:
            self.flood_wait_count += 1
            wait_time = e.seconds + 5
            logger.warning(f"Flood wait {self.flood_wait_count}/{self.max_flood_waits}: {wait_time} seconds")
            
            if self.flood_wait_count >= self.max_flood_waits:
                logger.warning("Too many flood waits, stopping progress updates")
                return
                
            await asyncio.sleep(wait_time)
            try:
                await self.client.edit_message(
                    self.chat_id, 
                    self.message_id, 
                    text, 
                    parse_mode=self.parse_mode
                )
                self.last_update_time = current_time
            except Exception as e:
                logger.error(f"Error editing after flood wait: {e}")
                await self._send_new(text)
        except Exception as e:
            logger.error(f"Error updating progress: {e}")
            await self._send_new(text)
    
    async def _send_new(self, text):
        try:
            msg = await self.client.send_message(
                self.chat_id, 
                text, 
                parse_mode=self.parse_mode
            )
            self.message_id = msg.id
            self.last_update_time = current_time
        except Exception as e:
            logger.error(f"Error sending new progress message: {e}")

class UploadProgressBar:
    def __init__(self, client, chat_id, name):
        self.client = client
        self.chat_id = chat_id
        self.name = name
        self.start_time = time.time()
        self.last_update = 0
        self.message = None
        self.cancelled = False
    
    async def update(self, current, total):
        if self.cancelled:
            return
            
        now = time.time()
        if (now - self.last_update) >= 3 or current == total:
            self.last_update = now
            percent = round(current / total * 100, 2) if total > 0 else 0
            speed = current / (now - self.start_time) if (now - self.start_time) > 0 else 0
            eta = round((total - current) / speed) if speed > 0 else 0
            bar_length = 20
            filled_length = int(round(bar_length * current / float(total))) if total > 0 else 0
            bar = "█" * filled_length + '▒' * (bar_length - filled_length)
            
            progress_str = f"""<blockquote><b>‣ Anime Name : <i>{self.name}</i></b></blockquote>
            
<blockquote><b>‣ Status : </b><i>Uploading</i>
<code>[{bar}] {percent}%</code></blockquote>

<blockquote><b>    ‣ Size : </b> {format_size(current)} / {format_size(total)}
<b>    ‣ Speed : </b> {format_speed(speed)}
<b>    ‣ Time Took : </b> {format_time(now - self.start_time)}
<b>    ‣ Time Left : </b> {format_time(eta)}</blockquote>"""
            
            if self.message:
                try:
                    await self.client.edit_message(self.chat_id, self.message.id, progress_str, parse_mode='html')
                except Exception as e:
                    logger.error(f"Error updating upload progress: {e}")
                    try:
                        self.message = await self.client.send_message(self.chat_id, progress_str, parse_mode='html')
                    except Exception as e:
                        logger.error(f"Error sending new progress message: {e}")
            else:
                try:
                    self.message = await self.client.send_message(self.chat_id, progress_str, parse_mode='html')
                except Exception as e:
                    logger.error(f"Error sending initial progress message: {e}")
    
    async def finish(self):
        if self.message:
            try:
                await self.client.delete_messages(self.chat_id, [self.message.id])
            except Exception as e:
                logger.error(f"Error finishing upload progress: {e}")
    
    def cancel(self):
        self.cancelled = True

async def safe_edit(event, text, **kwargs):
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            return await event.edit(text, **kwargs)
        except FloodWaitError as e:
            retry_count += 1
            wait_time = e.seconds + (5 * retry_count)
            logger.warning(f"Flood wait (attempt {retry_count}/{max_retries}): {wait_time} seconds")
            await asyncio.sleep(wait_time)
        except Exception as e:
            logger.error(f"Error editing message: {e}")
            try:
                return await event.respond(text, **kwargs)
            except Exception as e:
                logger.error(f"Error sending fallback message: {e}")
                return None
    
    logger.error(f"Max retries ({max_retries}) reached for editing message")
    return None

async def safe_respond(event, text, **kwargs):
    try:
        return await event.respond(text, **kwargs)
    except FloodWaitError as e:
        logger.warning(f"Flood wait: {e.seconds} seconds")
        await asyncio.sleep(e.seconds + 1)
        try:
            return await event.respond(text, **kwargs)
        except Exception as e:
            logger.error(f"Error responding after flood wait: {e}")
            return None
    except Exception as e:
        logger.error(f"Error responding: {e}")
        return None

async def safe_send_message(chat_id, text, **kwargs):
    try:
        return await client.send_message(chat_id, text, **kwargs)
    except FloodWaitError as e:
        logger.warning(f"Flood wait: {e.seconds} seconds")
        await asyncio.sleep(e.seconds + 1)
        try:
            return await client.send_message(chat_id, text, **kwargs)
        except Exception as e:
            logger.error(f"Error sending message after flood wait: {e}")
            return None
    except Exception as e:
        logger.error(f"Error sending message: {e}")
        return None

class AutoDownloadState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.state: Dict[str, Any] = {
            "enabled": False,
            "last_checked": None,
            "processed_episodes": [],
            "interval_seconds": 300,
            "retry_attempts": 3,
            "retry_delay": 300,
            "processing_info": {
                "title": None,
                "episode": None,
                "quality": None,
                "status": None
            }
        }
        self.load_state()
        
    def get_interval(self) -> int:
        return self.state["interval_seconds"]
    
    def load_state(self) -> None:
        try:
            if AUTO_DOWNLOAD_STATE_FILE.exists():
                with open(AUTO_DOWNLOAD_STATE_FILE, 'r', encoding='utf-8') as f:
                    loaded_state = json.load(f)
                    self.state.update(loaded_state)
                logger.info("Auto download state loaded successfully")
        except json.JSONDecodeError as e:
            logger.error(f"Corrupted state file: {e}")
            self._backup_corrupted_state()
        except Exception as e:
            logger.error(f"Error loading auto download state: {str(e)}")
    
    def _backup_corrupted_state(self) -> None:
        try:
            if AUTO_DOWNLOAD_STATE_FILE.exists():
                backup_path = AUTO_DOWNLOAD_STATE_FILE.with_suffix('.json.bak')
                AUTO_DOWNLOAD_STATE_FILE.rename(backup_path)
                logger.info(f"Corrupted state file backed up to {backup_path}")
        except Exception as e:
            logger.error(f"Failed to backup corrupted state file: {e}")
    
    def save_state(self) -> None:
        with self._lock:
            temp_file = AUTO_DOWNLOAD_STATE_FILE.with_suffix('.tmp')
            try:
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(self.state, f, indent=2)
                temp_file.replace(AUTO_DOWNLOAD_STATE_FILE)
                logger.info("Auto download state saved successfully")
            except Exception as e:
                logger.error(f"Error saving auto download state: {str(e)}")
                if temp_file.exists():
                    temp_file.unlink()
    
    @property
    def enabled(self) -> bool:
        return self.state.get("enabled", False)
    
    @enabled.setter
    def enabled(self, value: bool) -> None:
        self.state["enabled"] = bool(value)
        self.save_state()
    
    @property
    def interval(self) -> int:
        return self.state.get("interval_seconds", 300)
    
    @interval.setter
    def interval(self, seconds: int) -> None:
        if not isinstance(seconds, int) or seconds <= 0:
            raise ValueError("Interval must be a positive integer")
        self.state["interval_seconds"] = seconds
        self.save_state()
    
    @property
    def last_checked(self) -> Optional[str]:
        return self.state.get("last_checked")
    
    @last_checked.setter
    def last_checked(self, timestamp: Optional[str]) -> None:
        self.state["last_checked"] = timestamp
        self.save_state()

auto_download_state = AutoDownloadState()

def load_json_data():
    try:
        with open(JSON_DATA_FILE, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"processed_episodes": [], "posted_banners": [], "anime_hashtags": [], "admins": [], "bot_settings": {}}

def save_json_data(data):
    with open(JSON_DATA_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def is_admin(user_id: int) -> bool:
    if user_id == ADMIN_CHAT_ID:
        return True
    
    if admins_collection is not None:
        try:
            result = admins_collection.find_one({"user_id": user_id})
            return result is not None
        except Exception as e:
            logger.error(f"Error checking admin status: {e}")
            return False
    else:
        data = load_json_data()
        for admin in data.get("admins", []):
            if admin["user_id"] == user_id:
                return True
        return False

def add_admin(user_id: int, username: str = None) -> bool:
    if admins_collection is not None:
        try:
            admins_collection.update_one(
                {"user_id": user_id},
                {"$set": {
                    "username": username,
                    "added_at": datetime.now()
                }},
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error adding admin: {e}")
            return False
    else:
        data = load_json_data()
        
        for admin in data.get("admins", []):
            if admin["user_id"] == user_id:
                return True
        
        data.setdefault("admins", []).append({
            "user_id": user_id,
            "username": username,
            "added_at": datetime.now().isoformat()
        })
        
        save_json_data(data)
        return True

def remove_admin(user_id: int) -> bool:
    if admins_collection is not None:
        try:
            result = admins_collection.delete_one({"user_id": user_id})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"Error removing admin: {e}")
            return False
    else:
        data = load_json_data()
        admins = data.get("admins", [])
        
        for i, admin in enumerate(admins):
            if admin["user_id"] == user_id:
                admins.pop(i)
                save_json_data(data)
                return True
        
        return False

def is_episode_processed(anime_title: str, episode_number: int) -> bool:
    if processed_episodes_collection is not None:
        try:
            result = processed_episodes_collection.find_one({
                "anime_title": anime_title,
                "episode_number": episode_number
            })
            if result:
                processed_qualities = set(result.get("qualities", []))
                enabled_qualities = set(quality_settings.enabled_qualities)
                return processed_qualities.issuperset(enabled_qualities)
        except Exception as e:
            logger.error(f"Error checking processed episode: {e}")
            return False
    else:
        data = load_json_data()
        for ep in data["processed_episodes"]:
            if ep["anime_title"] == anime_title and ep["episode_number"] == episode_number:
                processed_qualities = set(ep.get("qualities", []))
                enabled_qualities = set(quality_settings.enabled_qualities)
                return processed_qualities.issuperset(enabled_qualities)
    return False

def update_processed_qualities(anime_title: str, episode_number: int, quality: str) -> bool:
    if processed_episodes_collection is not None:
        try:
            doc = processed_episodes_collection.find_one({
                "anime_title": anime_title,
                "episode_number": episode_number
            })
            
            if doc:
                current_qualities = set(doc.get("qualities", []))
                current_qualities.add(quality)
                processed_episodes_collection.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {
                        "qualities": list(current_qualities),
                        "updated_at": datetime.now()
                    }}
                )
            else:
                processed_episodes_collection.insert_one({
                    "anime_title": anime_title,
                    "episode_number": episode_number,
                    "qualities": [quality],
                    "created_at": datetime.now()
                })
            return True
        except Exception as e:
            logger.error(f"Error updating processed qualities: {e}")
            return False
    else:
        data = load_json_data()

        entry_exists = False
        for ep in data["processed_episodes"]:
            if ep["anime_title"] == anime_title and ep["episode_number"] == episode_number:
                if quality not in ep["qualities"]:
                    ep["qualities"].append(quality)
                    ep["updated_at"] = datetime.now().isoformat()
                entry_exists = True
                break
        
        if not entry_exists:
            data["processed_episodes"].append({
                "anime_title": anime_title,
                "episode_number": episode_number,
                "qualities": [quality],
                "created_at": datetime.now().isoformat()
            })
        
        save_json_data(data)
        return True

def mark_episode_processed(anime_title: str, episode_number: int, qualities: List[str]) -> bool:
    if processed_episodes_collection is not None:
        try:
            processed_episodes_collection.update_one(
                {"anime_title": anime_title, "episode_number": episode_number},
                {"$set": {
                    "qualities": qualities,
                    "processed_at": datetime.now()
                }},
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error marking episode as processed: {e}")
            return False
    else:
        data = load_json_data()
        data["processed_episodes"].append({
            "anime_title": anime_title,
            "episode_number": episode_number,
            "qualities": qualities,
            "processed_at": datetime.now().isoformat()
        })
        save_json_data(data)
        return True

def is_banner_posted(anime_title: str) -> bool:
    if anime_banners_collection is not None:
        try:
            result = anime_banners_collection.find_one({"anime_title": anime_title})
            return result is not None
        except Exception as e:
            logger.error(f"Error checking banner posted: {e}")
            return False
    else:
        data = load_json_data()
        for banner in data["posted_banners"]:
            if banner["anime_title"] == anime_title:
                return True
        return False

def mark_banner_posted(anime_title: str) -> bool:
    if anime_banners_collection is not None:
        try:
            anime_banners_collection.update_one(
                {"anime_title": anime_title},
                {"$set": {"posted_at": datetime.now()}},
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error marking banner posted: {e}")
            return False
    else:
        data = load_json_data()
        data["posted_banners"].append({
            "anime_title": anime_title,
            "posted_at": datetime.now().isoformat()
        })
        save_json_data(data)
        return True

def get_anime_hashtag(anime_title: str) -> str:
    if anime_hashtags_collection is not None:
        try:
            result = anime_hashtags_collection.find_one({"anime_title": anime_title})
            if result:
                return result["hashtag"]
        except Exception as e:
            logger.error(f"Error getting anime hashtag: {e}")
    else:
        data = load_json_data()
        for hashtag_data in data["anime_hashtags"]:
            if hashtag_data["anime_title"] == anime_title:
                return hashtag_data["hashtag"]
    
    predefined_hashtags = {
        "Lord of the Mysteries": "LOTM",
        "Nukitashi THE ANIMATION": "NTA",
        "ONE PIECE": "OP",
        "There's No Freaking Way I'll be Your Lover! Unless...": "Freaking",
        "Grand Blue Dreaming Season 2": "GB",
        "SAKAMOTO DAYS Part 2": "SD",
        "Sword of the Demon Hunter: Kijin Gentosho": "KG",
        "Solo Leveling": "SL",
        "Jujutsu Kaisen": "JK",
        "Demon Slayer": "DS",
        "Attack on Titan": "AOT",
        "My Hero Academia": "MHA",
        "Naruto": "NAR",
        "Bleach": "BL",
        "Dragon Ball Z": "DBZ",
        "Death Note": "DN",
        "One Punch Man": "OPM",
        "Tokyo Ghoul": "TG",
        "Black Clover": "BC",
        "Hunter x Hunter": "HXH",
        "Fullmetal Alchemist": "FMA",
        "Steins;Gate": "SG",
        "Re:Zero": "RZ",
        "No Game No Life": "NGL",
        "The Rising of the Shield Hero": "SH",
        "Sword Art Online": "SAO",
        "Fairy Tail": "FT",
        "Dragon Ball Super": "DBS",
        "Boruto": "BOR",
        "Dragon Ball": "DB",
        "One Piece Film: Red": "OPR",
        "Demon Slayer: Entertainment District Arc": "DS2",
        "Attack on Titan Final Season": "AOT4",
        "Jujutsu Kaisen 0": "JK0",
        "My Hero Academia: Heroes Rising": "MHA2",
        "Fate/Zero": "FZ",
        "Fate/stay night": "FSN",
        "Fate/stay night: Unlimited Blade Works": "UBW",
        "Fate/Apocrypha": "FA",
        "Fate/Grand Order": "FGO",
        "Fate/kaleid liner Prisma Illya": "PI",
        "Fate/hollow ataraxia": "FH",
        "Fate/EXTRA": "FE",
        "Fate/strange Fake": "FSF",
        "Fate/Prototype": "FP",
        "Fate/Labyrinth": "FL",
        "Fate/side material": "FSM",
        "Fate/complete material": "FCM",
        "Fate/extra material": "FEM",
        "Fate/Apocrypha material": "FAM",
        "Fate/Grand Order material": "FGOM",
        "Fate/Zero material": "FZM",
        "Fate/stay night material": "FSNM",
        "Fate/hollow ataraxia material": "FHM",
        "Fate/EXTRA material material": "FEMMMM",
        "Fate/strange Fake material material": "FSFMMM",
        "Fate/Prototype material material": "FPMMM",
        "Fate/Labyrinth material material": "FLMMM",
    }
    
    for title, hashtag in predefined_hashtags.items():
        if title.lower() in anime_title.lower():
            if anime_hashtags_collection is not None:
                try:
                    anime_hashtags_collection.update_one(
                        {"anime_title": anime_title},
                        {"$set": {"hashtag": hashtag}},
                        upsert=True
                    )
                except Exception as e:
                    logger.error(f"Error storing anime hashtag: {e}")
            else:
                data = load_json_data()
                data["anime_hashtags"].append({
                    "anime_title": anime_title,
                    "hashtag": hashtag,
                    "created_at": datetime.now().isoformat()
                })
                save_json_data(data)
            
            return hashtag
    
    words = re.findall(r'\b\w+\b', anime_title)
    common_words = {'the', 'a', 'an', 'and', 'or', 'but', 'of', 'to', 'in', 'on', 'at', 'for', 'with', 'by', 'from', 'up', 'about', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'between', 'among', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'shall', 'should', 'may', 'might', 'must', 'can', 'could'}
    
    filtered_words = [word for word in words if word.lower() not in common_words and len(word) > 2]
    
    if filtered_words:
        hashtag = ''.join([word[0].upper() for word in filtered_words[:3]])
    else:
        hashtag = words[0][:3].upper() if words else "ANI"
    
    if anime_hashtags_collection is not None:
        try:
            anime_hashtags_collection.update_one(
                {"anime_title": anime_title},
                {"$set": {"hashtag": hashtag}},
                upsert=True
            )
        except Exception as e:
            logger.error(f"Error storing anime hashtag: {e}")
    else:
        data = load_json_data()
        data["anime_hashtags"].append({
            "anime_title": anime_title,
            "hashtag": hashtag,
            "created_at": datetime.now().isoformat()
        })
        save_json_data(data)
    
    return hashtag

async def encode(string: str) -> str:
    string_bytes = string.encode("ascii")
    base64_bytes = base64.b64encode(string_bytes)
    base64_string = base64_bytes.decode("ascii")
    return base64_string

async def generate_batch_link(file_ids: List[int], quality: str) -> str:
    if not file_ids:
        return None
    
    first_id = min(file_ids)
    last_id = max(file_ids)
    batch_string = f"get-{first_id}-{last_id}"
    encoded_string = await encode(batch_string)
    
    bot_username = BOT_USERNAME
    return f"https://t.me/{bot_username}?start={encoded_string}"

async def generate_single_link(file_id: int) -> str:
    string = f"get-{file_id}"
    encoded_string = await encode(string)
    
    bot_username = BOT_USERNAME
    return f"https://t.me/{bot_username}?start={encoded_string}"

async def post_anime_with_buttons(client, anime_title, anime_info, episode_number, audio_type, quality_files):
    if not CHANNEL_ID and not CHANNEL_USERNAME:
        logger.warning("No main channel configured. Banner not posted.")
        return None
    
    anime_id = anime_info.get('id')
    if not anime_id:
        logger.error(f"No anime ID found for {anime_title}")
        return None
    
    banner_url = f"https://img.anili.st/media/{anime_id}"
    
    banner_path = os.path.join(THUMBNAIL_DIR, f"{sanitize_filename(anime_title)}_banner.jpg")
    async with aiohttp.ClientSession() as session:
        async with session.get(banner_url) as response:
            if response.status == 200:
                with open(banner_path, 'wb') as f:
                    f.write(await response.read())
            else:
                logger.error(f"Failed to download banner for {anime_title}")
                return None
    
    english_title = anime_info.get('title', {}).get('english') or anime_info.get('title', {}).get('romaji')
    romaji_title = anime_info.get('title', {}).get('romaji')
    japanese_title = anime_info.get('title', {}).get('native')
    
    hashtag = get_anime_hashtag(anime_title)
    
    main_channel_username = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
    
    qualities_str = " | ".join(quality_files.keys())
    caption = (
        f"<blockquote><b><i> {english_title} | {romaji_title}\n#{hashtag}_OngoingArc</i></b></blockquote>\n"
        f"<b>──────────────────────</b>\n"
        f"<blockquote><b>❍ Episode: {episode_number if episode_number else 'N/A'}</b>\n"
        f"<b>◇ Quality: {qualities_str}</b>\n"
        f"<b>〄 Audio: {audio_type}</b></blockquote>\n"
        f"<b>──────────────────────</b>\n"
        f"<b><blockquote>ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{main_channel_username}'>𝗢𝗻𝗴𝗼𝗶𝗻𝗴 𝗔𝗻𝗶𝗺𝗲- 𝗔𝗿𝗰</a></blockquote></b>"
    )

    buttons = []
    for quality, file_ids in quality_files.items():
        batch_link = await generate_batch_link(file_ids, quality)
        if batch_link:
            buttons.append(Button.url(quality, batch_link))

    keyboard = []
    for i in range(0, len(buttons), 2):
        row = buttons[i:i+2]
        keyboard.append(row)

    if not keyboard:
        keyboard = None

    try:
        if CHANNEL_ID:
            msg = await client.send_file(
                CHANNEL_ID,
                banner_path,
                caption=caption,
                parse_mode='html',
                buttons=keyboard
            )
        elif CHANNEL_USERNAME:
            msg = await client.send_file(
                CHANNEL_USERNAME,
                banner_path,
                caption=caption,
                parse_mode='html',
                buttons=keyboard
            )
        logger.info(f"Posted banner with buttons for {anime_title}")

        try:
            await client.send_message(
                CHANNEL_ID if CHANNEL_ID else CHANNEL_USERNAME,
                file=STICKER_ID
            )
            logger.info("Sent sticker after banner")
        except Exception as e:
            logger.error(f"Error sending sticker: {e}")
        
        mark_banner_posted(anime_title)
        return msg
    except Exception as e:
        logger.error(f"Error posting banner with buttons: {e}")
        return None
    finally:
        try:
            os.remove(banner_path)
        except:
            pass

async def post_anime_batch_with_buttons(client, anime_title, anime_info, quality_files):
    if not CHANNEL_ID and not CHANNEL_USERNAME:
        logger.warning("No main channel configured. Banner not posted.")
        return None
    
    anime_id = anime_info.get('id')
    if not anime_id:
        logger.error(f"No anime ID found for {anime_title}")
        return None
    
    banner_url = f"https://img.anili.st/media/{anime_id}"
    
    banner_path = os.path.join(THUMBNAIL_DIR, f"{sanitize_filename(anime_title)}_banner.jpg")
    async with aiohttp.ClientSession() as session:
        async with session.get(banner_url) as response:
            if response.status == 200:
                with open(banner_path, 'wb') as f:
                    f.write(await response.read())
            else:
                logger.error(f"Failed to download banner for {anime_title}")
                return None

    english_title = anime_info.get('title', {}).get('english') or anime_info.get('title', {}).get('romaji')
    romaji_title = anime_info.get('title', {}).get('romaji')
    japanese_title = anime_info.get('title', {}).get('native')
    
    hashtag = get_anime_hashtag(anime_title)
    
    main_channel_username = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
    total_episodes = 0
    for quality, file_ids in quality_files.items():
        total_episodes = max(total_episodes, len(file_ids))

    qualities_str = " | ".join(quality_files.keys())
    caption = (
        f"<blockquote><b><i> {english_title} | {romahi_title}\n#{hashtag}_OngoingArc</i></b></blockquote>\n"
        f"<b>──────────────────────</b>\n"
        f"<blockquote><b>❍ Episode: {episode_number if episode_number else 'N/A'}</b>\n"
        f"<b>◇ Quality: {qualities_str}</b>\n"
        f"<b>〄 Audio: {audio_type}</b></blockquote>\n"
        f"<b>──────────────────────</b>\n"
        f"<b><blockquote>ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{main_channel_username}'>𝗢𝗻𝗴𝗼𝗶𝗻𝗴 𝗔𝗻𝗶𝗺𝗲- 𝗔𝗿𝗰</a></blockquote></b>"
    )

    buttons = []
    for quality, file_ids in quality_files.items():
        batch_link = await generate_batch_link(file_ids, quality)
        if batch_link:
            buttons.append(Button.url(quality, batch_link))
    
    keyboard = []
    for i in range(0, len(buttons), 2):
        row = buttons[i:i+2]
        keyboard.append(row)
    
    if not keyboard:
        keyboard = None
    
    try:
        if CHANNEL_ID:
            msg = await client.send_file(
                CHANNEL_ID,
                banner_path,
                caption=caption,
                parse_mode='html',
                buttons=keyboard
            )
        elif CHANNEL_USERNAME:
            msg = await client.send_file(
                CHANNEL_USERNAME,
                banner_path,
                caption=caption,
                parse_mode='html',
                buttons=keyboard
            )
        logger.info(f"Posted batch banner with buttons for {anime_title}")
        
        try:
            await client.send_message(
                CHANNEL_ID if CHANNEL_ID else CHANNEL_USERNAME,
                file=STICKER_ID
            )
            logger.info("Sent sticker after banner")
        except Exception as e:
            logger.error(f"Error sending sticker: {e}")
        
        mark_banner_posted(anime_title)
        return msg
    except Exception as e:
        logger.error(f"Error posting batch banner with buttons: {e}")
        return None
    finally:
        try:
            os.remove(banner_path)
        except:
            pass

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True
)
async def search_anime(query: str) -> Optional[List[Dict[str, Any]]]:
    search_url = f"https://animepahe.ru/api?m=search&q={quote(query)}"
    
    async with aiohttp.ClientSession() as session:
        async with session.get(search_url, headers=HEADERS) as response:
            response.raise_for_status()
            data = await response.json()
            
            if data.get('total', 0) == 0:
                return None
            
            return data.get('data', [])

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True
)
async def get_episode_list(session_id: str, page: int = 1) -> Dict[str, Any]:
    episodes_url = f"https://animepahe.ru/api?m=release&id={session_id}&sort=episode_asc&page={page}"
    
    async with aiohttp.ClientSession() as session:
        async with session.get(episodes_url, headers=HEADERS) as response:
            response.raise_for_status()
            return await response.json()

def get_latest_releases(page=1):
    releases_url = f"https://animepahe.ru/api?m=airing&page={page}"
    response = requests.get(releases_url, headers=HEADERS).json()
    return response

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=4, max=10),
    reraise=True
)
def get_download_links(anime_session, episode_session):
    if '-' in episode_session:
        episode_url = f"https://animepahe.ru/play/{episode_session}"
    else:
        episode_url = f"https://animepahe.ru/play/{anime_session}/{episode_session}"
    
    try:
        session = requests.Session()
        session.headers.update(HEADERS)
        time.sleep(random.uniform(2, 5))
        local_headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }
        session.headers.update(local_headers)
        session.get("https://animepahe.ru/")
        logger.info(f"Fetching episode page: {episode_url}")
        response = session.get(episode_url)
        response.raise_for_status()
        
        for parser in ['lxml', 'html.parser', 'html5lib']:
            try:
                soup = BeautifulSoup(response.content, parser)
                break
            except:
                continue
        
        links = []
        
        selectors = [
            "#pickDownload a.dropdown-item",
            "#downloadMenu a",
            "a[download]",
            "a.btn-download",
            "a[href*='download']",
            ".download-wrapper a"
        ]
        
        for selector in selectors:
            elements = soup.select(selector)
            if elements:
                logger.info(f"Found {len(elements)} links with selector: {selector}")
                for element in elements:
                    href = element.get('href') or element.get('data-url') or element.get('data-href')
                    if href:
                        if not href.startswith('http'):
                            href = f"https://animepahe.ru{href}"
                        links.append({
                            'text': element.get_text(strip=True),
                            'href': href
                        })
        
        if not links:
            for a in soup.find_all('a', href=True):
                href = a['href']
                text = a.get_text(strip=True)
                if any(keyword in href.lower() or keyword in text.lower() 
                      for keyword in ['download', 'kwik.si', 'video', 'player']):
                    if not href.startswith('http'):
                        href = f"https://animepahe.ru{href}"
                    links.append({
                        'text': text or 'Download',
                        'href': href
                    })
        
        if links:
            logger.info(f"Found {len(links)} download links")
            return links
        
        logger.error(f"No download links found for episode {episode_url}")
        logger.debug(f"Page content sample: {response.text[:1000]}")
        return None
        
    except Exception as e:
        logger.error(f"Error getting download links: {str(e)}")
        logger.error(f"URL attempted: {episode_url}")
        return None

def step_2(s, seperator, base=10):
    mapped_range = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ+/"
    numbers = mapped_range[0:base]
    max_iter = 0
    for index, value in enumerate(s[::-1]):
        max_iter += int(value if value.isdigit() else 0) * (seperator**index)
    mid = ''
    while max_iter > 0:
        mid = numbers[int(max_iter % base)] + mid
        max_iter = (max_iter - (max_iter % base)) / base
    return mid or '0'

def step_1(data, key, load, seperator):
    payload = ""
    i = 0
    seperator = int(seperator)
    load = int(load)
    while i < len(data):
        s = ""
        while data[i] != key[seperator]:
            s += data[i]
            i += 1
        for index, value in enumerate(key):
            s = s.replace(value, str(index))
        payload += chr(int(step_2(s, seperator, 10)) - load)
        i += 1
    payload = re.findall(
        r'action="([^\"]+)" method="POST"><input type="hidden" name="_token"\s+value="([^\"]+)', payload
    )[0]
    return payload

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True
)
def get_dl_link(link):
    try:
        time.sleep(random.uniform(1, 3))

        scraper = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False},
            interpreter='nodejs'
        )

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0'
        }
        
        scraper.get("https://animepahe.ru/", headers=headers)
        
        resp = scraper.get(link, headers=headers)
        
        patterns = [
            r'\("([^"]+)",(\d+),"([^"]+)",(\d+),(\d+)',
            r'\("(\S+)",\d+,"(\S+)",(\d+),(\d+)'
        ]
        
        match = None
        for pattern in patterns:
            match = re.search(pattern, resp.text)
            if match:
                break
        
        if not match:
            logger.error(f"Could not find required pattern in response from {link}")
            return None
        
        if len(match.groups()) == 5:
            data, _, key, load, seperator = match.groups()
        else:
            data, key, load, seperator = match.groups()
        
        url, token = step_1(data=data, key=key, load=load, seperator=seperator)

        post_url = url if url.startswith('http') else f"https://kwik.si{url}"
        data = {"_token": token}
        post_headers = {
            'referer': link,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://kwik.si'
        }
        
        resp = scraper.post(url=post_url, data=data, headers=post_headers, allow_redirects=False)
        
        if 'location' in resp.headers:
            direct_link = resp.headers["location"]
            return WORKER_BASE_URL + direct_link
        
        resp = scraper.post(url=post_url, data=data, headers=post_headers, allow_redirects=True)
        
        if resp.url != post_url and not resp.url.startswith('https://kwik.si/'):
            direct_link = resp.url
            return WORKER_BASE_URL + direct_link
        
        return None
        
    except Exception as e:
        logger.error(f"Error getting direct link: {str(e)}")
        raise

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True
)
def extract_kwik_link(url):
    try:
        time.sleep(random.uniform(1, 3))
        
        scraper = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False},
            interpreter='nodejs'
        )
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
            'Referer': 'https://animepahe.ru/'
        }
        
        scraper.get("https://animepahe.ru/", headers=headers)
        
        response = scraper.get(url, headers=headers)
        response.raise_for_status()
        
        logger.info(f"Got response from {url}, status code: {response.status_code}")
        
        for parser in ['lxml', 'html.parser', 'html5lib']:
            try:
                soup = BeautifulSoup(response.text, parser)
                logger.info(f"Parsed with {parser}")
                break
            except Exception as e:
                logger.warning(f"Parser {parser} failed: {str(e)}")
                continue
        
        for script in soup.find_all('script'):
            if script.string:
                match = re.search(r'https://kwik\.si/f/[\w\d-]+', script.string)
                if match:
                    return match.group(0)
        
        download_elements = soup.select('a[href*="kwik.si"], a[onclick*="kwik.si"]')
        for element in download_elements:
            href = element.get('href') or element.get('onclick', '')
            match = re.search(r'https://kwik\.si/f/[\w\d-]+', href)
            if match:
                return match.group(0)
        
        page_text = str(soup)
        matches = re.findall(r'https://kwik\.si/f/[\w\d-]+', page_text)
        if matches:
            return matches[0]
        
        return None
    except Exception as e:
        logger.error(f"Error extracting kwik link: {str(e)}")
        raise

async def get_anime_info(title: str) -> Dict[str, Any]:
    query = """
    query ($search: String) {
        Media (search: $search, type: ANIME) {
            id
            title {
                romaji
                english
                native
            }
            description
            coverImage {
                large
            }
            format
            episodes
            duration
            status
            season
            seasonYear
            source
            genres
        }
    }
    """
    
    variables = {
        'search': title
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(ANILIST_API, json={'query': query, 'variables': variables}) as response:
                data = await response.json()
                if data.get('data', {}).get('Media'):
                    return data['data']['Media']
    except Exception as e:
        logger.error(f"Error fetching anime info from AniList: {e}")
    
    return None

async def download_anime_poster(anime_title: str) -> Optional[str]:
    try:
        anime_info = await get_anime_info(anime_title)
        if not anime_info:
            return None
        
        poster_url = anime_info.get('coverImage', {}).get('large')
        if not poster_url:
            return None

        async with aiohttp.ClientSession() as session:
            async with session.get(poster_url) as response:
                if response.status == 200:
                    poster_path = os.path.join(THUMBNAIL_DIR, f"{sanitize_filename(anime_title)}.jpg")
                    with open(poster_path, 'wb') as f:
                        f.write(await response.read())
                    return poster_path
    except Exception as e:
        logger.error(f"Error downloading anime poster: {e}")
    
    return None

async def rename_video_with_ffmpeg(input_path: str, output_path: str) -> bool:
    if not FFMPEG_AVAILABLE:
        logger.warning("FFmpeg not available. Skipping video conversion.")
        return False
        
    try:
        cmd = [
            FFMPEG_PATH,
            '-i', input_path,
            '-c', 'copy',
            output_path
        ]
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode == 0:
            return True
        else:
            logger.warning(f"FFmpeg error: {stderr.decode()}")
            return False
    except Exception as e:
        logger.warning(f"Error renaming video with FFmpeg: {e}")
        return False

async def get_all_episodes(anime_session):
    all_episodes = []
    page = 1
    while True:
        episode_data = await get_episode_list(anime_session, page)
        if not episode_data or 'data' not in episode_data:
            break
        episodes = episode_data['data']
        all_episodes.extend(episodes)
        if page >= episode_data.get('last_page', 1):
            break
        page += 1
    return all_episodes

def find_closest_episode(episodes, target_episode):
    try:
        target = int(target_episode)
    except (ValueError, TypeError):
        return None
    
    valid_episodes = []
    for ep in episodes:
        try:
            ep_num = int(ep['episode'])
            valid_episodes.append((ep_num, ep))
        except (ValueError, TypeError):
            continue
    
    if not valid_episodes:
        return None
    
    valid_episodes.sort(key=lambda x: x[0])
    
    closest = None
    for ep_num, ep in valid_episodes:
        if ep_num <= target:
            closest = ep
        else:
            break
    
    if closest is None and valid_episodes:
        closest = valid_episodes[0][1]
    
    return closest

async def download_anime_batch(event, anime_session, anime_title):
    logger.info(f"Starting batch download for {anime_title}")
    
    try:
        progress = ProgressMessage(client, event.chat_id, f"<b>Starting batch download for:</b> <i>{anime_title}</i>")
        if not await progress.send():
            await safe_respond(event, "Failed to initialize progress tracking")
            return False
        
        await progress.update(f"<b>Fetching all episodes for:</b> <i>{anime_title}</i>")
        episodes = await get_all_episodes(anime_session)
        if not episodes:
            logger.error(f"Failed to get episode list for {anime_title}")
            await progress.update("Failed to get episode list")
            return False
        
        total_episodes = len(episodes)
        logger.info(f"Found {total_episodes} episodes for {anime_title}")
        
        enabled_qualities = quality_settings.enabled_qualities
        sorted_qualities = sorted(enabled_qualities, key=lambda x: int(x[:-1]))
        total_qualities = len(sorted_qualities)
        
        quality_files = {}
        for quality in sorted_qualities:
            quality_files[quality] = []
        
        for quality_idx, quality in enumerate(sorted_qualities):
            quality_progress = quality_idx + 1
            await progress.update(
                f"<b>Processing quality:</b> <i>{quality}</i> ({quality_progress}/{total_qualities})\n\n"
                f"<b>Anime:</b> <i>{anime_title}</i>\n\n"
                f"<b>Total episodes:</b> <i>{total_episodes}</i>"
            )
            
            for ep_idx, episode in enumerate(episodes):
                episode_number = episode['episode']
                episode_session = episode['session']
                episode_title = episode['title']
                
                await progress.update(
                    f"<b>Processing quality:</b> <i>{quality}</i> ({quality_progress}/{total_qualities})\n\n"
                    f"<b>Anime:</b> <i>{anime_title}</i>\n\n"
                    f"<b>Episode:</b> <i>{episode_number} - {episode_title}</i> ({ep_idx+1}/{total_episodes})"
                )
                
                download_links = get_download_links(anime_session, episode_session)
                if not download_links:
                    logger.error(f"No download links found for {anime_title} Episode {episode_number}")
                    continue
                
                is_dub = any('eng' in link['text'].lower() for link in download_links)
                audio_type = "Dub" if is_dub else "Sub"

                quality_link = None
                for link in download_links:
                    if quality in link['text']:
                        quality_link = link
                        break
                
                if not quality_link:
                    logger.error(f"Quality {quality} not found for {anime_title} Episode {episode_number}")
                    continue

                base_name = format_filename(anime_title, episode_number, quality, audio_type)
                main_channel_username = CHANNEL_USERNAME if CHANNEL_USERNAME else BOT_USERNAME
                full_caption = f"{base_name} {main_channel_username}"
                filename = sanitize_filename(full_caption) + ".mkv"
                download_path = os.path.join(DOWNLOAD_DIR, filename)
                
                kwik_link = extract_kwik_link(quality_link['href'])
                if not kwik_link:
                    logger.error(f"Failed to extract kwik link for {quality}")
                    continue
                
                direct_link = get_dl_link(kwik_link)
                if not direct_link:
                    logger.error(f"Failed to get direct link for {quality}")
                    continue
                
                try:
                    ydl_opts = {
                        'outtmpl': download_path,
                        'quiet': True,
                        'no_warnings': True,
                        'http_headers': YTDLP_HEADERS,
                        'downloader_args': {'chunk_size': 10485760},
                        'nocheckcertificate': True,
                        'compat_opts': ['no-keep-video'],
                    }
                    
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        ydl.download([direct_link])
                    
                    if not os.path.exists(download_path) or os.path.getsize(download_path) < 1000:
                        logger.error(f"Downloaded file is too small or doesn't exist for {quality}")
                        continue

                    if FFMPEG_AVAILABLE:
                        final_path = os.path.join(DOWNLOAD_DIR, f"EP{episode_number:02d} - {anime_title} [{quality}].mkv")
                        if await rename_video_with_ffmpeg(download_path, final_path):
                            os.remove(download_path)
                            download_path = final_path
                    
                    caption = full_caption
                    
                    try:
                        thumb = await get_fixed_thumbnail()
                        
                        upload_progress = UploadProgressBar(client, event.chat_id, full_caption)
                        
                        dump_msg_id = None
                        if DUMP_CHANNEL_ID:
                            try:
                                msg = await client.send_file(
                                    DUMP_CHANNEL_ID,
                                    download_path,
                                    caption=caption,
                                    thumb=thumb,
                                    force_document=True,
                                    attributes=None,
                                    supports_streaming=False,
                                    progress_callback=upload_progress.update,
                                    part_size_kb=8192
                                )

                                await upload_progress.finish()
                                
                                dump_msg_id = msg.id
                                logger.info(f"Sent to dump channel {DUMP_CHANNEL_ID}")
                            except Exception as e:
                                logger.error(f"Failed to send to dump channel: {e}")
                        elif DUMP_CHANNEL_USERNAME:
                            try:
                                msg = await client.send_file(
                                    DUMP_CHANNEL_USERNAME,
                                    download_path,
                                    caption=caption,
                                    thumb=thumb,
                                    force_document=True,
                                    attributes=None,
                                    supports_streaming=False,
                                    progress_callback=upload_progress.update,
                                    part_size_kb=8192
                                )
                                
                                await upload_progress.finish()
                                
                                dump_msg_id = msg.id
                                logger.info(f"Sent to dump channel {DUMP_CHANNEL_USERNAME}")
                            except Exception as e:
                                logger.error(f"Failed to send to dump channel: {e}")
                        else:
                            logger.warning("No dump channel configured. File not uploaded.")
                        
                        if dump_msg_id:
                            quality_files[quality].append(dump_msg_id)
                        
                        logger.info(f"Successfully uploaded {anime_title} Episode {episode_number} {quality}")
                        
                    except FloodWaitError as e:
                        logger.error(f"Flood wait error: {e.seconds} seconds")
                        await asyncio.sleep(e.seconds + 5)
                    except Exception as e:
                        logger.error(f"Eʀʀᴏʀ sᴇɴᴅɪɴɢ ᴠɪᴅᴇᴏ: {e}")
                    
                    try:
                        os.remove(download_path)
                    except:
                        pass
                
                except Exception as e:
                    logger.error(f"Error processing {quality} for episode {episode_number}: {e}")
        
        for episode in episodes:
            episode_number = episode['episode']
            mark_episode_processed(anime_title, episode_number, sorted_qualities)
        
        if quality_files and any(quality_files.values()):
            anime_info = await get_anime_info(anime_title)
            if anime_info:
                await post_anime_batch_with_buttons(client, anime_title, anime_info, quality_files)
        
        await progress.update(f"<b>Batch download completed for:</b> <i>{anime_title}</i>")
        return True
    
    except Exception as e:
        logger.error(f"Error in batch download for {anime_title}: {e}")
        await safe_respond(event, f"<b>Error in batch download:</b> <i>{str(e)}</i>", parse_mode='html')
        return False

async def auto_download_latest_episode():
    global currently_processing
    
    logger.info("Starting auto download process...")
    
    if currently_processing:
        logger.info("Already processing an episode. Skipping auto check.")
        return False
    
    currently_processing = True
    
    progress = None
    if ADMIN_CHAT_ID:
        progress = ProgressMessage(client, ADMIN_CHAT_ID, "<b>Auto processing started...</b>")
        await progress.send()
    
    try:
        if auto_download_state.last_checked:
            last_check = datetime.fromisoformat(auto_download_state.last_checked)
            time_since_last_check = (datetime.now() - last_check).total_seconds()
            
            cooldown_period = auto_download_state.interval / 2
            if time_since_last_check < cooldown_period:
                logger.info(f"Skipping auto check, last check was {time_since_last_check:.1f} seconds ago")
                return False
        
        if progress:
            await progress.update("<b>Checking for new episodes...</b>")
        
        latest_data = get_latest_releases(page=1)
        if not latest_data or 'data' not in latest_data:
            logger.error("Failed to get latest releases")
            if progress:
                await progress.update("<b>Failed to get latest releases</b>")
            return False
        
        latest_anime = latest_data['data'][0]
        anime_title = latest_anime.get('anime_title', 'Unknown Anime')
        episode_number = latest_anime.get('episode', 0)
        
        logger.info(f"Latest airing anime: {anime_title} Episode {episode_number}")
        
        if progress:
            await progress.update(f"<b>Found:</b> <i>{anime_title}</i> <b>Episode</b> <i>{episode_number}</i>")

        if is_episode_processed(anime_title, episode_number):
            logger.info(f"Episode {episode_number} of {anime_title} already processed. Skipping.")
            if progress:
                await progress.update(f"<b>{anime_title} Episode {episode_number}</b> already processed")
            return True
        
        search_results = await search_anime(anime_title)
        if not search_results:
            logger.error(f"Anime not found: {anime_title}")
            if progress:
                await progress.update(f"<b>Anime not found:</b> <i>{anime_title}</i>")
            return False
        
        anime_info = search_results[0]
        anime_session = anime_info['session']
        
        episodes = await get_all_episodes(anime_session)
        if not episodes:
            logger.error(f"Failed to get episode list for {anime_title}")
            if progress:
                await progress.update(f"❌ <b>Failed to get episode list for:</b> <i>{anime_title}</i>")
            return False
        
        target_episode = None
        for ep in episodes:
            try:
                if int(ep['episode']) == episode_number:
                    target_episode = ep
                    break
            except (ValueError, TypeError):
                continue
        
        if not target_episode:
            logger.warning(f"Episode {episode_number} not found for {anime_title}. Looking for closest available.")
            target_episode = find_closest_episode(episodes, episode_number)
            if target_episode:
                actual_episode = int(target_episode['episode'])
                logger.info(f"Found closest episode: {actual_episode}")
                episode_number = actual_episode
            else:
                logger.error(f"No episodes found for {anime_title}")
                if progress:
                    await progress.update(f"<b>No episodes found for:</b> <i>{anime_title}</i>")
                return False
        
        episode_session = target_episode['session']
        
        download_links = get_download_links(anime_session, episode_session)
        if not download_links:
            logger.error(f"No download links found for {anime_title} Episode {episode_number}")
            if progress:
                await progress.update(f"<b>No download links found for:</b> <i>{anime_title} Episode {episode_number}</i>")
            return False
        
        enabled_qualities = quality_settings.enabled_qualities

        if progress:
            await progress.update(f"<b>Checking quality availability for:</b> <i>{anime_title} Episode {episode_number}</i>")
        
        available_qualities = []
        missing_qualities = []
        
        for quality in enabled_qualities:
            quality_available = False
            for link in download_links:
                if quality in link['text']:
                    available_qualities.append(quality)
                    quality_available = True
                    break
            if not quality_available:
                missing_qualities.append(quality)
        
        if missing_qualities:
            logger.warning(f"Not all qualities available for {anime_title} Episode {episode_number}")
            logger.warning(f"Available qualities: {available_qualities}")
            logger.warning(f"Missing qualities: {missing_qualities}")
            
            queue_info = {
                'title': anime_title,
                'episode': episode_number,
                'session': anime_session,
                'episode_session': episode_session,
                'available_qualities': available_qualities,
                'missing_qualities': missing_qualities,
                'audio_type': "Dub" if is_dub else "Sub"
            }
            
            anime_queue.add_to_pending(queue_info)
            
            if progress:
                await progress.update(
                    f"⚠<b>Added to queue</b> <i>{anime_title} Episode {episode_number}</i>\n"
                    f"<b>Available qualities:</b> <i>{', '.join(available_qualities)}</i>\n"
                    f"<b>Missing qualities:</b> <i>{', '.join(missing_qualities)}</i>\n\n"
                    f"<b>Added to pending queue. Will check again later.</b>"
                )
            
            if ADMIN_CHAT_ID:
                await safe_send_message(
                    ADMIN_CHAT_ID,
                    f"⚠<b>Added to queue</b> <i>{anime_title} Episode {episode_number}</i>\n"
                    f"<b>Available qualities:</b> <i>{', '.join(available_qualities)}</i>\n"
                    f"<b>Missing qualities:</b> <i>{', '.join(missing_qualities)}</i>\n\n"
                    f"<b>Bot will process when all qualities are available.</b>\n"
                    f"<b>Queue size:</b> <i>{len(anime_queue.pending_queue)} episodes</i>",
                    parse_mode='html'
                )
            
            return await check_and_process_next_episode(progress)
        
        logger.info(f"All qualities available for {anime_title} Episode {episode_number}: {available_qualities}")
        
        if progress:
            await progress.update(
                f"<b>All qualities available for:</b> <i>{anime_title} Episode {episode_number}</i>\n"
                f"<b>Qualities:</b> <i>{', '.join(available_qualities)}</i>\n\n"
                f"<b>Starting download...</b>"
            )
        
        is_dub = any('eng' in link['text'].lower() for link in download_links)
        audio_type = "Dub" if is_dub else "Sub"
        
        sorted_qualities = sorted(enabled_qualities, key=lambda x: int(x[:-1]))
        
        downloaded_qualities = []
        quality_files = {}
        
        for quality_idx, quality in enumerate(sorted_qualities):
            try:
                logger.info(f"Downloading {anime_title} Episode {episode_number} {quality}")
                
                if progress:
                    await progress.update(
                        f"<b>Processing:</b> <i>{anime_title} Episode {episode_number}</i>\n"
                        f"<b>Quality:</b> <i>{quality}</i> ({quality_idx + 1}/{len(sorted_qualities)})\n"
                        f"<b>Audio:</b> <i>{audio_type}</i>\n\n"
                        f"<b>Status:</b> <i>Finding download link...</i>"
                    )
                
                quality_link = None
                for link in download_links:
                    if quality in link['text']:
                        quality_link = link
                        break
                
                if not quality_link:
                    logger.error(f"Quality {quality} not found for {anime_title} Episode {episode_number}")
                    continue
                
                base_name = format_filename(anime_title, episode_number, quality, "Sub" if not is_dub else "Dub")
                main_channel_username = CHANNEL_USERNAME if CHANNEL_USERNAME else BOT_USERNAME
                full_caption = f"{base_name} {main_channel_username}"
                filename = sanitize_filename(full_caption) + ".mkv"
                download_path = os.path.join(DOWNLOAD_DIR, filename)

                if progress:
                    await progress.update(
                        f"<b>Processing:</b> <i>{anime_title} Episode {episode_number}</i>\n"
                        f"<b>Quality:</b> <i>{quality}</i> ({quality_idx + 1}/{len(sorted_qualities)})\n"
                        f"<b>Audio:</b> <i>{audio_type}</i>\n\n"
                        f"<b>Status:</b> <i>Extracting direct link...</i>"
                    )
                
                kwik_link = extract_kwik_link(quality_link['href'])
                if not kwik_link:
                    logger.error(f"Failed to extract kwik link for {quality}")
                    continue
                
                direct_link = get_dl_link(kwik_link)
                if not direct_link:
                    logger.error(f"Failed to get direct link for {quality}")
                    continue
                
                if progress:
                    await progress.update(
                        f"<b>Processing:</b> <i>{anime_title} Episode {episode_number}</i>\n"
                        f"<b>Quality:</b> <i>{quality}</i> ({quality_idx + 1}/{len(sorted_qualities)})\n"
                        f"<b>Audio:</b> <i>{audio_type}</i>\n\n"
                        f"<b>Status:</b> <i>Starting optimized download...</i>"
                    )
                
                last_update = time.time()
                download_start = time.time()
                
                def progress_hook(d):
                    nonlocal last_update
                    if d['status'] == 'downloading':
                        current_time = time.time()
                        if current_time - last_update >= 3:
                            downloaded_bytes = d.get('downloaded_bytes')
                            total_bytes = d.get('total_bytes')
                            speed = d.get('speed')
                            
                            downloaded = downloaded_bytes if downloaded_bytes is not None else 0
                            total = total_bytes if total_bytes is not None else 1
                            speed_val = speed if speed is not None else 0
                            
                            try:
                                downloaded = int(downloaded)
                                total = int(total)
                                speed_val = float(speed_val)
                            except (ValueError, TypeError):
                                downloaded = 0
                                total = 1
                                speed_val = 0.0

                            if total > 0:
                                percent = min(100, (downloaded / total) * 100)
                            else:
                                percent = 0
                            
                            if progress:
                                progress_text = (
                                    f"<b>Processing:</b> <i>{anime_title} Episode {episode_number}</i>\n"
                                    f"<b>Quality:</b> <i>{quality}</i> ({quality_idx + 1}/{len(sorted_qualities)})\n"
                                    f"<b>Audio:</b> <i>{audio_type}</i>\n\n"
                                    f"<b>Downloading:</b> <i>{percent:.1f}%</i>\n"
                                    f"<b>Size:</b> <i>{format_size(downloaded)}/{format_size(total)}</i>\n"
                                    f"<b>Speed:</b> <i>{format_speed(speed_val)}</i>"
                                )
                                
                                try:
                                    asyncio.create_task(progress.update(progress_text))
                                except:
                                    pass
                            
                            last_update = current_time
                
                ydl_opts_optimized = {
                    'outtmpl': download_path,
                    'quiet': True,
                    'no_warnings': True,
                    'http_headers': YTDLP_HEADERS,
                    'progress_hooks': [progress_hook],
                    'nocheckcertificate': True,
                    'compat_opts': ['no-keep-video'],
                    'concurrent_fragment_downloads': 16,
                    'fragment_retries': 10,
                    'retries': 10,
                    'downloader_args': {
                        'chunk_size': 16777216,
                        'connections': 16,
                        'continue_dl': True
                    },
                    'socket_timeout': 30,
                    'buffersize': 16384,
                }
                
                download_success = False
                try:
                    ydl_opts_aria = {
                        'outtmpl': download_path,
                        'quiet': True,
                        'no_warnings': True,
                        'http_headers': YTDLP_HEADERS,
                        'progress_hooks': [progress_hook],
                        'nocheckcertificate': True,
                        'compat_opts': ['no-keep-video'],
                        'external_downloader': 'aria2c',
                        'external_downloader_args': {
                            'aria2c': [
                                '--max-connection-per-server=16',
                                '--max-concurrent-downloads=8', 
                                '--split=8',
                                '--min-split-size=1M',
                                '--max-download-limit=0',
                                '--file-allocation=none',
                                '--continue=true',
                                '--auto-file-renaming=false',
                                '--allow-overwrite=true',
                                '--max-tries=5',
                                '--retry-wait=3',
                                '--timeout=60',
                                '--connect-timeout=30'
                            ]
                        }
                    }
                    
                    with yt_dlp.YoutubeDL(ydl_opts_aria) as ydl:
                        ydl.download([direct_link])
                    download_success = True
                    logger.info(f"Downloaded {quality} using aria2c successfully")
                    
                except Exception as aria_error:
                    logger.warning(f"Aria2c not available or failed: {aria_error}")
                    try:
                        with yt_dlp.YoutubeDL(ydl_opts_optimized) as ydl:
                            ydl.download([direct_link])
                        download_success = True
                        logger.info(f"Downloaded {quality} using optimized yt-dlp")
                    except Exception as fallback_error:
                        logger.warning(f"Optimized Dᴏᴡɴʟᴏᴀᴅ ғᴀɪʟᴇᴅ: {fallback_error}")
                        ydl_opts_basic = {
                            'outtmpl': download_path,
                            'quiet': True,
                            'no_warnings': True,
                            'http_headers': YTDLP_HEADERS,
                            'progress_hooks': [progress_hook],
                            'nocheckcertificate': True,
                            'compat_opts': ['no-keep-video'],
                            'downloader_args': {'chunk_size': 10485760},
                        }
                        
                        with yt_dlp.YoutubeDL(ydl_opts_basic) as ydl:
                            ydl.download([direct_link])
                        download_success = True
                
                if not os.path.exists(download_path) or os.path.getsize(download_path) < 1000:
                    logger.error(f"Downloaded file is too small or doesn't exist for {quality}")
                    continue
                
                if FFMPEG_AVAILABLE:
                    final_path = os.path.join(DOWNLOAD_DIR, f"EP{episode_number:02d} - {anime_title} [{quality}].mkv")
                    if await rename_video_with_ffmpeg(download_path, final_path):
                        os.remove(download_path)
                        download_path = final_path
                
                caption = full_caption
                
                try:
                    thumb = await get_fixed_thumbnail()
                    
                    dump_msg_id = None
                    if DUMP_CHANNEL_ID:
                        try:
                            msg = await client.send_file(
                                DUMP_CHANNEL_ID,
                                download_path,
                                caption=caption,
                                thumb=thumb,
                                force_document=True,
                                attributes=None,
                                supports_streaming=False,
                                part_size_kb=8192
                            )
                            
                            dump_msg_id = msg.id
                            logger.info(f"Sent to dump channel {DUMP_CHANNEL_ID}")
                        except Exception as e:
                            logger.error(f"Failed to send to dump channel: {e}")
                    elif DUMP_CHANNEL_USERNAME:
                        try:
                            msg = await client.send_file(
                                DUMP_CHANNEL_USERNAME,
                                download_path,
                                caption=caption,
                                thumb=thumb,
                                force_document=True,
                                attributes=None,
                                supports_streaming=False,
                                part_size_kb=8192
                            )
                            
                            dump_msg_id = msg.id
                            logger.info(f"Sent to dump channel {DUMP_CHANNEL_USERNAME}")
                        except Exception as e:
                            logger.error(f"Failed to send to dump channel: {e}")
                    else:
                        logger.warning("No dump channel configured. File not uploaded.")
                    
                    if dump_msg_id:
                        if quality not in quality_files:
                            quality_files[quality] = []
                        quality_files[quality].append(dump_msg_id)
                    
                    update_processed_qualities(anime_title, episode_number, quality)
                    downloaded_qualities.append(quality)
                    logger.info(f"Successfully uploaded {quality} version")
                    
                except FloodWaitError as e:
                    logger.error(f"Flood wait error: {e.seconds} seconds")
                    await asyncio.sleep(e.seconds + 5)
                except Exception as e:
                    logger.error(f"Eʀʀᴏʀ sᴇɴᴅɪɴɢ ᴠɪᴅᴇᴏ: {e}")
                
                try:
                    os.remove(download_path)
                except:
                    pass
                
            except Exception as e:
                logger.error(f"Error processing {quality}: {e}")
        
        if quality_files:
            anime_info = await get_anime_info(anime_title)
            if anime_info:
                await post_anime_with_buttons(client, anime_title, anime_info, episode_number, audio_type, quality_files)
        
        if is_episode_processed(anime_title, episode_number):
            logger.info(f"All qualities processed for {anime_title} Episode {episode_number}")
            
            if ADMIN_CHAT_ID:
                await safe_send_message(
                    ADMIN_CHAT_ID,
                    f"<b>Successfully processed</b> <i>{anime_title}</i> <b>Episode</b> <i>{episode_number}</i>\n"
                    f"<b>Qualities:</b> <i>{', '.join(downloaded_qualities)}</i>",
                    parse_mode='html'
                )
            return True
        else:
            logger.error(f"Not all qualities downloaded for {anime_title} Episode {episode_number}")
            if ADMIN_CHAT_ID:
                await safe_send_message(
                    ADMIN_CHAT_ID,
                    f"⚠<b>Partially processed</b> <i>{anime_title}</i> <b>Episode</b> <i>{episode_number}</i>\n"
                    f"<b>Downloaded qualities:</b> <i>{', '.join(downloaded_qualities)}</i>\n"
                    f"<b>Missing qualities:</b> <i>{', '.join(set(enabled_qualities) - set(downloaded_qualities))}</i>",
                    parse_mode='html'
                )
            return False
    
    except Exception as e:
        logger.error(f"Error in auto download process: {e}")
        if ADMIN_CHAT_ID:
            await safe_send_message(
                ADMIN_CHAT_ID,
                f"<b>Error in auto download process:</b> <i>{str(e)}</i>",
                parse_mode='html'
            )
        return False
    finally:
        currently_processing = False

async def process_latest_airing_anime():
    global currently_processing
    
    logger.info("Processing latest airing anime with all qualities...")
    
    if currently_processing:
        logger.info("Already processing an episode. Skipping auto check.")
        return False
    
    currently_processing = True
    try:
        latest_data = get_latest_releases(page=1)
        if not latest_data or 'data' not in latest_data:
            logger.error("Failed to get latest releases")
            return False
        
        latest_anime = latest_data['data'][0]
        anime_title = latest_anime.get('anime_title', 'Unknown Anime')
        episode_number = latest_anime.get('episode', 0)
        
        logger.info(f"Latest airing anime: {anime_title} Episode {episode_number}")
        
        if is_episode_processed(anime_title, episode_number):
            logger.info(f"Episode {episode_number} of {anime_title} already processed. Skipping.")
            return True
        
        search_results = await search_anime(anime_title)
        if not search_results:
            logger.error(f"Anime not found: {anime_title}")
            return False
        
        anime_info = search_results[0]
        anime_session = anime_info['session']

        episodes = await get_all_episodes(anime_session)
        if not episodes:
            logger.error(f"Failed to get episode list for {anime_title}")
            return False

        target_episode = None
        for ep in episodes:
            try:
                if int(ep['episode']) == episode_number:
                    target_episode = ep
                    break
            except (ValueError, TypeError):
                continue

        if not target_episode:
            logger.warning(f"Episode {episode_number} not found for {anime_title}. Looking for closest available.")
            target_episode = find_closest_episode(episodes, episode_number)
            if target_episode:
                actual_episode = int(target_episode['episode'])
                logger.info(f"Found closest episode: {actual_episode}")
                episode_number = actual_episode
            else:
                logger.error(f"No episodes found for {anime_title}")
                return False
        
        episode_session = target_episode['session']
        
        download_links = get_download_links(anime_session, episode_session)
        if not download_links:
            logger.error(f"No download links found for {anime_title} Episode {episode_number}")
            return False
        
        enabled_qualities = quality_settings.enabled_qualities
        
        is_dub = any('eng' in link['text'].lower() for link in download_links)
        audio_type = "Dub" if is_dub else "Sub"

        sorted_qualities = sorted(enabled_qualities, key=lambda x: int(x[:-1]))
        
        downloaded_qualities = []
        quality_files = {}
        
        for quality in sorted_qualities:
            try:
                logger.info(f"Downloading {anime_title} Episode {episode_number} {quality}")
                
                quality_link = None
                for link in download_links:
                    if quality in link['text']:
                        quality_link = link
                        break
                
                if not quality_link:
                    logger.error(f"Quality {quality} not found for {anime_title} Episode {episode_number}")
                    continue
                
                base_name = format_filename(anime_title, episode_number, quality, "Sub" if not is_dub else "Dub")
                main_channel_username = CHANNEL_USERNAME if CHANNEL_USERNAME else BOT_USERNAME
                full_caption = f"{base_name} {main_channel_username}"
                filename = sanitize_filename(full_caption) + ".mkv"
                download_path = os.path.join(DOWNLOAD_DIR, filename)
                
                kwik_link = extract_kwik_link(quality_link['href'])
                if not kwik_link:
                    logger.error(f"Failed to extract kwik link for {quality}")
                    continue
                
                direct_link = get_dl_link(kwik_link)
                if not direct_link:
                    logger.error(f"Failed to get direct link for {quality}")
                    continue
                
                ydl_opts = {
                    'outtmpl': download_path,
                    'quiet': True,
                    'no_warnings': True,
                    'http_headers': YTDLP_HEADERS,
                    'downloader_args': {'chunk_size': 10485760},
                    'nocheckcertificate': True,
                    'compat_opts': ['no-keep-video'],
                }
                
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([direct_link])

                if not os.path.exists(download_path) or os.path.getsize(download_path) < 1000:
                    logger.error(f"Downloaded file is too small or doesn't exist for {quality}")
                    continue

                if FFMPEG_AVAILABLE:
                    final_path = os.path.join(DOWNLOAD_DIR, f"EP{episode_number:02d} - {anime_title} [{quality}].mkv")
                    if await rename_video_with_ffmpeg(download_path, final_path):
                        os.remove(download_path)
                        download_path = final_path
                
                caption = full_caption
                
                try:
                    thumb = await get_fixed_thumbnail()

                    dump_msg_id = None
                    if DUMP_CHANNEL_ID:
                        try:
                            msg = await client.send_file(
                                DUMP_CHANNEL_ID,
                                download_path,
                                caption=caption,
                                thumb=thumb,
                                force_document=True,
                                attributes=None,
                                supports_streaming=False,
                                part_size_kb=8192
                            )
                            
                            dump_msg_id = msg.id
                            logger.info(f"Sent to dump channel {DUMP_CHANNEL_ID}")
                        except Exception as e:
                            logger.error(f"Failed to send to dump channel: {e}")
                    elif DUMP_CHANNEL_USERNAME:
                        try:
                            msg = await client.send_file(
                                DUMP_CHANNEL_USERNAME,
                                download_path,
                                caption=caption,
                                thumb=thumb,
                                force_document=True,
                                attributes=None,
                                supports_streaming=False,
                                part_size_kb=8192
                            )
                            
                            dump_msg_id = msg.id
                            logger.info(f"Sent to dump channel {DUMP_CHANNEL_USERNAME}")
                        except Exception as e:
                            logger.error(f"Failed to send to dump channel: {e}")
                    else:
                        logger.warning("No dump channel configured. File not uploaded.")

                    if dump_msg_id:
                        if quality not in quality_files:
                            quality_files[quality] = []
                        quality_files[quality].append(dump_msg_id)
                    
                    update_processed_qualities(anime_title, episode_number, quality)
                    downloaded_qualities.append(quality)
                    logger.info(f"Successfully uploaded {quality} version")
                    
                except FloodWaitError as e:
                    logger.error(f"Flood wait error: {e.seconds} seconds")
                    await asyncio.sleep(e.seconds + 5)
                except Exception as e:
                    logger.error(f"Eʀʀᴏʀ sᴇɴᴅɪɴɢ ᴠɪᴅᴇᴏ: {e}")
                
                try:
                    os.remove(download_path)
                except:
                    pass
                
            except Exception as e:
                logger.error(f"Error processing {quality}: {e}")
        
        if quality_files:
            anime_info = await get_anime_info(anime_title)
            if anime_info:
                await post_anime_with_buttons(client, anime_title, anime_info, episode_number, audio_type, quality_files)

        if is_episode_processed(anime_title, episode_number):
            logger.info(f"All qualities processed for {anime_title} Episode {episode_number}")

            if ADMIN_CHAT_ID:
                await safe_send_message(
                    ADMIN_CHAT_ID,
                    f"<b>Successfully processed</b> <i>{anime_title}</i> <b>Episode</b> <i>{episode_number}</i>\n"
                    f"<b>Qualities:</b> <i>{', '.join(downloaded_qualities)}</i>",
                    parse_mode='html'
                )
            return True
        else:
            logger.error(f"Not all qualities downloaded for {anime_title} Episode {episode_number}")
            if ADMIN_CHAT_ID:
                await safe_send_message(
                    ADMIN_CHAT_ID,
                    f"⚠<b>Partially processed</b> <i>{anime_title}</i> <b>Episode</b> <i>{episode_number}</i>\n"
                    f"<b>Downloaded qualities:</b> <i>{', '.join(downloaded_qualities)}</i>\n"
                    f"<b>Missing qualities:</b> <i>{', '.join(set(enabled_qualities) - set(downloaded_qualities))}</i>",
                    parse_mode='html'
                )
            return False
    
    except Exception as e:
        logger.error(f"Error processing latest airing anime: {e}")
        if ADMIN_CHAT_ID:
            await safe_send_message(
                ADMIN_CHAT_ID,
                f"<b>Error processing latest airing anime:</b> <i>{str(e)}</i>",
                parse_mode='html'
            )
        return False
    finally:
        currently_processing = False

async def download_anime_by_index(event, index: int, force_redownload: bool = False):
    global currently_processing
    
    logger.info(f"Downloading anime at index {index} from latest airing list...")
    
    if currently_processing:
        await safe_respond(event, "⚠️ Already processing another anime. Please wait.")
        return False
    
    currently_processing = True
    try:
        progress = ProgressMessage(client, event.chat_id, f"Adding task to download anime at index {index}...")
        if not await progress.send():
            await safe_respond(event, "Failed to initialize progress tracking")
            return False
        
        await progress.update("Fetching latest anime list...")
        latest_data = get_latest_releases(page=1)
        if not latest_data or 'data' not in latest_data:
            logger.error("Failed to get latest releases")
            await progress.update("Failed to get latest releases")
            return False
        
        if index < 1 or index > len(latest_data['data']):
            logger.error(f"Invalid index: {index}. Must be between 1 and {len(latest_data['data'])}")
            await progress.update(f"Invalid index: {index}. Must be between 1 and {len(latest_data['data'])}")
            return False
        
        anime_data = latest_data['data'][index - 1]
        anime_title = anime_data.get('anime_title', 'Unknown Anime')
        episode_number = anime_data.get('episode', 0)
        
        logger.info(f"Selected anime: {anime_title} Episode {episode_number}")
        await progress.update(f"Selected: {anime_title} Episode {episode_number}\n\nFetching anime details...")
        
        await progress.update(f"Selected: {anime_title} Episode {episode_number}\n\nSearching anime...")
        search_results = await search_anime(anime_title)
        if not search_results:
            logger.error(f"Anime not found: {anime_title}")
            await progress.update(f"Anime not found: {anime_title}")
            return False
        
        anime_info = search_results[0]
        anime_session = anime_info['session']
        
        await progress.update(f"Selected: {anime_title} Episode {episode_number}\n\nFetching episode list...")
        episodes = await get_all_episodes(anime_session)
        if not episodes:
            logger.error(f"Failed to get episode list for {anime_title}")
            await progress.update(f"❌ Failed to get episode list for {anime_title}")
            return False
        
        target_episode = None
        for ep in episodes:
            try:
                if int(ep['episode']) == episode_number:
                    target_episode = ep
                    break
            except (ValueError, TypeError):
                continue
        
        if not target_episode:
            logger.warning(f"Episode {episode_number} not found for {anime_title}. Looking for closest available.")
            target_episode = find_closest_episode(episodes, episode_number)
            if target_episode:
                actual_episode = int(target_episode['episode'])
                logger.info(f"Found closest episode: {actual_episode}")
                episode_number = actual_episode
                await progress.update(f"Selected: {anime_title} Episode {episode_number}\n\nFound closest episode: {actual_episode}")
            else:
                logger.error(f"No episodes found for {anime_title}")
                await progress.update(f"No episodes found for {anime_title}")
                return False
        
        episode_session = target_episode['session']
        
        await progress.update(f"🔄 Selected: {anime_title} Episode {episode_number}\n\nFetching download links...")
        download_links = get_download_links(anime_session, episode_session)
        if not download_links:
            logger.error(f"No download links found for {anime_title} Episode {episode_number}")
            await progress.update(f"❌ No download links found for {anime_title} Episode {episode_number}")
            return False
        
        enabled_qualities = quality_settings.enabled_qualities

        is_dub = any('eng' in link['text'].lower() for link in download_links)
        audio_type = "Dub" if is_dub else "Sub"
        
        sorted_qualities = sorted(enabled_qualities, key=lambda x: int(x[:-1]))
        
        downloaded_qualities = []
        quality_files = {}
        
        for quality in sorted_qualities:
            try:
                logger.info(f"Downloading {anime_title} Episode {episode_number} {quality}")
                
                quality_link = None
                for link in download_links:
                    if quality in link['text']:
                        quality_link = link
                        break
                
                if not quality_link:
                    logger.error(f"Quality {quality} not found for {anime_title} Episode {episode_number}")
                    continue
                
                await progress.update(f"Selected: {anime_title} Episode {episode_number}\n\nDownloading {quality}...")
                
                base_name = format_filename(anime_title, episode_number, quality, "Sub" if not is_dub else "Dub")
                main_channel_username = CHANNEL_USERNAME if CHANNEL_USERNAME else BOT_USERNAME
                full_caption = f"{base_name} {main_channel_username}"
                filename = sanitize_filename(full_caption) + ".mkv"
                download_path = os.path.join(DOWNLOAD_DIR, filename)
                
                kwik_link = extract_kwik_link(quality_link['href'])
                if not kwik_link:
                    logger.error(f"Failed to extract kwik link for {quality}")
                    continue
                
                direct_link = get_dl_link(kwik_link)
                if not direct_link:
                    logger.error(f"Failed to get direct link for {quality}")
                    continue
                
                logger.info(f"Downloading {anime_title} Episode {episode_number} {quality}")

                last_update = time.time()
                download_start = time.time()
                
                def progress_hook(d):
                    nonlocal last_update
                    if d['status'] == 'downloading':
                        current_time = time.time()
                        if current_time - last_update >= 5:
                            downloaded_bytes = d.get('downloaded_bytes')
                            total_bytes = d.get('total_bytes')
                            speed = d.get('speed')
                            
                            downloaded = downloaded_bytes if downloaded_bytes is not None else 0
                            total = total_bytes if total_bytes is not None else 1
                            speed_val = speed if speed is not None else 0

                            try:
                                downloaded = int(downloaded)
                                total = int(total)
                                speed_val = float(speed_val)
                            except (ValueError, TypeError):
                                downloaded = 0
                                total = 1
                                speed_val = 0.0

                            if total > 0:
                                percent = min(100, (downloaded / total) * 100)
                            else:
                                percent = 0
                            
                            progress_text = (
                                f"Selected: {anime_title} Episode {episode_number}\n\n"
                                f"⬇Downloading {quality}: {percent:.1f}%\n"
                                f"{format_size(downloaded)}/{format_size(total)}\n"
                                f"{format_speed(speed_val)}"
                            )
                            
                            try:
                                asyncio.create_task(progress.update(progress_text))
                            except:
                                pass
                            
                            last_update = current_time
                
                ydl_opts = {
                    'outtmpl': download_path,
                    'quiet': True,
                    'no_warnings': True,
                    'http_headers': YTDLP_HEADERS,
                    'downloader_args': {'chunk_size': 10485760},
                    'progress_hooks': [progress_hook],
                    'nocheckcertificate': True,
                    'compat_opts': ['no-keep-video'],
                }
                
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([direct_link])
                
                if not os.path.exists(download_path) or os.path.getsize(download_path) < 1000:
                    logger.error(f"Downloaded file is too small or doesn't exist for {quality}")
                    await progress.update(f"❌ Downloaded file is too small or doesn't exist for {quality}")
                    continue
                
                download_time = time.time() - download_start
                file_size = os.path.getsize(download_path)
                avg_speed = file_size / download_time if download_time > 0 else 0
                
                await progress.update(
                    f"Selected: {anime_title} Episode {episode_number}\n\n"
                    f"Download completed for {quality}!\n"
                    f"Time: {download_time:.1f}s\n"
                    f"Size: {format_size(file_size)}\n"
                    f"Avg Sᴘᴇᴇᴅ: {format_speed(avg_speed)}\n\n"
                    f"Preparing upload..."
                )
                
                if FFMPEG_AVAILABLE:
                    final_path = os.path.join(DOWNLOAD_DIR, f"EP{episode_number:02d} - {anime_title} [{quality}].mkv")
                    if await rename_video_with_ffmpeg(download_path, final_path):
                        os.remove(download_path)
                        download_path = final_path
                
                await progress.update(f"Selected: {anime_title} Episode {episode_number}\n\nUploading {quality} to Telegram...")
                
                caption = full_caption
                
                try:
                    thumb = await get_fixed_thumbnail()
                    
                    upload_progress = UploadProgressBar(client, event.chat_id, full_caption)
                    
                    dump_msg_id = None
                    if DUMP_CHANNEL_ID:
                        try:
                            msg = await client.send_file(
                                DUMP_CHANNEL_ID,
                                download_path,
                                caption=caption,
                                thumb=thumb,
                                force_document=True,
                                attributes=None,
                                supports_streaming=False,
                                progress_callback=upload_progress.update,
                                part_size_kb=8192
                            )
                            
                            await upload_progress.finish()
                            
                            dump_msg_id = msg.id
                            logger.info(f"Sent to dump channel {DUMP_CHANNEL_ID}")
                        except Exception as e:
                            logger.error(f"Failed to send to dump channel: {e}")
                    elif DUMP_CHANNEL_USERNAME:
                        try:
                            msg = await client.send_file(
                                DUMP_CHANNEL_USERNAME,
                                download_path,
                                caption=caption,
                                thumb=thumb,
                                force_document=True,
                                attributes=None,
                                supports_streaming=False,
                                progress_callback=upload_progress.update,
                                part_size_kb=8192
                            )
                            
                            await upload_progress.finish()
                            
                            dump_msg_id = msg.id
                            logger.info(f"Sent to dump channel {DUMP_CHANNEL_USERNAME}")
                        except Exception as e:
                            logger.error(f"Failed to send to dump channel: {e}")
                    else:
                        logger.warning("No dump channel configured. File not uploaded.")
                    
                    if dump_msg_id:
                        if quality not in quality_files:
                            quality_files[quality] = []
                        quality_files[quality].append(dump_msg_id)

                    update_processed_qualities(anime_title, episode_number, quality)
                    downloaded_qualities.append(quality)
                    logger.info(f"Successfully uploaded {quality} version")
                    
                except FloodWaitError as e:
                    logger.error(f"Flood wait error: {e.seconds} seconds")
                    await progress.update(f"Flood wait: {e.seconds} seconds. Waiting...")
                    await asyncio.sleep(e.seconds + 5)
                    
                    try:
                        if DUMP_CHANNEL_ID:
                            msg = await client.send_file(
                                DUMP_CHANNEL_ID,
                                download_path,
                                caption=caption,
                                thumb=thumb,
                                force_document=True,
                                attributes=None,
                                supports_streaming=False,
                                part_size_kb=8192
                            )
                            
                            dump_msg_id = msg.id
                            logger.info(f"Sent to dump channel {DUMP_CHANNEL_ID}")
                        elif DUMP_CHANNEL_USERNAME:
                            msg = await client.send_file(
                                DUMP_CHANNEL_USERNAME,
                                download_path,
                                caption=caption,
                                thumb=thumb,
                                force_document=True,
                                attributes=None,
                                supports_streaming=False,
                                part_size_kb=8192
                            )
                            
                            dump_msg_id = msg.id
                            logger.info(f"Sent to dump channel {DUMP_CHANNEL_USERNAME}")
                    except Exception as e:
                        logger.error(f"Error sending video after flood wait: {e}")
                except Exception as e:
                    logger.error(f"Eʀʀᴏʀ sᴇɴᴅɪɴɢ ᴠɪᴅᴇᴏ: {e}")
                
                try:
                    os.remove(download_path)
                except:
                    pass
                
            except Exception as e:
                logger.error(f"Error processing {quality}: {e}")
        
        if quality_files:
            anime_info = await get_anime_info(anime_title)
            if anime_info:
                await post_anime_with_buttons(client, anime_title, anime_info, episode_number, audio_type, quality_files)
        
        if is_episode_processed(anime_title, episode_number):
            logger.info(f"All qualities processed for {anime_title} Episode {episode_number}")
            await progress.update(f"Successfully processed {anime_title} Episode {episode_number} ({', '.join(downloaded_qualities)})")
            return True
        else:
            logger.error(f"Not all qualities downloaded for {anime_title} Episode {episode_number}")
            await progress.update(f"Partially processed {anime_title} Episode {episode_number}\nDᴏᴡɴʟᴏᴀᴅᴇᴅ: {', '.join(downloaded_qualities)}\nMissing: {', '.join(set(enabled_qualities) - set(downloaded_qualities))}")
            return False
    
    except Exception as e:
        logger.error(f"Error in download_anime_by_index: {e}")
        await safe_respond(event, f"Error: {str(e)}")
        return False
    finally:
        currently_processing = False

async def check_and_process_next_episode(progress=None):
    try:
        logger.info("Checking for other new episodes to process...")
        
        latest_data = get_latest_releases(page=1)
        if not latest_data or 'data' not in latest_data:
            return False
        
        for idx, anime_data in enumerate(latest_data['data']):
            if idx >= 5:
                break
                
            anime_title = anime_data.get('anime_title', 'Unknown Anime')
            episode_number = anime_data.get('episode', 0)
            
            if anime_queue.is_processed(anime_title, episode_number):
                continue
            
            episode_id = f"{anime_title}_{episode_number}"
            if episode_id in [item['id'] for item in anime_queue.pending_queue]:
                continue
            
            logger.info(f"Found unprocessed episode: {anime_title} Episode {episode_number}")
            
            if progress:
                await progress.update(
                    f"<b>Checking next episode...</b>\n"
                    f"<i>{anime_title} Episode {episode_number}</i>"
                )
            
            success = await process_single_episode(anime_title, episode_number, progress)
            if success:
                return True
        
        return await process_pending_queue(progress)
        
    except Exception as e:
        logger.error(f"Error checking next episode: {e}")
        return False

async def process_pending_queue(progress=None):
    try:
        pending_item = anime_queue.get_next_pending()
        if not pending_item:
            logger.info("No items in pending queue")
            return False
        
        logger.info(f"Processing from queue: {pending_item['id']}")
        
        if progress:
            await progress.update(
                f"<b>Processing from queue...</b>\n"
                f"<i>{pending_item['title']} Episode {pending_item['episode']}</i>\n"
                f"<b>Queue size:</b> <i>{len(anime_queue.pending_queue)} episodes</i>"
            )
        
        success = await process_single_episode(
            pending_item['title'],
            pending_item['episode'],
            progress,
            from_queue=True
        )
        
        if success:
            anime_queue.remove_from_pending(pending_item['id'])
            logger.info(f"Successfully processed from queue: {pending_item['id']}")
        else:
            pending_item['last_checked'] = datetime.now().isoformat()
            anime_queue.save_queue()
        
        return success
        
    except Exception as e:
        logger.error(f"Error processing pending queue: {e}")
        return False

async def process_single_episode(anime_title, episode_number, progress=None, from_queue=False):
    global currently_processing
    
    try:
        if currently_processing:
            logger.info("Already processing an episode. Adding to queue.")
            return False
        
        currently_processing = True

        search_results = await search_anime(anime_title)
        if not search_results:
            logger.error(f"Anime not found: {anime_title}")
            return False
        
        anime_info = search_results[0]
        anime_session = anime_info['session']
        
        episodes = await get_all_episodes(anime_session)
        if not episodes:
            logger.error(f"Failed to get episode list for {anime_title}")
            return False
        
        target_episode = None
        for ep in episodes:
            try:
                if int(ep['episode']) == episode_number:
                    target_episode = ep
                    break
            except (ValueError, TypeError):
                continue
        
        if not target_episode:
            logger.error(f"Episode {episode_number} not found for {anime_title}")
            return False
        
        episode_session = target_episode['session']

        download_links = get_download_links(anime_session, episode_session)
        if not download_links:
            logger.error(f"No download links found for {anime_title} Episode {episode_number}")
            return False
        
        enabled_qualities = quality_settings.enabled_qualities
        available_qualities = []
        missing_qualities = []
        
        for quality in enabled_qualities:
            quality_available = False
            for link in download_links:
                if quality in link['text']:
                    available_qualities.append(quality)
                    quality_available = True
                    break
            if not quality_available:
                missing_qualities.append(quality)
        
        if missing_qualities and not from_queue:
            queue_info = {
                'title': anime_title,
                'episode': episode_number,
                'session': anime_session,
                'episode_session': episode_session,
                'available_qualities': available_qualities,
                'missing_qualities': missing_qualities,
                'audio_type': "Dub" if any('eng' in link['text'].lower() for link in download_links) else "Sub"
            }
            
            anime_queue.add_to_pending(queue_info)
            
            if progress:
                await progress.update(
                    f"<b>Added to queue:</b> <i>{anime_title} Episode {episode_number}</i>\n"
                    f"<b>Missing:</b> <i>{', '.join(missing_qualities)}</i>"
                )
            
            return False

        if missing_qualities and from_queue:
            logger.info(f"Still missing qualities for {anime_title} Episode {episode_number}")
            return False
        
        anime_queue.mark_as_processed(anime_title, episode_number)
        
        return True
        
    except Exception as e:
        logger.error(f"Error processing episode: {e}")
        return False
    finally:
        currently_processing = False

async def check_for_new_episodes(client):
    if not auto_download_state.enabled:
        return
    
    if currently_processing:
        logger.info("Already processing an episode. Skipping auto check.")
        return
    
    logger.info("Checking for new episodes and pending queue...")
    
    if anime_queue.pending_queue:
        logger.info(f"Processing {len(anime_queue.pending_queue)} pending episodes first...")
        await process_pending_queue()
    
    try:
        if auto_download_state.last_checked:
            last_check = datetime.fromisoformat(auto_download_state.last_checked)
            time_since_last_check = (datetime.now() - last_check).total_seconds()
            
            cooldown_period = auto_download_state.interval / 2
            if time_since_last_check < cooldown_period:
                logger.info(f"Skipping auto check, last check was {time_since_last_check:.1f} seconds ago")
                return
        
        latest_data = get_latest_releases(page=1)
        if not latest_data or 'data' not in latest_data:
            logger.error("Failed to get latest releases")
            return
        
        latest_anime = latest_data['data'][0]
        anime_title = latest_anime.get('anime_title', 'Unknown Anime')
        episode_number = latest_anime.get('episode', 0)
        
        if is_episode_processed(anime_title, episode_number):
            logger.info(f"Episode {episode_number} of {anime_title} already processed. Skipping.")
            return

        if ADMIN_CHAT_ID:
            progress = ProgressMessage(client, ADMIN_CHAT_ID, f"Checking for new episodes...")
            if not await progress.send():
                logger.error("Failed to send progress message")
                return
            
            await progress.update(f"Checking for new episodes...\n\nProcessing: {anime_title} Episode {episode_number}")
        
        success = await auto_download_latest_episode()
        auto_download_state.last_checked = datetime.now().isoformat()
        
        if success:
            logger.info("Successfully processed latest episode")
            if ADMIN_CHAT_ID:
                await safe_send_message(
                    ADMIN_CHAT_ID,
                    f"<b>Successfully processed:</b> <i>{anime_title}</i> <b>Episode</b> <i>{episode_number}</i>",
                    parse_mode='html'
                )
        else:
            logger.error("Failed to process latest episode")
            if ADMIN_CHAT_ID:
                await safe_send_message(
                    ADMIN_CHAT_ID,
                    f"<b>Failed to process:</b> <i>{anime_title}</i> <b>Episode</b> <i>{episode_number}</i>",
                    parse_mode='html'
                )
    except Exception as e:
        logger.error(f"Error checking for new episodes: {str(e)}")
        if ADMIN_CHAT_ID:
            await safe_send_message(
                ADMIN_CHAT_ID,
                f"<b>Error checking for new episodes:</b> <i>{str(e)}</i>",
                parse_mode='html'
            )

async def process_all_qualities(client):
    if currently_processing:
        logger.info("Already processing an episode. Skipping auto check.")
        return
    
    logger.info("Processing latest airing anime with all qualities...")
    
    try:
        latest_data = get_latest_releases(page=1)
        if not latest_data or 'data' not in latest_data:
            logger.error("Failed to get latest releases")
            return

        latest_anime = latest_data['data'][0]
        anime_title = latest_anime.get('anime_title', 'Unknown Anime')
        episode_number = latest_anime.get('episode', 0)

        if is_episode_processed(anime_title, episode_number):
            logger.info(f"Episode {episode_number} of {anime_title} already processed. Skipping.")
            return
        
        if ADMIN_CHAT_ID:
            progress = ProgressMessage(client, ADMIN_CHAT_ID, f"Processing latest airing anime with all qualities...")
            if not await progress.send():
                logger.error("Failed to send progress message")
                return
            
            await progress.update(f"Processing latest airing anime with all qualities...\n\nProcessing: {anime_title} Episode {episode_number}")
        
        success = await process_latest_airing_anime()
        
        if success:
            logger.info("Successfully processed latest episode with all qualities")
            if ADMIN_CHAT_ID:
                await safe_send_message(
                    ADMIN_CHAT_ID,
                    f"<b>Successfully processed:</b> <i>{anime_title}</i> <b>Episode</b> <i>{episode_number}</i> <b>with all qualities</b>",
                    parse_mode='html'
                )
        else:
            logger.error("Failed to process latest episode with all qualities")
            if ADMIN_CHAT_ID:
                await safe_send_message(
                    ADMIN_CHAT_ID,
                    f"<b>Failed to process:</b> <i>{anime_title}</i> <b>Episode</b> <i>{episode_number}</i> <b>with all qualities</b>",
                    parse_mode='html'
                )
    except Exception as e:
        logger.error(f"Error processing latest airing anime: {str(e)}")
        if ADMIN_CHAT_ID:
            await safe_send_message(
                ADMIN_CHAT_ID,
                f"<b>Error processing latest airing anime:</b> <i>{str(e)}</i>",
                parse_mode='html'
            )

def setup_scheduler(client):
    def schedule_check():
        asyncio.create_task(check_for_new_episodes(client))
    
    def schedule_queue_check():
        asyncio.create_task(process_pending_queue())
    
    def reschedule():
        schedule.clear()
        interval = auto_download_state.interval
        schedule.every(interval).seconds.do(schedule_check)
        logger.info(f"Auto download scheduler (re)started with {interval} second interval")
    
    reschedule()
    
    orig_setter = auto_download_state.__class__.interval.fset
    def interval_setter(self, seconds):
        orig_setter(self, seconds)
        reschedule()
    
    auto_download_state.__class__.interval = auto_download_state.__class__.interval.setter(interval_setter)
    
    async def scheduler_loop():
        while True:
            schedule.run_pending()
            await asyncio.sleep(1)
    
    asyncio.create_task(scheduler_loop())

@client.on(events.NewMessage(pattern=r'^/start(?:\s+(.*))?$'))
async def start_handler(event):
    user_id = event.sender_id
    is_sub = await is_subscribed(client, user_id)
    
    if not is_sub:
        await not_joined(client, event)
        return
    
    user = await event.get_sender()
    mention = f"<a href='tg://user?id={user.id}'>{user.first_name}</a>"
    
    param = event.pattern_match.group(1)

    if param:
        try:
            decoded_bytes = base64.b64decode(param)
            decoded = decoded_bytes.decode('ascii')

            if decoded.startswith('get-'):
                parts = decoded.split('-')
                if len(parts) == 3:
                    try:
                        first_id = int(parts[1])
                        last_id = int(parts[2])
                    except ValueError:
                        await event.respond("Invalid link format.")
                        return

                    dump_channel = (
                        bot_settings.get("dump_channel_id")
                        or bot_settings.get("dump_channel_username")
                    )
                    if not dump_channel:
                        await event.respond("Dump channel not configured.")
                        return

                    try:
                        processing_msg = await event.respond("Processing your download request... Please wait!")
                        
                        messages = await client.get_messages(
                            dump_channel, ids=list(range(first_id, last_id + 1))
                        )

                        delete_timer = bot_settings.get("file_delete_timer", 600)
                        minutes = delete_timer // 60

                        sent_messages = []
                        file_count = 0
                        
                        for msg in messages:
                            if msg and msg.media:
                                file_count += 1
                                sent_msg = await client.send_file(
                                    event.chat_id,
                                    file=msg.media,
                                    caption=msg.message,
                                    attributes=msg.document.attributes if msg.document else None
                                )
                                sent_messages.append(sent_msg)

                        try:
                            await processing_msg.delete()
                        except:
                            pass

                        if sent_messages:
                            final_msg = await client.send_message(
                                event.chat_id, 
                                f"<blockquote><b>Successfully sent {file_count} file(s)!</b></blockquote>\n"
                                f"<blockquote><b>Files will be deleted in {minutes} mins. Please save or forward them before they get deleted.</b></blockquote>",
                                parse_mode='html'
                            )
                            sent_messages.append(final_msg)

                            for sent_msg in sent_messages:
                                asyncio.create_task(delete_message_after(sent_msg, delete_timer))
                        else:
                            await event.respond("No files found for this batch.")
                            
                    except Exception as e:
                        logger.error(f"Error sending files: {e}")
                        try:
                            await processing_msg.delete()
                        except:
                            pass
                        await event.respond("An error occurred while sending the files. Please try again later.")

                else:
                    await event.respond("Invalid link format.")
            else:
                await event.respond("Invalid link format.")

        except Exception as e:
            logger.error(f"Error in start_with_param: {e}")
            await event.respond("An error occurred while processing your request.")

    else:
        try:
            start_pic_path = bot_settings.get("start_pic", None)
            use_fallback = False

            if start_pic_path and os.path.exists(start_pic_path):
                start_media = start_pic_path
            else:
                start_media = START_PIC_URL
                use_fallback = True

            caption_text=(
                f"<blockquote><b>🍁 Hᴇʏ, {mention}!</b></blockquote>\n"
                "<blockquote><b><i>I'ᴍ ᴀ ᴀᴜᴛᴏ ᴀɴɪᴍᴇ ʙᴏᴛ. ɪ ᴄᴀɴ ᴅᴏᴡɴʟᴏᴀᴅ ᴏɴɢᴏɪɴɢ ᴀɴᴅ ғɪɴɪsʜᴇᴅ ᴀɴɪᴍᴇ ғʀᴏᴍ ᴀɴɪᴍᴇᴘᴀʜᴇ.ʀᴜ ᴀɴᴅ ᴜᴘʟᴏᴀᴅ ᴛʜᴏsᴇ ғɪʟᴇs ᴏɴ ʏᴏᴜʀ ᴄʜᴀɴɴᴇʟ ᴅɪʀᴇᴄᴛʟʏ...</i></b>\n</blockquote>"
                "<blockquote><b>ᴘᴏᴡᴇʀᴇᴅ ʙʏ - <a href='https://t.me/AnimesOngoing'>𝗢𝗻𝗴𝗼𝗶𝗻𝗴 𝗔𝗻𝗶𝗺𝗲- 𝗔𝗿𝗰</a></b></blockquote>"
            )
            
            if is_admin(event.chat_id):
                    buttons = [
                        [Button.inline("sᴇᴀʀᴄʜ ᴀɴɪᴍᴇ", b"search_anime"), Button.inline("ʜᴇʟᴘ", b"show_help")],
                        [Button.inline("ᴀᴜᴛᴏ ᴅᴏᴡɴʟᴏᴀᴅ sᴇᴛᴛɪɴɢs", b"auto_settings")]
                    ]
            else:
                buttons = [
                    [Button.url("Dᴇᴠᴇʟᴏᴘᴇʀ", "https://t.me/DARKXSIDE78"),
                     Button.url("Mᴀɪɴ Cʜᴀɴɴᴇʟ", "https://t.me/ChibiHub")],
                    [Button.url("Bᴀᴄᴋᴜᴘ Cʜᴀɴɴᴇʟ", "https://t.me/AnimesOngoing")]
                ]

            await client.send_file(
                event.chat_id,
                start_media,
                caption=caption_text,
                parse_mode='HTML',
                force_document=False,
                buttons=buttons
            )

        
        except Exception as e:
            logger.error(f"Error sending start message: {e}")
            if is_admin(event.chat_id):
                await safe_respond(event, f"<b>🍁 Hᴇʏ, {mention}!\nI'ᴍ ᴀ ᴀᴜᴛᴏ ᴀɴɪᴍᴇ ʙᴏᴛ. ɪ ᴄᴀɴ ᴅᴏᴡɴʟᴏᴀᴅ ᴏɴɢᴏɪɴɢ ᴀɴᴅ ғɪɴɪsʜᴇᴅ ᴀɴɪᴍᴇ ғʀᴏᴍ ᴀɴɪᴍᴇᴘᴀʜᴇ.ʀᴜ ᴀɴᴅ ᴜᴘʟᴏᴀᴅ ᴛʜᴏsᴇ ғɪʟᴇs ᴏɴ ʏᴏᴜʀ ᴄʜᴀɴɴᴇʟ ᴅɪʀᴇᴄᴛʟʏ...\n<blockquote>ᴘᴏᴡᴇʀᴇᴅ ʙʏ - <a href='https://t.me/AnimesOngoing'>𝗢𝗻𝗴𝗼𝗶𝗻𝗴 𝗔𝗻𝗶𝗺𝗲- 𝗔𝗿𝗰</a></blockquote></b>", parse_mode='html')
            else:
                await safe_respond(event, f"<b>🍁 Hᴇʏ, {mention}!\nI'ᴍ ᴀ ᴀᴜᴛᴏ ᴀɴɪᴍᴇ ʙᴏᴛ. Please contact the developer for support.\n<blockquote>ᴘᴏᴡᴇʀᴇᴅ ʙʏ - <a href='https://t.me/AnimesOngoing'>𝗢𝗻𝗴𝗼𝗶𝗻𝗴 𝗔𝗻𝗶𝗺𝗲- 𝗔𝗿𝗰</a></blockquote></b>", parse_mode='html')

async def delete_message_after(message, seconds):
    await asyncio.sleep(seconds)
    try:
        await client.delete_messages(message.chat_id, [message.id])
        logger.info(f"Deleted message {message.id} from chat {message.chat_id}")
    except Exception as e:
        logger.error(f"Failed to delete message: {e}")

@client.on(events.NewMessage(pattern='/help'))
async def help_command(event):
    if not is_admin(event.chat_id):
        return
    
    help_text = (
        "<b>🤖 Anime Downloader Bot Help</b>\n\n"
        "<b>🔍 How to use:</b>\n"
        "<i>1. Send me the name of an anime you want to download</i>\n"
        "<i>2. Select the anime from the search results</i>\n"
        "<i>3. Choose an episode to download</i>\n"
        "<i>4. Select the quality you prefer</i>\n"
        "<i>5. I'll download and send it to you!</i>\n\n"
        "<b>✅ Now supporting files up to 2GB!</b>\n\n"
        "<b>🔧 Commands:</b>\n"
        "<code>/start</code> - Start the bot\n"
        "<code>/help</code> - Show this help message\n"
        "<code>/cancel</code> - Cancel current operation\n"
        "<code>/ping</code> - Test if bot is working\n"
        "<code>/latest</code> - Get latest airing anime\n"
        "<code>/airing</code> - Get currently airing anime\n"
        "<code>/del_timer</code> - Set file delete timer\n\n"
        "<b>👤 Admin Commands:</b>\n"
        "<code>/auto_status</code> - Check auto download status\n"
        "<code>/auto_toggle</code> - Enable/disable auto downloads\n"
        "<code>/auto_check</code> - Manually check for new episodes (downloads highest quality)\n"
        "<code>/auto_interval [seconds]</code> - Set check interval\n"
        "<code>/process_latest</code> - Process the latest airing anime (downloads all qualities)\n"
        "<code>/addtask [number]</code> - Download specific anime from latest airing list\n"
        "<code>/redownload [number]</code> - Force redownload a specific anime\n"
        "<code>/set_qual</code> - Set quality preferences\n"
        "<code>/add_admin [user_id]</code> - Add a new admin\n"
        "<code>/remove_admin [user_id]</code> - Remove an admin\n"
        "<code>/start_pic</code> - Change start picture\n"
        "<code>/set_dump</code> - Change dump channel"
    )
    
    await safe_respond(event, help_text, parse_mode='html')

@client.on(events.NewMessage(pattern='/ping'))
async def ping(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        return
    
    await safe_respond(event, "🏓 <b>Pong!</b> <i>Bot is working fine.</i>", parse_mode='html')

@client.on(events.NewMessage(pattern='/cancel'))
async def cancel(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        return
    
    await safe_respond(event, "❌ <b>Operation cancelled.</b> <i>Send /start to begin again.</i>", parse_mode='html')

@client.on(events.NewMessage(pattern='/add_admin'))
async def add_admin_command(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        return
    
    # Check if user is the owner
    if event.chat_id != ADMIN_CHAT_ID:
        await safe_respond(event, "❌ <b>Only the owner can add new admins.</b>", parse_mode='html')
        return
    
    # Get user ID from command
    parts = event.text.split()
    if len(parts) < 2:
        await safe_respond(event, "❌ <b>Please provide a user ID to add as admin.</b>\n\n<b>Usage:</b> <code>/add_admin [user_id]</code>", parse_mode='html')
        return
    
    try:
        user_id = int(parts[1])
        
        # Try to get user info
        try:
            user = await client.get_entity(user_id)
            username = user.username if hasattr(user, 'username') else None
            name = f"{user.first_name} {user.last_name}" if user.last_name else user.first_name
        except:
            username = None
            name = f"User {user_id}"
        
        # Add admin
        if add_admin(user_id, username):
            await safe_respond(event, f"✅ <b>Successfully added</b> <i>{name}</i> <b>as admin.</b>", parse_mode='html')
        else:
            await safe_respond(event, f"❌ <b>Failed to add</b> <i>{name}</i> <b>as admin.</b>", parse_mode='html')
    except ValueError:
        await safe_respond(event, "❌ <b>Please provide a valid user ID.</b>\n\n<b>Usage:</b> <code>/add_admin [user_id]</code>", parse_mode='html')
    except Exception as e:
        logger.error(f"Error adding admin: {e}")
        await safe_respond(event, f"❌ <b>Error adding admin:</b> <i>{str(e)}</i>", parse_mode='html')

@client.on(events.NewMessage(pattern='/remove_admin'))
async def remove_admin_command(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        return
    
    # Check if user is the owner
    if event.chat_id != ADMIN_CHAT_ID:
        await safe_respond(event, "❌ <b>Only the owner can remove admins.</b>", parse_mode='html')
        return
    
    # Get user ID from command
    parts = event.text.split()
    if len(parts) < 2:
        await safe_respond(event, "❌ <b>Please provide a user ID to remove from admin.</b>\n\n<b>Usage:</b> <code>/remove_admin [user_id]</code>", parse_mode='html')
        return
    
    try:
        user_id = int(parts[1])
        
        # Try to get user info
        try:
            user = await client.get_entity(user_id)
            username = user.username if hasattr(user, 'username') else None
            name = f"{user.first_name} {user.last_name}" if user.last_name else user.first_name
        except:
            username = None
            name = f"User {user_id}"
        
        # Remove admin
        if remove_admin(user_id):
            await safe_respond(event, f"✅ <b>Successfully removed</b> <i>{name}</i> <b>from admin.</b>", parse_mode='html')
        else:
            await safe_respond(event, f"❌ <b>Failed to remove</b> <i>{name}</i> <b>from admin.</b>", parse_mode='html')
    except ValueError:
        await safe_respond(event, "❌ <b>Please provide a valid user ID.</b>\n\n<b>Usage:</b> <code>/remove_admin [user_id]</code>", parse_mode='html')
    except Exception as e:
        logger.error(f"Error removing admin: {e}")
        await safe_respond(event, f"❌ <b>Error removing admin:</b> <i>{str(e)}</i>", parse_mode='html')

@client.on(events.NewMessage(pattern='/addfsub'))
async def add_force_sub(event):
    if not is_admin(event.chat_id):
        return
        
    temp = await safe_respond(event, "⏳ Adding force sub channel...")
    args = event.text.split(maxsplit=1)
    
    if len(args) != 2:
        return await safe_edit(temp, "Usage: /addfsub <channel_id>")
        
    try:
        chat_id = int(args[1])
    except ValueError:
        return await safe_edit(temp, "Invalid chat ID!")
        
    all_chats = await codeflixbots.show_channels()
    if chat_id in all_chats:
        return await safe_edit(temp, f"Channel already added: {chat_id}")
        
    try:
        chat = await client.get_entity(chat_id)
        if not hasattr(chat, 'title'):
            return await safe_edit(temp, "Invalid channel/supergroup!")
            
        # Check if bot is admin
        try:
            bot_permissions = await client.get_permissions(chat_id, 'me')
            if not bot_permissions.is_admin:
                return await safe_edit(temp, "Bot must be admin in the channel!")
        except:
            return await safe_edit(temp, "Bot must be admin in the channel!")
            
        # Get invite link
        try:
            invite = await client(functions.messages.ExportChatInviteRequest(peer=chat_id))
            link = invite.link
        except:
            link = f"https://t.me/{chat.username}" if hasattr(chat, 'username') and chat.username else "Private Channel"
            
        await codeflixbots.add_channel(chat_id)
        await safe_edit(temp, f"✅ Added successfully!\n\nChannel: [{chat.title}]({link})\nID: `{chat_id}`")
        
    except Exception as e:
        await safe_edit(temp, f"❌ Failed to add channel:\n`{chat_id}`\n\nError: {str(e)}")

@client.on(events.NewMessage(pattern='/delfsub'))
async def del_force_sub(event):
    if not is_admin(event.chat_id):
        return
        
    temp = await safe_respond(event, "⏳ Removing force sub channel...")
    args = event.text.split(maxsplit=1)
    all_channels = await codeflixbots.show_channels()
    
    if len(args) != 2:
        return await safe_edit(temp, "Usage: /delfsub <channel_id | all>")
        
    if args[1].lower() == "all":
        if not all_channels:
            return await safe_edit(temp, "No force sub channels found.")
        for ch_id in all_channels:
            await codeflixbots.rem_channel(ch_id)
        return await safe_edit(temp, "✅ All force sub channels removed.")
        
    try:
        ch_id = int(args[1])
    except ValueError:
        return await safe_edit(temp, "Invalid channel ID")
        
    if ch_id in all_channels:
        await codeflixbots.rem_channel(ch_id)
        await safe_edit(temp, f"✅ Channel removed: `{ch_id}`")
    else:
        await safe_edit(temp, f"Channel not found in force sub list: `{ch_id}`")

@client.on(events.NewMessage(pattern='/channels'))
async def list_channels(event):
    if not is_admin(event.chat_id):
        return
        
    temp = await safe_respond(event, "⏳ Fetching channels...")
    channels = await codeflixbots.show_channels()
    
    if not channels:
        return await safe_edit(temp, "No force sub channels configured.")
        
    result = "📋 **Force Sub Channels:**\n\n"
    for ch_id in channels:
        try:
            chat = await client.get_entity(ch_id)
            mode = await codeflixbots.get_channel_mode(ch_id)
            status = "🟢 ON" if mode == "on" else "🔴 OFF"
            link = f"https://t.me/{chat.username}" if hasattr(chat, 'username') and chat.username else f"Private Channel"
            result += f"• [{chat.title}]({link}) [`{ch_id}`] - {status}\n"
        except:
            result += f"• `{ch_id}` - Unavailable\n"
            
    await safe_edit(temp, result)

@client.on(events.NewMessage(pattern='/fsub_mode'))
async def toggle_fsub_mode(event):
    if not is_admin(event.chat_id):
        return
        
    temp = await safe_respond(event, "⏳ Loading channels...")
    channels = await codeflixbots.show_channels()
    
    if not channels:
        return await safe_edit(temp, "No force sub channels found.")
        
    buttons = []
    for ch_id in channels:
        try:
            chat = await client.get_entity(ch_id)
            mode = await codeflixbots.get_channel_mode(ch_id)
            status = "🟢" if mode == "on" else "🔴"
            buttons.append([Button.inline(f"{status} {chat.title}", f"toggle_fsub_{ch_id}")])
        except:
            buttons.append([Button.inline(f"⚠️ {ch_id} (Unavailable)", f"toggle_fsub_{ch_id}")])
            
    buttons.append([Button.inline("❌ Close", b"close_menu")])
    await safe_edit(temp, "⚡ Select a channel to toggle force sub mode:", buttons=buttons)

@client.on(events.CallbackQuery(data=b"check_joined"))
async def check_joined_callback(event):
    logger.info(f"User {event.sender_id} clicked the 'check_joined' button.")

    try:
        user_id = event.sender_id
        subscribed_all = await is_subscribed(client, user_id)
        logger.debug(f"Subscription check result for user {user_id}: {subscribed_all}")

        if subscribed_all:
            logger.info(f"User {user_id} has successfully joined the required channels.")
            try:
                await safe_edit(
                    event,
                    "<b>Wᴇʟʟ ᴅᴏɴᴇ — You have joined the required channel(s)!\n"
                    "You can now use the bot. Please send /start ᴛᴏ ʙᴇɢɪɴ.</b>",
                    parse_mode='html'
                )
                logger.debug("Successfully updated the FSUB message to 'joined' status.")
            except Exception as edit_error:
                logger.warning(f"Failed to edit FSUB message for user {user_id}: {edit_error}")
                try:
                    await event.answer("Wᴇʟʟ ᴅᴏɴᴇ — You have joined the required channel(s)! Please send /start.", alert=True)
                except Exception as answer_error:
                    logger.error(f"Failed to send alert for user {user_id}: {answer_error}")

        else:
            logger.info(f"User {user_id} has NOT joined all required channels yet.")
            try:
                channel_ids = await codeflixbots.show_channels()
                missing = []
                buttons = []
                for ch in channel_ids:
                    try:
                        member = await is_sub(client, user_id, ch)
                        if not member:
                            try:
                                chat = await client.get_entity(ch)
                                mode = await codeflixbots.get_channel_mode(ch)
                                if hasattr(chat, 'username') and chat.username:
                                    link = f"https://t.me/{chat.username}"
                                else:
                                    try:
                                        invite = await client(functions.messages.ExportChatInviteRequest(peer=ch))
                                        link = invite.link
                                    except Exception:
                                        link = None

                                missing.append((getattr(chat, 'title', str(ch)), link, ch))
                                if link:
                                    buttons.append([Button.url("Jᴏɪɴ Cʜᴀɴɴᴇʟ", link)])
                                else:
                                    buttons.append([Button.inline(f"{getattr(chat, 'title', str(ch))}", b"no_link")])
                            except Exception as e:
                                logger.warning(f"Could not prepare invite for channel {ch}: {e}")
                                missing.append((str(ch), None, ch))
                    except Exception as e:
                        logger.error(f"Error checking membership for channel {ch}: {e}")

                if missing:
                    text = "❌ You haven't joined the following required channel(s):\n\n"
                    for title, link, ch in missing:
                        if link:
                            text += f"• {title} — Link provided\n"
                        else:
                            text += f"• {title} — (Private / no link available)\n"

                    buttons.append([Button.inline("Rᴇᴛʀʏ", b"check_joined")])

                    try:
                        await safe_edit(event, text, parse_mode='html', buttons=buttons)
                    except Exception:
                        try:
                            await event.answer("You haven't joined all required channels yet.", alert=True)
                        except:
                            pass
                        await safe_respond(event, text, parse_mode='html', buttons=buttons)
                else:
                    try:
                        await event.answer("You haven't joined the required channels yet. Please join them and click Retry.", alert=True)
                    except:
                        pass

            except Exception as answer_error:
                logger.error(f"Failed to prepare missing channels info for user {user_id}: {answer_error}")
                try:
                    await event.answer("❌ You haven't joined all required channels yet! Please join them and try clicking this button again.", alert=True)
                except Exception as answer_error2:
                    logger.error(f"Failed to send 'not joined' alert to user {user_id}: {answer_error2}")

    except Exception as e:
        logger.error(f"Unexpected error in check_joined_callback for user {event.sender_id}: {e}", exc_info=True)
        try:
            await event.answer("⚠️ An error occurred while checking. Please try sending /start again.", alert=True)
        except:
            pass

@client.on(events.CallbackQuery(data=b"close_menu"))
async def close_menu_callback(event):
    await event.delete()

@client.on(events.CallbackQuery(data=lambda data: data.startswith(b"toggle_fsub_")))
async def toggle_fsub_callback(event):
    if not is_admin(event.sender_id):
        await event.answer("Only admins can do this!", alert=True)
        return
        
    channel_id = int(event.data.decode().split('_')[-1])
    current_mode = await codeflixbots.get_channel_mode(channel_id)
    new_mode = "off" if current_mode == "on" else "on"
    
    await codeflixbots.set_channel_mode(channel_id, new_mode)
    await event.answer(f"Force sub mode set to {new_mode.upper()} for channel {channel_id}")
    
    channels = await codeflixbots.show_channels()
    buttons = []
    for ch_id in channels:
        try:
            chat = await client.get_entity(ch_id)
            mode = await codeflixbots.get_channel_mode(ch_id)
            status = "🟢" if mode == "on" else "🔴"
            buttons.append([Button.inline(f"{status} {chat.title}", f"toggle_fsub_{ch_id}")])
        except:
            buttons.append([Button.inline(f"⚠️ {ch_id} (Unavailable)", f"toggle_fsub_{ch_id}")])
            
    buttons.append([Button.inline("❌ Close", b"close_menu")])
    await safe_edit(event, "⚡ Select a channel to toggle force sub mode:", buttons=buttons)

@client.on(events.NewMessage(pattern='/start_pic'))
async def set_start_pic(event):
    if not is_admin(event.chat_id):
        return
    
    if not event.photo and not (event.document and event.document.mime_type.startswith('image/')):
        await safe_respond(event, "❌ <b>Please reply to an image or send an image with this command to set as start picture.</b>", parse_mode='html')
        return
        
    try:
        # Save permanently
        save_path = THUMBNAIL_DIR / "start_pic.jpg"
        pic_path = await event.download_media(file=str(save_path))
        
        if os.path.exists(pic_path):
            bot_settings.set("start_pic", str(save_path))  # Save permanent path
            await safe_respond(event, "✅ <b>Start picture has been set successfully!</b>", parse_mode='html')
        else:
            await safe_respond(event, "❌ <b>Failed to set start picture.</b> <i>Please try again.</i>", parse_mode='html')
            
    except Exception as e:
        await safe_respond(event, f"❌ <b>Error setting start picture:</b> <i>{str(e)}</i>", parse_mode='html')

@client.on(events.NewMessage(pattern='/set_dump'))
async def set_dump_channel(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        return
    
    # Check if user is the owner
    if event.chat_id != ADMIN_CHAT_ID:
        await safe_respond(event, "❌ <b>Only the owner can change dump channel.</b>", parse_mode='html')
        return
    
    # Get channel from command
    parts = event.text.split(maxsplit=1)
    if len(parts) < 2:
        await safe_respond(event, "❌ <b>Please provide a channel ID or username.</b>\n\n<b>Usage:</b> <code>/set_dump [channel_id_or_username]</code>", parse_mode='html')
        return
    
    channel_input = parts[1].strip()
    
    try:
        # Try to get channel entity
        channel = await client.get_entity(channel_input)
        
        # Get channel ID and username
        if hasattr(channel, 'id'):
            channel_id = channel.id
            bot_settings.set("dump_channel_id", channel_id)
            
        if hasattr(channel, 'username') and channel.username:
            channel_username = f"@{channel.username}"
            bot_settings.set("dump_channel_username", channel_username)
        
        await safe_respond(event, f"✅ <b>Successfully set dump channel to:</b> <i>{channel_input}</i>", parse_mode='html')
    except Exception as e:
        logger.error(f"Error setting dump channel: {e}")
        await safe_respond(event, f"❌ <b>Error setting dump channel:</b> <i>{str(e)}</i>", parse_mode='html')

@client.on(events.NewMessage(pattern='/auto_status'))
async def auto_status(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        return
    
    enabled = auto_download_state.enabled
    interval = auto_download_state.get_interval()
    last_checked = auto_download_state.last_checked
    
    status_text = (
        "<b>🤖 Auto Download Status</b>\n\n"
        f"<b>📊 Status:</b> {'✅ <i>Enabled</i>' if enabled else '❌ <i>Disabled</i>'}\n"
        f"<b>⏱️ Check Interval:</b> <i>{interval} seconds</i>\n"
        f"<b>🕒 Last Checked:</b> <i>{last_checked or 'Never'}</i>"
    )
    
    # Create buttons in 2 rows of 2
    if enabled:
        btn1 = Button.inline("❌ Disable Auto Download", b"auto_disable")
    else:
        btn1 = Button.inline("✅ Enable Auto Download", b"auto_enable")
    
    buttons = [
        [btn1, Button.inline("🔄 Check Now", b"auto_check_now")],
        [Button.inline("⚙️ Change Interval", b"auto_interval"), Button.inline("🔙 Back", b"back_to_main")]
    ]
    
    await safe_respond(event, status_text, buttons=buttons, parse_mode='html')

@client.on(events.NewMessage(pattern='/auto_toggle'))
async def auto_toggle(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        return
    
    current_status = auto_download_state.enabled
    auto_download_state.enabled = not current_status
    
    status_text = (
        f"✅ <b>Auto download has been {'enabled' if not current_status else 'disabled'}.</b>\n\n"
        "<i>Use /auto_status to check current settings.</i>"
    )
    
    await safe_respond(event, status_text, parse_mode='html')

@client.on(events.NewMessage(pattern='/auto_check'))
async def auto_check(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        return
    
    await safe_respond(event, "🔄 <b>Checking for new episodes...</b>", parse_mode='html')
    await check_for_new_episodes(client)

@client.on(events.NewMessage(pattern='/process_latest'))
async def process_latest(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        return
    
    await safe_respond(event, "🔄 <b>Processing latest airing anime with all qualities...</b>", parse_mode='html')
    await process_all_qualities(client)

@client.on(events.NewMessage(pattern='/addtask'))
async def add_task(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        return
    
    parts = event.text.split()
    if len(parts) < 2:
        # Show the latest airing list
        try:
            await safe_respond(event, "🔍 <b>Fetching latest anime list...</b>", parse_mode='html')
            
            API_URL = "https://animepahe.ru/api?m=airing&page=1"
            async with aiohttp.ClientSession() as session:
                async with session.get(API_URL, headers=HEADERS) as response:
                    if response.status == 200:
                        data = await response.json()
                        anime_list = data.get('data', [])
                        
                        if not anime_list:
                            await safe_respond(event, "<b>No latest anime available at the moment.</b>", parse_mode='html')
                            return
                        
                        latest_anime_text = "<b>📺 Latest Airing Anime:</b>\n\n"
                        for idx, anime in enumerate(anime_list[:10], start=1):  # Limit to 10 results
                            title = anime.get('anime_title', 'Unknown Title')
                            episode = anime.get('episode', 'N/A')
                            latest_anime_text += f"<b>{idx}. {title} [E{episode}]</b>\n"
                        
                        latest_anime_text += "\n\n<b>Use /addtask [number] to download a specific anime.</b>"
                        await safe_respond(event, latest_anime_text, parse_mode='html', link_preview=False)
                    else:
                        await safe_respond(event, f"<b>Failed to fetch data.</b> <i>Status code: {response.status}</i>", parse_mode='html')
        except Exception as e:
            logger.error(f"Error in add_task: {e}")
            await safe_respond(event, "<b>Something went wrong.</b> <i>Please try again later.</i>", parse_mode='html')
        return
    
    try:
        index = int(parts[1])
        if index < 1:
            await safe_respond(event, "❌ <b>Index must be a positive number.</b>", parse_mode='html')
            return
        
        await safe_respond(event, f"🔄 <b>Adding task to download anime at index {index}...</b>", parse_mode='html')
        success = await download_anime_by_index(event, index)
        
        if success:
            await safe_respond(event, f"✅ <b>Successfully downloaded and uploaded anime at index {index}</b>", parse_mode='html')
        else:
            await safe_respond(event, f"❌ <b>Failed to download anime at index {index}</b>", parse_mode='html')
    except ValueError:
        await safe_respond(event, "❌ <b>Please provide a valid number.</b>", parse_mode='html')
    except Exception as e:
        logger.error(f"Error in add_task: {e}")
        await safe_respond(event, f"❌ <b>Error:</b> <i>{str(e)}</i>", parse_mode='html')

@client.on(events.NewMessage(pattern='/redownload'))
async def redownload(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        return
    
    parts = event.text.split()
    if len(parts) < 2:
        # Show the latest airing list
        try:
            await safe_respond(event, "🔍 <b>Fetching latest anime list...</b>", parse_mode='html')
            
            API_URL = "https://animepahe.ru/api?m=airing&page=1"
            async with aiohttp.ClientSession() as session:
                async with session.get(API_URL, headers=HEADERS) as response:
                    if response.status == 200:
                        data = await response.json()
                        anime_list = data.get('data', [])
                        
                        if not anime_list:
                            await safe_respond(event, "<b>No latest anime available at the moment.</b>", parse_mode='html')
                            return
                        
                        latest_anime_text = "<b>📺 Latest Airing Anime:</b>\n\n"
                        for idx, anime in enumerate(anime_list[:10], start=1):  # Limit to 10 results
                            title = anime.get('anime_title', 'Unknown Title')
                            episode = anime.get('episode', 'N/A')
                            latest_anime_text += f"<b>{idx}. {title} [E{episode}]</b>\n"
                        
                        latest_anime_text += "\n\n<b>Use /redownload [number] to force redownload a specific anime.</b>"
                        await safe_respond(event, latest_anime_text, parse_mode='html', link_preview=False)
                    else:
                        await safe_respond(event, f"<b>Failed to fetch data.</b> <i>Status code: {response.status}</i>", parse_mode='html')
        except Exception as e:
            logger.error(f"Error in redownload: {e}")
            await safe_respond(event, "<b>Something went wrong.</b> <i>Please try again later.</i>", parse_mode='html')
        return
    
    try:
        index = int(parts[1])
        if index < 1:
            await safe_respond(event, "❌ <b>Index must be a positive number.</b>", parse_mode='html')
            return
        
        await safe_respond(event, f"🔄 <b>Adding task to redownload anime at index {index}...</b>", parse_mode='html')
        success = await download_anime_by_index(event, index, force_redownload=True)
        
        if success:
            await safe_respond(event, f"✅ <b>Successfully downloaded and uploaded anime at index {index}</b>", parse_mode='html')
        else:
            await safe_respond(event, f"❌ <b>Failed to download anime at index {index}</b>", parse_mode='html')
    except ValueError:
        await safe_respond(event, "❌ <b>Please provide a valid number.</b>", parse_mode='html')
    except Exception as e:
        logger.error(f"Error in redownload: {e}")
        await safe_respond(event, f"❌ <b>Error:</b> <i>{str(e)}</i>", parse_mode='html')

@client.on(events.NewMessage(pattern='/set_qual'))
async def set_qual(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        return
    
    # Get current settings
    enabled_qualities = quality_settings.enabled_qualities
    download_all = quality_settings.download_all
    batch_mode = quality_settings.batch_mode
    
    # Create buttons for quality selection
    buttons = []
    
    # Create quality buttons with checkmarks
    quality_buttons = []
    for quality in ["360p", "720p", "1080p"]:
        checked = "✅" if quality in enabled_qualities else "❌"
        quality_buttons.append(Button.inline(f"{checked} {quality}", f"toggle_{quality}"))
    
    # Arrange buttons in the requested pattern
    if len(quality_buttons) == 3:
        buttons.append([quality_buttons[0], quality_buttons[1]])
        buttons.append([quality_buttons[2]])
    elif len(quality_buttons) == 2:
        buttons.append(quality_buttons)
    else:
        buttons.append([btn] for btn in quality_buttons)
    
    # Add download all toggle
    all_checked = "✅" if download_all else "❌"
    buttons.append([Button.inline(f"{all_checked} Download All Enabled Qualities", b"toggle_all")])
    
    # Add batch mode toggle
    batch_checked = "✅" if batch_mode else "❌"
    buttons.append([Button.inline(f"{batch_checked} Batch Download Mode", b"toggle_batch")])
    
    # Add back button
    buttons.append([Button.inline("🔙 Back", b"back_to_main")])
    
    status_text = (
        "<b>⚙️ Quality Settings</b>\n\n"
        f"<b>Current enabled qualities:</b> <i>{', '.join(enabled_qualities)}</i>\n"
        f"<b>Download all enabled:</b> <i>{'Yes' if download_all else 'No'}</i>\n"
        f"<b>Batch download mode:</b> <i>{'Enabled' if batch_mode else 'Disabled'}</i>\n\n"
        "<b>Select qualities to enable/disable:</b>"
    )
    
    await safe_respond(event, status_text, buttons=buttons, parse_mode='html')

@client.on(events.NewMessage(pattern='/del_timer'))
async def del_timer_command(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        return
    
    parts = event.text.split()
    if len(parts) == 1:
        # Show current timer setting
        current_timer = bot_settings.get("file_delete_timer", 600)
        await safe_respond(event, f"⏱️ <b>Current file delete timer:</b> <i>{current_timer} seconds ({current_timer/60:.1f} minutes)</i>", parse_mode='html')
    else:
        try:
            seconds = int(parts[1])
            if seconds < 60:
                await safe_respond(event, "❌ <b>Timer must be at least 60 seconds.</b>", parse_mode='html')
                return
            bot_settings.set("file_delete_timer", seconds)
            await safe_respond(event, f"✅ <b>File delete timer set to {seconds} seconds ({seconds/60:.1f} minutes).</b>", parse_mode='html')
        except ValueError:
            await safe_respond(event, "❌ <b>Please provide a valid number.</b>", parse_mode='html')

# New commands for latest and airing anime
@client.on(events.NewMessage(pattern='/latest'))
async def latest_command(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        return
    
    try:
        await safe_respond(event, "🔍 <b>Fetching latest anime...</b>", parse_mode='html')
        
        API_URL = "https://animepahe.ru/api?m=airing&page=1"
        async with aiohttp.ClientSession() as session:
            async with session.get(API_URL, headers=HEADERS) as response:
                if response.status == 200:
                    data = await response.json()
                    anime_list = data.get('data', [])
                    
                    if not anime_list:
                        await safe_respond(event, "<b>No latest anime available at the moment.</b>", parse_mode='html')
                        return
                    
                    latest_anime_text = "<b>📺 Latest Airing Anime:</b>\n\n"
                    for idx, anime in enumerate(anime_list[:10], start=1):  # Limit to 10 results
                        title = anime.get('anime_title', 'Unknown Title')
                        anime_session = anime.get('anime_session', '')
                        episode = anime.get('episode', 'N/A')
                        link = f"https://animepahe.ru/anime/{anime_session}" if anime_session else "#"
                        
                        latest_anime_text += f"<b>{idx}. <a href='{link}'>{title}</a> [E{episode}]</b>\n"
                    
                    await safe_respond(event, latest_anime_text, parse_mode='html', link_preview=False)
                else:
                    await safe_respond(event, f"<b>Failed to fetch data.</b> <i>Status code: {response.status}</i>", parse_mode='html')
    
    except Exception as e:
        logger.error(f"Error in latest_command: {e}")
        await safe_respond(event, "<b>Something went wrong.</b> <i>Please try again later.</i>", parse_mode='html')

@client.on(events.NewMessage(pattern='/airing'))
async def airing_command(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        return
    
    try:
        await safe_respond(event, "🔍 <b>Fetching airing anime...</b>", parse_mode='html')
        
        API_URL = "https://animepahe.ru/anime/airing"
        async with aiohttp.ClientSession() as session:
            async with session.get(API_URL, headers=HEADERS) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, "html.parser")
                    anime_list = soup.select(".index-wrapper .index a")
                    
                    if not anime_list:
                        await safe_respond(event, "<b>No airing anime available at the moment.</b>", parse_mode='html')
                        return
                    
                    airing_anime_text = "<b>🎬 Currently Airing Anime:</b>\n\n"
                    for idx, anime in enumerate(anime_list[:15], start=1):  # Limit to 15 results
                        title = anime.get("title", "Unknown Title")
                        href = anime.get("href", "")
                        
                        if href:
                            link = f"https://animepahe.ru{href}"
                            airing_anime_text += f"<b>{idx}. <a href='{link}'>{title}</a></b>\n"
                        else:
                            airing_anime_text += f"<b>{idx}. {title}</b>\n"
                    
                    await safe_respond(event, airing_anime_text, parse_mode='html', link_preview=False)
                else:
                    await safe_respond(event, f"<b>Failed to fetch data.</b> <i>Status code: {response.status}</i>", parse_mode='html')
    
    except Exception as e:
        logger.error(f"Error in airing_command: {e}")
        await safe_respond(event, "<b>Something went wrong.</b> <i>Please try again later.</i>", parse_mode='html')

# Button callbacks
@client.on(events.CallbackQuery(data=b"search_anime"))
async def search_anime_callback(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        await event.answer("❌ This feature is only available to the bot admin.", alert=True)
        return
    
    await safe_edit(event, "🔍 <b>Please send me the name of an anime you want to download:</b>", parse_mode='html')

@client.on(events.CallbackQuery(data=b"show_help"))
async def show_help_callback(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        await event.answer("❌ This feature is only available to the bot admin.", alert=True)
        return
    
    help_text = (
        "<b>🤖 Anime Downloader Bot Help</b>\n\n"
        "<b>🔍 How to use:</b>\n"
        "<i>1. Send me the name of an anime you want to download</i>\n"
        "<i>2. Select the anime from the search results</i>\n"
        "<i>3. Choose an episode to download</i>\n"
        "<i>4. Select the quality you prefer</i>\n"
        "<i>5. I'll download and send it to you!</i>\n\n"
        "<b>✅ Now supporting files up to 2GB!</b>\n\n"
        "<b>🔧 Commands:</b>\n"
        "<code>/start</code> - Start the bot\n"
        "<code>/help</code> - Show this help message\n"
        "<code>/cancel</code> - Cancel current operation\n"
        "<code>/ping</code> - Test if bot is working\n"
        "<code>/latest</code> - Get latest airing anime\n"
        "<code>/airing</code> - Get currently airing anime\n"
        "<code>/del_timer</code> - Set file delete timer\n\n"
        "<b>👤 Admin Commands:</b>\n"
        "<code>/auto_status</code> - Check auto download status\n"
        "<code>/auto_toggle</code> - Enable/disable auto downloads\n"
        "<code>/auto_check</code> - Manually check for new episodes (downloads highest quality)\n"
        "<code>/auto_interval [seconds]</code> - Set check interval\n"
        "<code>/process_latest</code> - Process the latest airing anime (downloads all qualities)\n"
        "<code>/addtask [number]</code> - Download specific anime from latest airing list\n"
        "<code>/redownload [number]</code> - Force redownload a specific anime\n"
        "<code>/set_qual</code> - Set quality preferences\n"
        "<code>/add_admin [user_id]</code> - Add a new admin\n"
        "<code>/remove_admin [user_id]</code> - Remove an admin\n"
        "<code>/start_pic</code> - Change start picture\n"
        "<code>/set_dump</code> - Change dump channel"
    )
    
    await safe_edit(event, help_text, parse_mode='html')

@client.on(events.CallbackQuery(data=b"auto_settings"))
async def auto_settings_callback(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        await event.answer("❌ This feature is only available to the bot admin.", alert=True)
        return
    
    enabled = auto_download_state.enabled
    interval = auto_download_state.interval
    last_checked = auto_download_state.last_checked
    
    status_text = (
        "<b>🤖 Auto Download Settings</b>\n\n"
        f"<b>📊 Status:</b> {'✅ <i>Enabled</i>' if enabled else '❌ <i>Disabled</i>'}\n"
        f"<b>⏱️ Check Interval:</b> <i>{interval} Seconds</i>\n"
        f"<b>🕒 Last Checked:</b> <i>{last_checked or 'Never'}</i>"
    )
    
    # Create buttons in 2 rows of 2
    if enabled:
        btn1 = Button.inline("❌ Disable Auto Download", b"auto_disable")
    else:
        btn1 = Button.inline("✅ Enable Auto Download", b"auto_enable")
    
    buttons = [
        [btn1, Button.inline("🔄 Check Now", b"auto_check_now")],
        [Button.inline("⚙️ Change Interval", b"auto_interval"), Button.inline("🔙 Back", b"back_to_main")]
    ]
    
    await safe_edit(event, status_text, buttons=buttons, parse_mode='html')

@client.on(events.CallbackQuery(data=b"auto_enable"))
async def auto_enable_callback(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        await event.answer("❌ This feature is only available to the bot admin.", alert=True)
        return
    
    auto_download_state.enabled = True
    await safe_edit(event, 
        "✅ <b>Auto download has been enabled.</b>", 
        buttons=[[Button.inline("🔙 Back to Settings", b"auto_settings")]], 
        parse_mode='html'
    )

@client.on(events.CallbackQuery(data=b"auto_disable"))
async def auto_disable_callback(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        await event.answer("❌ This feature is only available to the bot admin.", alert=True)
        return
    
    auto_download_state.enabled = False
    await safe_edit(event, 
        "❌ <b>Auto download has been disabled.</b>", 
        buttons=[[Button.inline("🔙 Back to Settings", b"auto_settings")]], 
        parse_mode='html'
    )

@client.on(events.CallbackQuery(data=b"auto_check_now"))
async def auto_check_now_callback(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        await event.answer("❌ This feature is only available to the bot admin.", alert=True)
        return
    
    await safe_edit(event, "🔄 <b>Checking for new episodes...</b>", parse_mode='html')
    
    # Run the check in the background
    asyncio.create_task(check_for_new_episodes(client))
    
    # After 10 seconds, update the message to show a back button
    await asyncio.sleep(10)
    await safe_edit(event, 
        "✅ <b>Check initiated.</b>", 
        buttons=[[Button.inline("🔙 Back to Settings", b"auto_settings")]], 
        parse_mode='html'
    )

@client.on(events.CallbackQuery(data=b"auto_interval"))
async def auto_interval_callback(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        await event.answer("❌ This feature is only available to the bot admin.", alert=True)
        return
    
    current_interval = auto_download_state.interval
    await safe_edit(event, 
        f"<b>⏱️ Current check interval:</b> <i>{current_interval} seconds</i>\n\n"
        "<b>Please send me the new interval in seconds (60-86400):</b>",
        parse_mode='html',
        buttons=[[Button.inline("🔙 Back", b"auto_settings")]]
    )
    
    # Set waiting for interval flag
    if event.chat_id not in user_states:
        user_states[event.chat_id] = UserState()
    
    user_states[event.chat_id]._waiting_for_interval = True

@client.on(events.CallbackQuery(data=b"back_to_main"))
async def back_to_main_callback(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        await event.answer("❌ This feature is only available to the bot admin.", alert=True)
        return
    
    user = await event.get_sender()
    
    # Send structured message with image
    try:
        start_pic_url = bot_settings.get("start_pic", START_PIC_URL)
        await client.send_file(
            event.chat_id,
            start_pic_url,
            caption=(
                "<b>👋 Hello, Blakite!</b>\n\n"
                "<i>I'm an Anime Downloader Bot. I can help you download anime episodes from AnimePahe.</i>\n\n"
                "<b>🔍 To get started, just send me the name of an anime you want to download.</b>\n\n"
                "<b>📝 Example:</b> <code>Attack on Titan</code>\n\n"
                "<b>✅ Now supporting files up to 2GB!</b>"
            ),
            parse_mode='HTML',
            buttons=[
                [Button.inline("🔍 Search Anime", b"search_anime"), Button.inline("❓ Help", b"show_help")],
                [Button.inline("⚙️ Auto Download Settings", b"auto_settings")]
            ]
        )
    except Exception as e:
        logger.error(f"Error sending start message: {e}")
        await safe_respond(event, "Welcome to Anime Downloader Bot! Use /help to see available commands.")

# Quality settings callbacks
@client.on(events.CallbackQuery(data=b"toggle_360p"))
async def toggle_360p_callback(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        await event.answer("❌ This feature is only available to the bot admin.", alert=True)
        return
    
    enabled_qualities = quality_settings.enabled_qualities
    if "360p" in enabled_qualities:
        enabled_qualities.remove("360p")
    else:
        enabled_qualities.append("360p")
    
    quality_settings.enabled_qualities = enabled_qualities
    await event.answer(f"360p {'enabled' if '360p' in enabled_qualities else 'disabled'}")
    
    # Refresh the settings menu
    await set_qual(event)

@client.on(events.CallbackQuery(data=b"toggle_720p"))
async def toggle_720p_callback(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        await event.answer("❌ This feature is only available to the bot admin.", alert=True)
        return
    
    enabled_qualities = quality_settings.enabled_qualities
    if "720p" in enabled_qualities:
        enabled_qualities.remove("720p")
    else:
        enabled_qualities.append("720p")
    
    quality_settings.enabled_qualities = enabled_qualities
    await event.answer(f"720p {'enabled' if '720p' in enabled_qualities else 'disabled'}")
    
    # Refresh the settings menu
    await set_qual(event)

@client.on(events.CallbackQuery(data=b"toggle_1080p"))
async def toggle_1080p_callback(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        await event.answer("❌ This feature is only available to the bot admin.", alert=True)
        return
    
    enabled_qualities = quality_settings.enabled_qualities
    if "1080p" in enabled_qualities:
        enabled_qualities.remove("1080p")
    else:
        enabled_qualities.append("1080p")
    
    quality_settings.enabled_qualities = enabled_qualities
    await event.answer(f"1080p {'enabled' if '1080p' in enabled_qualities else 'disabled'}")
    
    # Refresh the settings menu
    await set_qual(event)

@client.on(events.CallbackQuery(data=b"toggle_all"))
async def toggle_all_callback(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        await event.answer("❌ This feature is only available to the bot admin.", alert=True)
        return
    
    quality_settings.download_all = not quality_settings.download_all
    await event.answer(f"Download all enabled qualities {'enabled' if quality_settings.download_all else 'disabled'}")
    
    # Refresh the settings menu
    await set_qual(event)

@client.on(events.CallbackQuery(data=b"toggle_batch"))
async def toggle_batch_callback(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        await event.answer("❌ This feature is only available to the bot admin.", alert=True)
        return
    
    quality_settings.batch_mode = not quality_settings.batch_mode
    await event.answer(f"Batch mode {'enabled' if quality_settings.batch_mode else 'disabled'}")
    
    # Refresh the settings menu
    await set_qual(event)

# Message handlers
@client.on(events.NewMessage)
async def handle_message(event):
    # Skip messages sent by the bot itself
    if event.out:
        return

    if not isinstance(event.peer_id, PeerUser):
        return
    # --- END OF PRIVATE CHAT CHECK ---

    # --- NOW ADD THE FORCE SUB CHECK (it will only run in private chats) ---
    user_id = event.sender_id
    # Optional: Although event.peer_id is PeerUser, sender_id is still good to use.
    # It also handles scenarios like anon admins (rare in private chats).
    is_sub = await is_subscribed(client, user_id)
    if not is_sub:
        await not_joined(client, event)
        return # Stop processing if not subscribed
    
    # Check if user is admin
    if not is_admin(event.chat_id):
        return
    
    # Initialize user state if not exists
    if event.chat_id not in user_states:
        user_states[event.chat_id] = UserState()
    
    user_state = user_states[event.chat_id]
    
    # Skip non-text messages
    if not event.text:
        return
    
    # Check if this is a command
    if event.text.startswith('/'):
        # Rate limiting for commands
        current_time = time.time()
        if current_time - user_state.last_command_time < 3:  # 3 seconds cooldown
            return
        user_state.last_command_time = current_time
        return  # Let the command handlers handle this
    
    # Handle interval setting
    if hasattr(user_state, '_waiting_for_interval') and user_state._waiting_for_interval:
        try:
            interval = int(event.text.strip())
            if interval < 60 or interval > 86400:
                await safe_respond(event, "❌ <b>Interval must be between 60 and 86400 seconds (24 hours).</b>", parse_mode='html')
                return
            
            auto_download_state.interval = interval
            await safe_respond(event, 
                f"✅ <b>Check interval set to {interval} seconds.</b>", 
                buttons=[[Button.inline("🔙 Back to Settings", b"auto_settings")]], 
                parse_mode='html'
            )
            user_state._waiting_for_interval = False
            return
        except ValueError:
            await safe_respond(event, "❌ <b>Please provide a valid number.</b>", parse_mode='html')
            return
    
    # Handle anime search
    query = event.text.strip()
    if not query:
        await safe_respond(event, "❌ <b>Please enter an anime name to search.</b>", parse_mode='html')
        return
    
    # Rate limiting for searches
    current_time = time.time()
    if hasattr(user_state, 'rate_limited_until') and current_time < user_state.rate_limited_until:
        return  # Ignore during rate limiting period
    
    if current_time - user_state.last_command_time < 5:  # 5 seconds cooldown
        user_state.rate_limited_until = user_state.last_command_time + 5
        await safe_respond(event, "⚠️ <b>Please wait a moment before searching again.</b>", parse_mode='html')
        return
    
    user_state.last_command_time = current_time
    
    search_msg = await safe_respond(event, f"🔍 <b>Searching for:</b> <i>{query}</i>...", parse_mode='html')
    
    try:
        anime_results = await search_anime(query)
        if not anime_results:
            await safe_edit(search_msg, "❌ <b>Anime not found.</b> <i>Please try another name.</i>", parse_mode='html')
            return
    except Exception as e:
        logger.error(f"Error in anime search: {str(e)}")
        await safe_edit(search_msg, "❌ <b>An error occurred while searching.</b> <i>Please try again.</i>", parse_mode='html')
        return
    
    buttons = []
    for i, anime in enumerate(anime_results[:10]):
        buttons.append([Button.inline(
            f"{anime['title']} ({anime['year']}) - {anime['episodes']} eps",
            f"anime_{i}".encode()
        )])
    
    buttons.append([Button.inline("❌ Cancel", b"cancel_search")])
    
    user_state.anime_results = anime_results
    
    await safe_respond(event,
        "<b>📺 Search Results:</b>\n\n<b>Select an anime from the list below:</b>",
        buttons=buttons,
        parse_mode='html'
    )

@client.on(events.CallbackQuery())
async def handle_callback(event):
    # Check if user is admin
    if not is_admin(event.chat_id):
        await event.answer("❌ This feature is only available to the bot admin.", alert=True)
        return
    
    data = event.data.decode('utf-8')
    
    if event.chat_id not in user_states:
        user_states[event.chat_id] = UserState()
    
    user_state = user_states[event.chat_id]
    
    # Rate limiting for callbacks
    current_time = time.time()
    if current_time - user_state.last_command_time < 1:  # 1 second cooldown
        return
    user_state.last_command_time = current_time
    
    if data == 'cancel_search':
        await safe_edit(event, "❌ <b>Search cancelled.</b> <i>Send /start to begin again.</i>", parse_mode='html')
        return
    
    if data.startswith('anime_'):
        if not user_state.anime_results:
            await safe_edit(event, "❌ <b>Session expired.</b> <i>Please search again.</i>", parse_mode='html')
            return
        
        anime_index = int(data.split('_')[1])
        if anime_index >= len(user_state.anime_results):
            await safe_edit(event, "❌ <b>Invalid selection.</b> <i>Please try again.</i>", parse_mode='html')
            return
        
        selected_anime = user_state.anime_results[anime_index]
        anime_session = selected_anime['session']
        anime_title = selected_anime['title']
        
        # Check if batch mode is enabled
        if quality_settings.batch_mode:
            # Start batch download for this anime
            await safe_edit(event, f"🔄 <b>Starting batch download for:</b> <i>{anime_title}</i>\n\nThis may take a while...", parse_mode='html')
            success = await download_anime_batch(event, anime_session, anime_title)
            if success:
                await safe_respond(event, f"✅ <b>Batch download completed for:</b> <i>{anime_title}</i>", parse_mode='html')
            else:
                await safe_respond(event, f"❌ <b>Batch download failed for:</b> <i>{anime_title}</i>", parse_mode='html')
            return
        
        # Normal mode (not batch)
        user_state.anime_session = anime_session
        user_state.anime_title = anime_title
        user_state.total_episodes = selected_anime['episodes']
        
        await safe_edit(event, f"📺 <b>{user_state.anime_title}</b>\n\n<b>Fetching episode list...</b>", parse_mode='html')
        
        episode_data = await get_episode_list(user_state.anime_session)
        if not episode_data or 'data' not in episode_data:
            await safe_edit(event, "❌ <b>Failed to get episode list.</b> <i>Please try again.</i>", parse_mode='html')
            return
        
        episodes = episode_data.get('data', [])
        if not episodes:
            await safe_edit(event, "❌ <b>No episodes found for this anime.</b>", parse_mode='html')
            return
        
        total_pages = episode_data.get('last_page', 1)
        total_episodes = episode_data.get('total', len(episodes))
        user_state.episodes = episodes
        user_state.current_page = 1
        user_state.total_pages = total_pages
        user_state.total_episodes = total_episodes
        
        episodes_per_page = 10
        total_pages_this_batch = (len(episodes) + episodes_per_page - 1) // episodes_per_page
        current_batch_page = 1
        
        buttons = []
        start_idx = (current_batch_page - 1) * episodes_per_page
        end_idx = start_idx + episodes_per_page
        page_episodes = episodes[start_idx:end_idx]
        
        for ep in page_episodes:
            buttons.append([Button.inline(
                f"Episode {ep['episode']}: {ep['title']}",
                f"eps_{ep['episode']}".encode()
            )])
        
        nav_buttons = []
        show_nav = total_episodes > episodes_per_page
        
        if show_nav:
            if current_batch_page > 1:
                nav_buttons.append(Button.inline("⬅️ Prev", b"ep_prev"))
            nav_buttons.append(Button.inline(f"Page {current_batch_page}/{total_pages_this_batch}", b"ep_page"))
            if current_batch_page < total_pages_this_batch or user_state.current_page < total_pages:
                nav_buttons.append(Button.inline("Next ➡️", b"ep_next"))
            buttons.append(nav_buttons)
        
        buttons.append([Button.inline("❌ Cancel", b"cancel_episode")])
        
        await safe_edit(event,
            f"📺 <b>{user_state.anime_title}</b>\n\n<b>Episodes (Page 1/{user_state.total_pages}):</b>\n\n<b>Select an episode to download:</b>",
            buttons=buttons,
            parse_mode='html'
        )
    
    elif data.startswith(('ep_', 'eps_')):
        if data in ['ep_prev', 'ep_next', 'ep_page']:
            action = data.split('_')[1]
            current_page = user_state.current_page
            total_pages = user_state.total_pages
            anime_session = user_state.anime_session
            anime_title = user_state.anime_title
            episodes = user_state.episodes
            total_episodes = len(episodes)
            
            episodes_per_page = 10
            total_pages_this_batch = (total_episodes + episodes_per_page - 1) // episodes_per_page
            current_batch_page = user_state.current_batch_page if hasattr(user_state, 'current_batch_page') else 1
            
            if action == 'prev':
                if current_batch_page > 1:
                    current_batch_page -= 1
                elif current_page > 1:
                    current_page -= 1
                    episode_data = await get_episode_list(anime_session, current_page)
                    episodes = episode_data['data']
                    user_state.episodes = episodes
                    total_episodes = len(episodes)
                    total_pages_this_batch = (total_episodes + episodes_per_page - 1) // episodes_per_page
                    current_batch_page = total_pages_this_batch
            elif action == 'next':
                if current_batch_page * episodes_per_page < total_episodes:
                    current_batch_page += 1
                elif current_page < total_pages:
                    current_page += 1
                    episode_data = await get_episode_list(anime_session, current_page)
                    episodes = episode_data['data']
                    current_batch_page = 1
            
            user_state.current_page = current_page
            user_state.current_batch_page = current_batch_page
            
            buttons = []
            start_idx = (current_batch_page - 1) * episodes_per_page
            end_idx = min(start_idx + episodes_per_page, total_episodes)
            page_episodes = episodes[start_idx:end_idx]
            
            for ep in page_episodes:
                buttons.append([Button.inline(
                    f"Episode {ep['episode']}: {ep['title']}",
                    f"eps_{ep['episode']}".encode()
                )])
            
            nav_buttons = []
            show_nav = total_episodes > episodes_per_page
            
            if show_nav:
                if current_batch_page > 1 or current_page > 1:
                    nav_buttons.append(Button.inline("⬅️ Prev", b"ep_prev"))
                nav_buttons.append(Button.inline(
                    f"Page {current_batch_page}/{total_pages_this_batch}" +
                    (f" (Set {current_page}/{total_pages})" if total_pages > 1 else ""),
                    b"ep_page"
                ))
                if current_batch_page * episodes_per_page < total_episodes or current_page < total_pages:
                    nav_buttons.append(Button.inline("Next ➡️", b"ep_next"))
                buttons.append(nav_buttons)
            
            buttons.append([Button.inline("❌ Cancel", b"cancel_episode")])
            
            await safe_edit(event,
                f"📺 <b>{anime_title}</b>\n\n<b>Episodes (Page {current_page}/{total_pages}):</b>\n\n<b>Select an episode to download:</b>",
                buttons=buttons,
                parse_mode='html'
            )
        else:
            episode_num = int(data.split('_')[1])
            episodes = user_state.episodes
            
            selected_episode = None
            for ep in episodes:
                if int(ep['episode']) == episode_num:
                    selected_episode = ep
                    break
            
            if not selected_episode:
                await safe_edit(event, "❌ <b>Episode not found.</b> <i>Please try again.</i>", parse_mode='html')
                return
            
            episode_number = selected_episode['episode']
            episode_session = selected_episode['session']
            anime_session = user_state.anime_session
            anime_title = user_state.anime_title
            
            user_state.episode_session = episode_session
            user_state.episode_number = episode_number
            
            await safe_edit(event,
                f"📺 <b>{anime_title}</b>\n\n<b>Episode {episode_number}:</b> <i>{selected_episode['title']}</i>\n\n<b>Fetching download links...</b>",
                parse_mode='html'
            )
            
            download_links = get_download_links(anime_session, episode_session)
            if not download_links:
                await safe_edit(event, "❌ <b>No download links found for this episode.</b>", parse_mode='html')
                return
            
            user_state.download_links = download_links
            
            buttons = []
            for i, link in enumerate(download_links):
                buttons.append([Button.inline(
                    link['text'],
                    f"qual_{i}".encode()
                )])
            
            buttons.append([Button.inline("❌ Cancel", b"cancel_quality")])
            
            await safe_edit(event,
                f"📺 <b>{anime_title}</b>\n\n<b>Episode {episode_number}:</b> <i>{selected_episode['title']}</i>\n\n<b>Select download quality:</b>",
                buttons=buttons,
                parse_mode='html'
            )
    
    elif data.startswith('qual_'):
        quality_index = int(data.split('_')[1])
        download_links = user_state.download_links
        
        if not download_links:
            await safe_edit(event, "❌ <b>No download links found!</b> <i>Please try again.</i>", parse_mode='html')
            return
        
        if quality_index >= len(download_links):
            await safe_edit(event, "❌ <b>Invalid selection.</b> <i>Please try again.</i>", parse_mode='html')
            return
        
        selected_quality = download_links[quality_index]
        anime_session = user_state.anime_session
        anime_title = user_state.anime_title
        episode_number = user_state.episode_number
        episode_session = user_state.episode_session
        
        await download_episode(event, anime_title, anime_session, episode_number, episode_session, selected_quality)
    
    elif data == 'cancel_episode':
        await safe_edit(event, "❌ <b>Operation cancelled.</b> <i>Send /start to begin again.</i>", parse_mode='html')
    
    elif data == 'cancel_quality':
        await safe_edit(event, "❌ <b>Operation cancelled.</b> <i>Send /start to begin again.</i>", parse_mode='html')
    
    elif data.startswith('download_closest_'):
        # Handle the download closest episode button
        episode_number = int(data.split('_')[2])
        
        # Get the anime information from user state
        anime_title = user_state.anime_title
        anime_session = user_state.anime_session
        
        # Find the episode in the episode list
        target_episode = None
        for ep in user_state.episodes:
            if int(ep['episode']) == episode_number:
                target_episode = ep
                break
        
        if not target_episode:
            await safe_edit(event, "❌ <b>Episode not found.</b> <i>Please try again.</i>", parse_mode='html')
            return
        
        episode_session = target_episode['session']
        
        # Get download links
        download_links = get_download_links(anime_session, episode_session)
        if not download_links:
            await safe_edit(event, "❌ <b>No download links found for this episode.</b>", parse_mode='html')
            return
        
        # Update user state
        user_state.episode_session = episode_session
        user_state.episode_number = episode_number
        user_state.download_links = download_links
        
        # Show quality selection
        buttons = []
        for i, link in enumerate(download_links):
            buttons.append([Button.inline(
                link['text'],
                f"qual_{i}".encode()
            )])
        
        buttons.append([Button.inline("❌ Cancel", b"cancel_quality")])
        
        await safe_edit(event,
            f"📺 <b>{anime_title}</b>\n\n<b>Episode {episode_number}:</b> <i>{target_episode['title']}</i>\n\n<b>Select download quality:</b>",
            buttons=buttons,
            parse_mode='html'
        )
    
    elif data == 'cancel_download':
        await safe_edit(event, "❌ <b>Download cancelled.</b> <i>Send /start to begin again.</i>", parse_mode='html')

# Download episode function
async def download_episode(event, anime_title, anime_session, episode_number, episode_session, quality_link):
    user_id = event.chat_id
    
    # Initialize progress message
    progress = ProgressMessage(client, user_id, f"⏳ <b>Preparing to download:</b> <i>{anime_title}</i> <b>Episode</b> <i>{episode_number}</i>")
    if not await progress.send():
        await safe_respond(event, "❌ <b>Failed to initialize progress tracking</b>", parse_mode='html')
        return
    
    resolution_match = re.search(r"\b(\d{3,4}p)\b", quality_link['text'])
    if not resolution_match:
        await progress.update("<b>Could not determine resolution for the selected quality.</b>")
        return
    
    resolution = resolution_match.group(1)
    is_dub = 'eng' in quality_link['text'].lower()
    type_str = "Dub" if is_dub else "Sub"
    
    # Create filename using the new format
    base_name = format_filename(anime_title, episode_number, resolution, type_str)
    # Get main channel username for caption
    main_channel_username = CHANNEL_USERNAME if CHANNEL_USERNAME else BOT_USERNAME
    full_caption = f"{base_name} {main_channel_username}"
    filename = sanitize_filename(full_caption) + ".mkv"
    download_path = os.path.join(DOWNLOAD_DIR, filename)
    
    await progress.update(f"⏳ <b>Preparing to download:</b> <i>{filename}</i>\n\n<b>Processing download link...</b>")
    
    kwik_link = extract_kwik_link(quality_link['href'])
    if not kwik_link:
        await progress.update("❌ <b>Failed to extract download link.</b>")
        return
    
    try:
        direct_link = get_dl_link(kwik_link)
        if not direct_link:
            await progress.update("❌ <b>Failed to get direct download link.</b>")
            return
    except Exception as e:
        await progress.update(f"❌ <b>Error generating download link:</b> <i>{str(e)}</i>")
        return
    
    await progress.update("⏬ <b>Starting download...</b>")
    
    try:
        # Progress tracking for download
        last_update = time.time()
        download_start = time.time()
        
        def progress_hook(d):
            nonlocal last_update
            if d['status'] == 'downloading':
                current_time = time.time()
                if current_time - last_update >= 5:  # Update every 5 seconds
                    # Safely get values with defaults and None checks
                    downloaded_bytes = d.get('downloaded_bytes')
                    total_bytes = d.get('total_bytes')
                    speed = d.get('speed')
                    
                    # Convert to integers with fallbacks
                    downloaded = downloaded_bytes if downloaded_bytes is not None else 0
                    total = total_bytes if total_bytes is not None else 1
                    speed_val = speed if speed is not None else 0
                    
                    # Ensure we have valid numbers
                    try:
                        downloaded = int(downloaded)
                        total = int(total)
                        speed_val = float(speed_val)
                    except (ValueError, TypeError):
                        downloaded = 0
                        total = 1
                        speed_val = 0.0
                    
                    # Calculate percentage safely
                    if total > 0:
                        percent = min(100, (downloaded / total) * 100)
                    else:
                        percent = 0
                    
                    progress_text = (
                        f"⏬ <b>Dᴏᴡɴʟᴏᴀᴅɪɴɢ:</b> <i>{filename}</i>\n\n"
                        f"📊 <b>Progress:</b> <i>{percent:.1f}%</i>\n"
                        f"📦 <b>{format_size(downloaded)}/{format_size(total)}</b>\n"
                        f"⚡ <b>{format_speed(speed_val)}</b>"
                    )
                    
                    try:
                        asyncio.create_task(progress.update(progress_text))
                    except:
                        pass
                    
                    last_update = current_time
        
        ydl_opts = {
            'outtmpl': download_path,
            'quiet': True,
            'no_warnings': True,
            'http_headers': YTDLP_HEADERS,  # Use enhanced headers
            'downloader_args': {'chunk_size': 10485760},  # 10MB chunks for faster download
            'progress_hooks': [progress_hook],
            'nocheckcertificate': True,  # Disable SSL certificate verification
            'compat_opts': ['no-keep-video'],  # Additional compatibility options
        }
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([direct_link])
        
        # Check if file exists
        if not os.path.exists(download_path) or os.path.getsize(download_path) < 1000:
            raise Exception("Downloaded file is too small or doesn't exist")
        
        download_time = time.time() - download_start
        file_size = os.path.getsize(download_path)
        avg_speed = file_size / download_time if download_time > 0 else 0
        
        await progress.update(
            f"<b>Dᴏᴡɴʟᴏᴀᴅ ᴄᴏᴍᴘʟᴇᴛᴇᴅ!</b>\n"
            f"<b>Time:</b> <i>{download_time:.1f}s</i>\n"
            f"<b>Size:</b> <i>{format_size(file_size)}</i>\n"
            f"<b>Avg Sᴘᴇᴇᴅ:</b> <i>{format_speed(avg_speed)}</i>\n\n"
            f"<b>Preparing upload...</b>"
        )
    except Exception as e:
        logger.error(f"yt-dlp failed: {str(e)}")
        
        try:
            session = requests.Session()
            session.headers.update(YTDLP_HEADERS)
            response = session.get(direct_link, stream=True, timeout=30)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            start_time = time.time()
            last_update = start_time
            last_downloaded = 0
            
            with open(download_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024*1024):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        current_time = time.time()
                        if current_time - last_update >= 5:
                            time_diff = current_time - last_update
                            bytes_diff = downloaded - last_downloaded
                            speed = bytes_diff / time_diff if time_diff > 0 else 0
                            
                            if total_size > 0:
                                percent = min(100, (downloaded / total_size) * 100)
                                progress_text = (
                                    f"<b>Dᴏᴡɴʟᴏᴀᴅɪɴɢ:</b> <i>{filename}</i>\n\n"
                                    f"<b>Pʀᴏɢʀᴇss:</b> <i>{percent:.1f}%</i>\n"
                                    f"<b>Size:</b> <i>{format_size(downloaded)}/{format_size(total_size)}</i>\n"
                                    f"<b>Sᴘᴇᴇᴅ:</b> <i>{format_speed(speed)}</i>"
                                )
                            else:
                                progress_text = (
                                    f"<b>Dᴏᴡɴʟᴏᴀᴅɪɴɢ:</b> <i>{filename}</i>\n\n"
                                    f"<b>Dᴏᴡɴʟᴏᴀᴅᴇᴅ:</b> <i>{format_size(downloaded)}</i>\n"
                                    f"<b>Sᴘᴇᴇᴅ:</b> <i>{format_speed(speed)}</i>"
                                )
                            
                            try:
                                await progress.update(progress_text)
                            except:
                                pass
                            
                            last_update = current_time
                            last_downloaded = downloaded
            
            if os.path.getsize(download_path) < 1000:
                raise Exception("Downloaded file is too small")
        except Exception as e:
            logger.error(f"Dᴏᴡɴʟᴏᴀᴅ ғᴀɪʟᴇᴅ: {str(e)}")
            await progress.update(f"<b>Dᴏᴡɴʟᴏᴀᴅ ғᴀɪʟᴇᴅ:</b> <i>{str(e)}</i>")
            return
    
    await progress.update("<b>Dᴏᴡɴʟᴏᴀᴅ ᴄᴏᴍᴘʟᴇᴛᴇᴅ!</b> <b>Sᴛᴀʀᴛɪɴɢ ᴜᴘʟᴏᴀᴅ...</b>")
    
    upload_progress = UploadProgressBar(client, user_id, full_caption)
    
    try:
        file_size = os.path.getsize(download_path)
        caption = full_caption
        
        try:
            thumb = await get_fixed_thumbnail()
            
            dump_msg_id = None
            if DUMP_CHANNEL_ID:
                try:
                    msg = await client.send_file(
                        DUMP_CHANNEL_ID,
                        download_path,
                        caption=caption,
                        thumb=thumb,
                        force_document=True,
                        attributes=None,
                        supports_streaming=False,
                        progress_callback=upload_progress.update,
                        part_size_kb=8192
                    )
                    
                    await upload_progress.finish()
                    
                    dump_msg_id = msg.id
                    channel_msg = f"\n<b>Aʟsᴏ ᴘᴏsᴛᴇᴅ ᴛᴏ ᴅᴜᴍᴘ ᴄʜᴀɴɴᴇʟ</b>"
                except Exception as e:
                    logger.error(f"Failed to send to dump channel: {e}")
                    channel_msg = f"\n<b>Fᴀɪʟᴇᴅ ᴛᴏ ᴘᴏsᴛ ᴛᴏ ᴅᴜᴍᴘ ᴄʜᴀɴɴᴇʟ:</b> <i>{str(e)}</i>"
            elif DUMP_CHANNEL_USERNAME:
                try:
                    msg = await client.send_file(
                        DUMP_CHANNEL_USERNAME,
                        download_path,
                        caption=caption,
                        thumb=thumb,
                        force_document=True,
                        attributes=None,
                        supports_streaming=False,
                        progress_callback=upload_progress.update,
                        part_size_kb=8192
                    )
                    
                    await upload_progress.finish()
                    
                    dump_msg_id = msg.id
                    channel_msg = f"\n<b>Aʟsᴏ ᴘᴏsᴛᴇᴅ ᴛᴏ ᴅᴜᴍᴘ ᴄʜᴀɴɴᴇʟ</b>"
                except Exception as e:
                    logger.error(f"Failed to send to dump channel: {e}")
                    channel_msg = f"\n<b>Fᴀɪʟᴇᴅ ᴛᴏ ᴘᴏsᴛ ᴛᴏ ᴅᴜᴍᴘ ᴄʜᴀɴɴᴇʟ:</b> <i>{str(e)}</i>"
            else:
                channel_msg = ""
            
            await progress.update(f"<b>Sᴜᴄᴄᴇssғᴜʟʟʏ sᴇɴᴛ ʏᴏᴜʀ ᴠɪᴅᴇᴏ!</b>{channel_msg}")
        except FloodWaitError as e:
            logger.error(f"Flood wait during initial upload: {e.seconds} seconds")

            try:
                await safe_send_message(
                    user_id,
                    f"<b>Tᴇʟᴇɢʀᴀᴍ ʀᴀᴛᴇ ʟɪᴍɪᴛ ʜɪᴛ.</b> <i>Wᴀɪᴛɪɴɢ {e.seconds} sᴇᴄᴏɴᴅs...</i>",
                    parse_mode='html'
                )
            except FloodWaitError as e2:
                logger.error(f"Flood wait when sending rate limit message: {e2.seconds}")
            
            await asyncio.sleep(e.seconds + 5)
            

            if DUMP_CHANNEL_ID:
                try:
                    msg = await client.send_file(
                        DUMP_CHANNEL_ID,
                        download_path,
                        caption=caption,
                        thumb=thumb,
                        force_document=True,
                        attributes=None,
                        supports_streaming=False,
                        part_size_kb=8192
                    )
                    
                    dump_msg_id = msg.id
                    channel_msg = f"\n<b>Aʟsᴏ ᴘᴏsᴛᴇᴅ ᴛᴏ ᴅᴜᴍᴘ ᴄʜᴀɴɴᴇʟ</b>"
                except Exception as e:
                    logger.error(f"Failed to send to dump channel: {e}")
                    channel_msg = f"\n<b>Fᴀɪʟᴇᴅ ᴛᴏ ᴘᴏsᴛ ᴛᴏ ᴅᴜᴍᴘ ᴄʜᴀɴɴᴇʟ:</b> <i>{str(e)}</i>"
            elif DUMP_CHANNEL_USERNAME:
                try:
                    msg = await client.send_file(
                        DUMP_CHANNEL_USERNAME,
                        download_path,
                        caption=caption,
                        thumb=thumb,
                        force_document=True,
                        attributes=None,
                        supports_streaming=False,
                        part_size_kb=8192
                    )
                    
                    dump_msg_id = msg.id
                    channel_msg = f"\n<b>Aʟsᴏ ᴘᴏsᴛᴇᴅ ᴛᴏ ᴅᴜᴍᴘ ᴄʜᴀɴɴᴇʟ</b>"
                except Exception as e:
                    logger.error(f"Failed to send to dump channel: {e}")
                    channel_msg = f"\n<b>Fᴀɪʟᴇᴅ ᴛᴏ ᴘᴏsᴛ ᴛᴏ ᴅᴜᴍᴘ ᴄʜᴀɴɴᴇʟ:</b> <i>{str(e)}</i>"
            else:
                channel_msg = ""
            
            await safe_send_message(user_id, f"<b>Sᴜᴄᴄᴇssғᴜʟʟʏ sᴇɴᴛ ʏᴏᴜʀ ᴠɪᴅᴇᴏ ᴀғᴛᴇʀ ʀᴇᴛʀʏ!</b>{channel_msg}", parse_mode='html')
        except Exception as e:
            logger.error(f"Eʀʀᴏʀ sᴇɴᴅɪɴɢ ᴠɪᴅᴇᴏ: {str(e)}")
            try:
                await safe_send_message(user_id, f"<b>Eʀʀᴏʀ sᴇɴᴅɪɴɢ ᴠɪᴅᴇᴏ:</b> <i>{str(e)}</i>", parse_mode='html')
            except FloodWaitError:
                logger.error("Flood wait when sending error message")
    except Exception as e:
        logger.error(f"Eʀʀᴏʀ ɪɴ ᴜᴘʟᴏᴀᴅ ᴘʀᴏᴄᴇss: {str(e)}")
        try:
            await safe_send_message(user_id, f"<b>Eʀʀᴏʀ ɪɴ ᴜᴘʟᴏᴀᴅ ᴘʀᴏᴄᴇss:</b> <i>{str(e)}</i>", parse_mode='html')
        except FloodWaitError:
            logger.error("Flood wait when sending error message")
    
    try:
        os.remove(download_path)
    except:
        pass

app = FastAPI()

@app.get("/health")
async def health_check():
    return JSONResponse(
        status_code=200,
        content={"status": "healthy", "message": "Bot is running"}
    )

async def main():
    try:
        start_pic_path = download_start_pic_if_not_exists(START_PIC_URL)
        force_pic_path = download_force_pic_if_not_exists(FORCE_PIC)
        
        server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=PORT, log_level="info"))
        asyncio.create_task(server.serve())
        
        await client.start(bot_token=BOT_TOKEN)
        logger.info("Bot starting...")
        
        setup_scheduler(client)
        
        logger.info("Bot started!")
        
        await client.run_until_disconnected()
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        logger.info("Attempting to restart in 30 seconds...")
        await asyncio.sleep(30)
        await main()

if __name__ == '__main__':
    import asyncio
    asyncio.get_event_loop().run_until_complete(main())
