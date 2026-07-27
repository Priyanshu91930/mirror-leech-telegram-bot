import os
import shutil
import asyncio
import aiohttp
from time import time
from pathlib import Path
from secrets import token_urlsafe
from functools import partial

from mega.client import Mega
from mega.errors import RequestError
from natsort import natsorted
from pyrogram.filters import regex, user
from pyrogram.handlers import CallbackQueryHandler

from .... import (
    LOGGER,
    task_dict,
    task_dict_lock,
    DOWNLOAD_DIR,
)
from ....core.config_manager import Config
from ...ext_utils.task_manager import check_running_tasks, stop_duplicate_check
from ...ext_utils.bot_utils import new_task
from ...mirror_leech_utils.status_utils.mega_status import MegaStatus
from ...mirror_leech_utils.status_utils.queue_status import QueueStatus
from ...telegram_helper.button_build import ButtonMaker
from ...telegram_helper.message_utils import (
    send_status_message,
    send_message,
    delete_message,
    edit_message,
)

@new_task
async def selectMegaFolder(_, query, obj):
    data = query.data.split()
    await query.answer()
    action = data[1]

    if action == "select":
        obj.folder_path.append(obj.current_folder_id)
        obj.current_folder_id = data[2]
        obj.event.set()
    elif action == "download":
        obj.action = "download"
        obj.event.set()
    elif action == "download_all":
        obj.action = "download_all"
        obj.event.set()
    elif action == "back":
        if obj.folder_path:
            obj.current_folder_id = obj.folder_path.pop()
        obj.event.set()
    elif action == "cancel":
        obj.listener.is_cancelled = True
        obj.event.set()

class MegaFolderHelper:
    def __init__(self, listener, nodes, root_id):
        self.listener = listener
        self.nodes = nodes
        self.root_id = root_id
        self.current_folder_id = root_id
        self.folder_path = []
        self.selected_node_id = None
        self.action = None
        self.event = asyncio.Event()
        self._reply_to = None

    def _get_subfolders(self, parent_id):
        subfolders = []
        for node_id, node in self.nodes.items():
            if node.get("p") == parent_id and node.get("t") == 1:
                subfolders.append((node_id, node.get("attributes", {}).get("n", "Unknown Folder")))
        return subfolders

    async def _render_and_wait(self):
        pfunc = partial(selectMegaFolder, obj=self)
        handler = self.listener.client.add_handler(
            CallbackQueryHandler(
                pfunc, filters=regex("^megasel") & user(self.listener.user_id)
            ),
            group=-1,
        )
        try:
            while True:
                subfolders = self._get_subfolders(self.current_folder_id)

                buttons = ButtonMaker()
                for node_id, name in subfolders[:20]:
                    buttons.data_button(name[:30], f"megasel select {node_id}")

                buttons.data_button("Download This Folder", "megasel download", position="footer")

                if self.current_folder_id == self.root_id:
                    buttons.data_button("Download All", "megasel download_all", position="footer")

                if self.folder_path:
                    buttons.data_button("Back", "megasel back", position="footer")

                buttons.data_button("Cancel", "megasel cancel", position="footer")
                button = buttons.build_menu(2)

                current_name = self.nodes.get(self.current_folder_id, {}).get("attributes", {}).get("n", "MegaFolder")
                msg = f"<b>{current_name}</b>"
                if subfolders:
                    msg += f"\n{len(subfolders)} subfolder(s). Select to navigate:"
                else:
                    msg += "\nNo subfolders. Download this folder?"
                msg += "\nTimeout: 3 min"

                if self._reply_to:
                    await edit_message(self._reply_to, msg, button)
                else:
                    self._reply_to = await send_message(self.listener.message, msg, button)

                self.event.clear()
                self.action = None
                await asyncio.wait_for(self.event.wait(), timeout=180)

                if self.listener.is_cancelled:
                    return False

                if self.action == "download":
                    self.selected_node_id = self.current_folder_id
                    return True
                if self.action == "download_all":
                    self.selected_node_id = self.root_id
                    return True

        except asyncio.TimeoutError:
            await edit_message(self._reply_to, "Timed Out. Task has been cancelled!")
            self.listener.is_cancelled = True
            return False
        finally:
            self.listener.client.remove_handler(*handler)

    async def wait_for_selection(self):
        result = await self._render_and_wait()
        if not self.listener.is_cancelled:
            await delete_message(self._reply_to)
        return result

class ProxyClientSession(aiohttp.ClientSession):
    def __init__(self, proxy=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.proxy = proxy

    async def _request(self, method, str_or_url, *args, **kwargs):
        if self.proxy:
            kwargs['proxy'] = self.proxy
        return await super()._request(method, str_or_url, *args, **kwargs)

class CustomProgress:
    def __init__(self, helper):
        self.helper = helper

    def add_task(self, name, total):
        task_id = id(name)
        return task_id

    def advance(self, task_id, amount):
        self.helper.update_progress(amount)

    def remove_task(self, task_id):
        pass

class CustomProgressBar:
    def __init__(self, helper):
        self.progress = CustomProgress(helper)
        self.enabled = False

    def __enter__(self):
        return self.progress

    def __exit__(self, *args, **kwargs):
        pass

class MegaDownloadHelper:
    def __init__(self, listener):
        self.listener = listener
        self._processed_bytes = 0
        self._start_time = 0
        self._id = token_urlsafe(10)
        self.client = None
        self._proxies = [self._parse_proxy(p) for p in Config.MEGA_PROXIES if p]
        self._current_proxy_index = 0

    @property
    def speed(self):
        elapsed = time() - self._start_time
        if elapsed <= 0:
            return 0
        return self._processed_bytes / elapsed

    @property
    def processed_bytes(self):
        return self._processed_bytes

    def update_progress(self, amount):
        self._processed_bytes += amount
        if self.listener.is_cancelled:
            raise asyncio.CancelledError("Stopped by user!")

    def _parse_proxy(self, proxy_str):
        if not proxy_str:
            return None
        proxy_str = proxy_str.strip()
        if proxy_str.startswith(('http://', 'https://', 'socks5://')):
            return proxy_str
        parts = proxy_str.split(':')
        if len(parts) == 4:
            ip, port, user, password = parts
            return f"http://{user}:{password}@{ip}:{port}"
        elif len(parts) == 2:
            ip, port = parts
            return f"http://{ip}:{port}"
        return proxy_str

    async def _init_client(self):
        proxy = self._proxies[self._current_proxy_index] if self._proxies else None
        if proxy:
            LOGGER.info(f"Using proxy for Mega download: {proxy}")
        
        self.client = Mega(use_progress_bar=False)
        self.client._progress_bar = CustomProgressBar(self)
        
        if proxy:
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            self.client.api.session = ProxyClientSession(proxy=proxy, timeout=timeout)
        
        accounts = Config.MEGA_ACCOUNTS
        if not accounts and Config.MEGA_EMAIL:
            accounts = [(Config.MEGA_EMAIL, Config.MEGA_PASSWORD)]
            
        if accounts:
            if not hasattr(self, "_current_account_index"):
                self._current_account_index = 0
            self._current_account_index %= len(accounts)
            email, password = accounts[self._current_account_index]
            LOGGER.info(f"Logging in to Mega account: {email}")
            await self.client.login(email, password)
        else:
            LOGGER.info("Logging in anonymous temporary user...")
            await self.client.login(email=None, password=None)

    async def cancel_task(self):
        self.listener.is_cancelled = True
        LOGGER.info(f"Cancelling Mega download: {self.listener.name}")

    async def download(self, path):
        msg, button = await stop_duplicate_check(self.listener)
        if msg:
            await self.listener.on_download_error(msg, button)
            return

        add_to_queue, event = await check_running_tasks(self.listener)
        if add_to_queue:
            LOGGER.info(f"Added to Queue/Download: {self.listener.name}")
            async with task_dict_lock:
                task_dict[self.listener.mid] = QueueStatus(self.listener, self._id, "dl")
            await self.listener.on_download_start()
            if self.listener.multi <= 1 and not self.listener.is_rss:
                await send_status_message(self.listener.message)
            await event.wait()
            if self.listener.is_cancelled:
                return

        self._start_time = time()
        async with task_dict_lock:
            task_dict[self.listener.mid] = MegaStatus(self.listener, self, self._id, "dl")

        if add_to_queue:
            LOGGER.info(f"Start Queued Download from Mega: {self.listener.name}")
        else:
            LOGGER.info(f"Download from Mega: {self.listener.name}")
            await self.listener.on_download_start()
            if self.listener.multi <= 1 and not self.listener.is_rss:
                await send_status_message(self.listener.message)

        temp_dir = os.path.join(path, f"mega_tmp_{self._id}")
        os.makedirs(temp_dir, exist_ok=True)

        url = self.listener.link
        is_folder = "folder/" in url or "#F!" in url

        num_accounts = len(Config.MEGA_ACCOUNTS) if Config.MEGA_ACCOUNTS else 1
        num_proxies = len(self._proxies) if self._proxies else 1
        retries = max(num_accounts, num_proxies, 10)
        success = False
        error_msg = ""
        processed_files = set()
        for attempt in range(retries):
            if self.listener.is_cancelled:
                break
            try:
                await self._init_client()
                
                if is_folder:
                    folder_id, _ = self.client._parse_folder_url(url)
                    public_folder_id = folder_id
                    nodes = await self.client.get_nodes_public_folder(url)
                    root_id = next(iter(nodes))
                    
                    if not self.listener.name:
                        self.listener.name = nodes[root_id].get("attributes", {}).get("n", "MegaFolder")
                    
                    selected_node_id = root_id
                    root_subfolders = [
                        n for n in nodes.values()
                        if n.get("p") == root_id and n.get("t") == 1
                    ]
                    if root_subfolders:
                        helper = MegaFolderHelper(self.listener, nodes, root_id)
                        if not await helper.wait_for_selection():
                            return
                        selected_node_id = helper.selected_node_id
                    
                    if selected_node_id != root_id:
                        self.listener.name = nodes[selected_node_id].get("attributes", {}).get("n", "SubFolder")
                        folder_id = selected_node_id
                    
                    fs = await self.client._build_file_system(nodes, [selected_node_id])
                    fs = dict(natsorted(fs.items(), key=lambda x: x[0]))
                    self.listener.size = sum(node.get("s", 0) for node in fs.values() if node.get("t") == 0)
                    
                    LOGGER.info(f"Downloading Mega Folder: {self.listener.name} ({self.listener.size} bytes)")
                    
                    if self.listener.is_leech:
                        from ...mirror_leech_utils.telegram_uploader import TelegramUploader
                        from ...ext_utils.db_handler import database
                        
                        folder_name = self.listener.name
                        total_files = sum(1 for n in fs.values() if n.get("t") == 0)
                        all_uploaded_files = {}
                        total_corrupted = 0
                        
                        resume_dir = "mega_resume_state"
                        os.makedirs(resume_dir, exist_ok=True)
                        state_file = os.path.join(resume_dir, f"{folder_id}.txt")
                        
                        if not database._return:
                            db_state = await database.db.mega_resume.find_one({"_id": folder_id})
                            if db_state and not processed_files:
                                processed_files = set(db_state.get("processed_files", []))
                                LOGGER.info(f"Loaded {len(processed_files)} completed files from MongoDB resume state.")
                        
                        if not processed_files and os.path.exists(state_file):
                            with open(state_file, "r", encoding="utf-8") as f:
                                for line in f:
                                    if line.strip():
                                        processed_files.add(line.strip())
                            LOGGER.info(f"Loaded {len(processed_files)} completed files from local resume state.")
                        
                        count = 0
                        for rel_path, node in fs.items():
                            rel_path = str(rel_path)
                            if node["t"] != 0: # 0 represents NodeType.FILE
                                continue
                            if self.listener.is_cancelled:
                                break
                            
                            count += 1
                            if rel_path in processed_files:
                                continue
                            file_basename = os.path.basename(rel_path)
                            self.listener.name = f"{folder_name} | [{count}/{total_files}] {file_basename}"
                            LOGGER.info(f"Processing folder file {count}/{total_files}: {rel_path}")
                            
                            # 1. Download single file
                            sub_temp_dir = os.path.join(temp_dir, f"part_{count}")
                            try:
                                file_data = await self.client.api.request(
                                    {
                                        "a": "g",
                                        "g": 1,
                                        "n": node["h"],
                                    },
                                    {"n": public_folder_id},
                                )
                                file_url = file_data["g"]
                                file_size = file_data["s"]
                                
                                os.makedirs(sub_temp_dir, exist_ok=True)
                                download_path = Path(sub_temp_dir) / rel_path
                                os.makedirs(download_path.parent, exist_ok=True)
                                
                                await self.client._really_download_file(
                                    file_url,
                                    download_path,
                                    file_size,
                                    node["iv"],
                                    node["meta_mac"],
                                    node["k_decrypted"],
                                )
                                
                                # 2. Upload to Telegram immediately
                                if not self.listener.is_cancelled:
                                    uploader = TelegramUploader(self.listener, sub_temp_dir, is_sub_upload=True)
                                    msgs = await uploader.upload()
                                    if msgs:
                                        all_uploaded_files.update(msgs)
                                    total_corrupted += uploader._corrupted
                                
                                # 3. Delete from disk immediately to save space
                                shutil.rmtree(sub_temp_dir, ignore_errors=True)
                                processed_files.add(rel_path)
                                if not database._return:
                                    await database.db.mega_resume.update_one(
                                        {"_id": folder_id},
                                        {"$addToSet": {"processed_files": rel_path}},
                                        upsert=True
                                    )
                                with open(state_file, "a", encoding="utf-8") as f:
                                    f.write(rel_path + "\n")
                            except Exception as fe:
                                if "blocked" in str(fe).lower() or "eblocked" in str(fe).lower():
                                    LOGGER.warning(f"File blocked by Mega (DMCA/Suspended), skipping: {rel_path}")
                                    shutil.rmtree(sub_temp_dir, ignore_errors=True)
                                    processed_files.add(rel_path)
                                    if not database._return:
                                        await database.db.mega_resume.update_one(
                                            {"_id": folder_id},
                                            {"$addToSet": {"processed_files": rel_path}},
                                            upsert=True
                                        )
                                    with open(state_file, "a", encoding="utf-8") as f:
                                        f.write(rel_path + "\n")
                                    continue
                                raise fe
                        
                        self.listener.name = folder_name
                        if not database._return:
                            await database.db.mega_resume.delete_one({"_id": folder_id})
                        if os.path.exists(state_file):
                            try:
                                os.remove(state_file)
                            except:
                                pass
                        
                        if not self.listener.is_cancelled:
                            await self.listener.on_upload_complete(
                                None, all_uploaded_files, total_files, total_corrupted
                            )
                        return
                    else:
                        sem = asyncio.Semaphore(4) # Limit concurrent downloads to 4 for non-leech tasks (mirror)
                        
                        async def download_file_sem(file_node, file_rel_path):
                            async with sem:
                                file_data = await self.client.api.request(
                                    {
                                        "a": "g",
                                        "g": 1,
                                        "n": file_node["h"],
                                    },
                                    {"n": public_folder_id},
                                )
                                file_url = file_data["g"]
                                file_size = file_data["s"]
                                download_path = Path(temp_dir) / file_rel_path
                                await self.client._really_download_file(
                                    file_url,
                                    download_path,
                                    file_size,
                                    file_node["iv"],
                                    file_node["meta_mac"],
                                    file_node["k_decrypted"],
                                )

                        download_tasks = []
                        for rel_path, node in fs.items():
                            if node["t"] != 0: # 0 represents NodeType.FILE
                                continue
                            download_tasks.append(download_file_sem(node, Path(rel_path)))

                        with self.client._progress_bar:
                            await asyncio.gather(*download_tasks)
                else:
                    info = await self.client.get_public_url_info(url)
                    self.listener.size = info["size"]
                    if not self.listener.name:
                        self.listener.name = info["name"]
                    
                    LOGGER.info(f"Downloading Mega File: {self.listener.name} ({self.listener.size} bytes)")
                    await self.client.download_url(url, dest_path=temp_dir)

                success = True
                break
            except Exception as e:
                error_msg = str(e)
                LOGGER.error(f"Mega download attempt {attempt+1} failed: {e}")
                
                if self.client and self.client.api and self.client.api.session:
                    await self.client.api.session.close()

                # Rotate Mega account on bandwidth limit/quota errors
                if "bandwidth" in error_msg.lower() or "509" in error_msg or "overquota" in error_msg.lower() or "over quota" in error_msg.lower() or "temporary" in error_msg.lower():
                    accounts = Config.MEGA_ACCOUNTS
                    if accounts and len(accounts) > 1:
                        if not hasattr(self, "_current_account_index"):
                            self._current_account_index = 0
                        self._current_account_index += 1
                        LOGGER.info(f"Rotating to next Mega account: {accounts[self._current_account_index % len(accounts)][0]}")
                        self._processed_bytes = 0
                        continue

                # Rotate proxy on proxy/timeout errors
                if not error_msg or isinstance(e, (asyncio.TimeoutError, TimeoutError)) or "payment" in error_msg.lower() or "402" in error_msg or "timeout" in error_msg.lower() or "connect" in error_msg.lower():
                    if self._proxies and attempt < len(self._proxies) - 1:
                        self._current_proxy_index += 1
                        LOGGER.info(f"Rotating to next proxy: {self._proxies[self._current_proxy_index]}")
                        self._processed_bytes = 0
                        if os.path.exists(temp_dir):
                            for filename in os.listdir(temp_dir):
                                filepath = os.path.join(temp_dir, filename)
                                if os.path.isfile(filepath) or os.path.islink(filepath):
                                    os.unlink(filepath)
                                elif os.path.isdir(filepath):
                                    shutil.rmtree(filepath)
                        continue
                break

        try:
            if self.client and self.client.api and self.client.api.session:
                await self.client.api.session.close()
        except:
            pass

        if self.listener.is_cancelled:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            await self.listener.on_download_error("Stopped by user!")
            return

        if not success:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            await self.listener.on_download_error(f"Mega Download Failed: {error_msg}")
            return

        try:
            dest_final = os.path.join(path, self.listener.name)
            if is_folder:
                downloaded_items = os.listdir(temp_dir)
                if downloaded_items:
                    source_folder = os.path.join(temp_dir, downloaded_items[0])
                    if os.path.isdir(source_folder):
                        shutil.move(source_folder, dest_final)
                    else:
                        os.makedirs(dest_final, exist_ok=True)
                        shutil.move(source_folder, os.path.join(dest_final, downloaded_items[0]))
            else:
                downloaded_items = os.listdir(temp_dir)
                if downloaded_items:
                    shutil.move(os.path.join(temp_dir, downloaded_items[0]), dest_final)
            
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                
            await self.listener.on_download_complete()
        except Exception as e:
            LOGGER.error(f"Failed to move downloaded Mega files: {e}")
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            await self.listener.on_download_error(f"Post-processing error: {e}")

async def add_mega_download(listener, path):
    helper = MegaDownloadHelper(listener)
    asyncio.create_task(helper.download(path))
