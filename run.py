import argparse
import fnmatch
import hashlib
import logging
import math
import os
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from pathlib import Path

import requests
from pathvalidate import sanitize_filename
import shutil
from tqdm import tqdm


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s][%(funcName)20s()][%(levelname)-8s]: %(message)s",
    handlers=[
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("GoFile")


class File:
    def __init__(self, link: str, dest: str):
        self.link = link
        self.dest = Path(dest)

    def __str__(self):
        return f"{self.dest} ({self.link})"


class Downloader:
    def __init__(self, token):
        self.token = token
        self.progress_lock = Lock()
        self.progress_bar = None

    # send HEAD request to get the file size, and check if the site supports range
    def _get_total_size(self, link):
        r = requests.head(link, headers={"Cookie": f"accountToken={self.token}"})
        r.raise_for_status()
        return int(r.headers["Content-Length"]), r.headers.get("Accept-Ranges", "none") == "bytes"

    # download the range of the file
    def _download_range(self, link, start, end, temp_file, i):
        temp_file = Path(temp_file)
        existing_size = temp_file.stat().st_size if temp_file.exists() else 0
        range_start = start + existing_size
        if range_start > end:
            return i
        headers = {
            "Cookie": f"accountToken={self.token}",
            "Range": f"bytes={range_start}-{end}"
        }
        with requests.get(link, headers=headers, stream=True) as r:
            r.raise_for_status()
            with open(temp_file, "ab", encoding=None) as f:  # 바이너리 모드에서는 encoding 불필요
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        with self.progress_lock:
                            self.progress_bar.update(len(chunk))
        return i

    # merge temp files
    def _merge_temp_files(self, temp_dir, dest, num_threads):
        temp_dir = Path(temp_dir)
        dest = Path(dest)
        with open(dest, "wb") as outfile:
            for i in range(num_threads):
                temp_file = temp_dir / f"part_{i}"
                with open(temp_file, "rb") as f:
                    outfile.write(f.read())
                temp_file.unlink()
        shutil.rmtree(temp_dir)

    def download(self, file: File, num_threads=4):
        link = file.link
        dest = Path(file.dest)
        temp_dir = Path(str(dest) + "_parts")
        
        try:
            # get file size, and if the site supports range
            total_size, is_support_range = self._get_total_size(link)

            # skip download if the file has been fully downloaded
            if dest.exists() and dest.stat().st_size == total_size:
                return
            
            if num_threads == 1 or not is_support_range:
                temp_file = Path(str(dest) + ".part")

                # calculate downloaded bytes
                downloaded_bytes = temp_file.stat().st_size if temp_file.exists() else 0

                # start progress bar
                if len(dest.name) > 25:
                    display_name = dest.name[:10] + "....." + dest.name[-10:]
                else:
                    display_name = dest.name.rjust(25)
                self.progress_bar = tqdm(total=total_size, initial=downloaded_bytes, unit='B', unit_scale=True, desc=f'Downloading {display_name}')

                # download file
                headers = {
                    "Cookie": f"accountToken={self.token}",
                    "Range": f"bytes={downloaded_bytes}-"
                }
                dest.parent.mkdir(parents=True, exist_ok=True)
                with requests.get(link, headers=headers, stream=True) as r:
                    r.raise_for_status()
                    with open(temp_file, "ab") as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                self.progress_bar.update(len(chunk))

                # close progress bar
                self.progress_bar.close()

                # rename temp file
                temp_file.rename(dest)
            else:
                # remove single thread file if it exists
                single_part = Path(str(dest) + ".part")
                if single_part.exists():
                    single_part.unlink()

                # check if the num_threads is matched
                # remove the previous downloaded temp files if it doesn't match
                check_file = temp_dir / "num_threads"
                if temp_dir.exists():
                    prev_num_threads = None
                    if check_file.exists():
                        with open(check_file, 'r') as f:
                            prev_num_threads = int(f.read())
                    if prev_num_threads is None or prev_num_threads != num_threads:
                        shutil.rmtree(temp_dir)

                if not temp_dir.exists():
                    # create temp directory for temp files
                    temp_dir.mkdir(parents=True, exist_ok=True)

                    # add check_file
                    with open(check_file, "w") as f:
                        f.write(str(num_threads))

                # calculate the number of temp files
                part_size = math.ceil(total_size / num_threads)

                # calculate downloaded bytes
                downloaded_bytes = 0
                for i in range(num_threads):
                    part_file = temp_dir / f"part_{i}"
                    if part_file.exists():
                        downloaded_bytes += part_file.stat().st_size

                # start progress bar
                if len(dest.name) > 25:
                    display_name = dest.name[:10] + "....." + dest.name[-10:]
                else:
                    display_name = dest.name.rjust(25)
                self.progress_bar = tqdm(total=total_size, initial=downloaded_bytes, unit='B', unit_scale=True, desc=f'Downloading {display_name}')

                # download temp files
                futures = []
                with ThreadPoolExecutor(max_workers=num_threads) as executor:
                    for i in range(num_threads):
                        start = i * part_size
                        end = min(start + part_size - 1, total_size - 1)
                        temp_file = temp_dir / f"part_{i}"
                        futures.append(executor.submit(self._download_range, link, start, end, str(temp_file), i))
                    for future in as_completed(futures):
                        future.result()

                # close progress bar
                self.progress_bar.close()

                # merge temp files
                self._merge_temp_files(temp_dir, dest, num_threads)
        except Exception as e:
            if self.progress_bar:
                self.progress_bar.close()
            logger.error(f"failed to download ({e}): {dest} ({link})")


class GoFileMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class GoFile(metaclass=GoFileMeta):
    def __init__(self) -> None:
        self.token = ""
        self.wt = ""
        self.lock = Lock()

    def update_token(self) -> None:
        if self.token == "":
            data = requests.post("https://api.gofile.io/accounts").json()
            if data["status"] == "ok":
                self.token = data["data"]["token"]
                logger.info(f"updated token: {self.token}")
            else:
                raise Exception("cannot get token")

    def update_wt(self) -> None:
        if self.wt == "":
            alljs = requests.get("https://gofile.io/dist/js/config.js").text
            if 'appdata.wt = "' in alljs:
                self.wt = alljs.split('appdata.wt = "')[1].split('"')[0]
                logger.info(f"updated wt: {self.wt}")
            else:
                raise Exception("cannot get wt")

    def execute(
        self, 
        dir: str, 
        content_id: str = None, 
        url: str = None, 
        password: str = None, 
        proxy: str = None, 
        num_threads: int = 1, 
        includes: list[str] = None, 
        excludes: list[str] = None) -> None:
        if proxy is not None:
            logger.info(f"Proxy set to: {proxy}")
            os.environ['HTTP_PROXY'] = proxy
            os.environ['HTTPS_PROXY'] = proxy
        else:
            os.environ.pop('HTTP_PROXY', None)
            os.environ.pop('HTTPS_PROXY', None)

        files = self.get_files(dir, content_id, url, password, includes, excludes)
        for file in files:
            Downloader(token=self.token).download(file, num_threads=num_threads)

    def is_included(self, filename: str, includes: list[str]) -> bool:
        if len(includes) == 0:
            return True
        return any(fnmatch.fnmatch(filename, pattern) for pattern in includes)
    
    def is_excluded(self, filename: str, excludes: list[str]) -> bool:
        if len(excludes) == 0:
            return False
        return any(fnmatch.fnmatch(filename, pattern) for pattern in excludes)

    def get_files(
            self, dir: str, 
            content_id: str = None, 
            url: str = None, 
            password: str = None, 
            includes: list[str] = None,
            excludes: list[str] = None) -> list[File]:
        if includes is None:
            includes = []
        if excludes is None:
            excludes = []
        files = list()
        dir = Path(dir)
        
        if content_id is not None:
            self.update_token()
            self.update_wt()
            hash_password = hashlib.sha256(password.encode()).hexdigest() if password != None else ""
            data = requests.get(
                f"https://api.gofile.io/contents/{content_id}?cache=true&password={hash_password}",
                headers={
                    "Authorization": "Bearer " + self.token,
                    "X-Website-Token": self.wt,
                },
            ).json()
            if data["status"] == "ok":
                if data["data"].get("passwordStatus", "passwordOk") == "passwordOk":
                    if data["data"]["type"] == "folder":
                        dirname = data["data"]["name"]
                        dir = dir / sanitize_filename(dirname)  # os.path.join 대신 / 연산자
                        for (id, child) in data["data"]["children"].items():
                            if child["type"] == "folder":
                                folder_files = self.get_files(dir=str(dir), content_id=id, password=password, includes=includes, excludes=excludes)
                                files.extend(folder_files)
                            else:
                                filename = child["name"]
                                if self.is_included(filename, includes) and not self.is_excluded(filename, excludes):
                                    files.append(File(
                                        link=child["link"],
                                        dest=str(dir / sanitize_filename(filename))))
                    else:
                        filename = data["data"]["name"]
                        if self.is_included(filename, includes) and not self.is_excluded(filename, excludes):
                            files.append(File(
                                link=data["data"]["link"],
                                dest=str(dir / sanitize_filename(filename))))
                else:
                    logger.error(f"invalid password: {data['data'].get('passwordStatus')}")
        elif url is not None:
            if url.startswith("https://gofile.io/d/"):
                files = self.get_files(dir=str(dir), content_id=url.split("/")[-1], password=password, includes=includes, excludes=excludes)
            else:
                logger.error(f"invalid url: {url}")
        else:
            logger.error(f"invalid parameters")
        return files

def load_config(config_path):
    """YAML 설정 파일을 로드합니다."""
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                logger.info(f"Loaded configuration from {config_path}")
                return config if config else {}
        except Exception as e:
            logger.error(f"Failed to load config file: {e}")
            return {}
    else:
        logger.warning(f"Config file not found: {config_path}. Using arguments/defaults.")
        return {}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GoFile Downloader with YAML config support")
    
    # 설정 파일 인수 추가
    parser.add_argument("-c", "--config", default="config.yaml", help="path to YAML config file (default: config.yaml)")

    # 기존 인수들을 유지하되, required=False 등으로 유연하게 변경
    # argparse 인수들은 YAML 파일의 내용을 '덮어쓰는(override)' 용도로 사용됩니다.
    parser.add_argument("url", nargs='?', default=None, help="url to process")
    parser.add_argument("-f", type=str, dest="file_input", help="local file to process (list of URLs)")
    parser.add_argument("-t", type=int, dest="num_threads", help="number of threads")
    parser.add_argument("-d", type=str, dest="output_dir", help="output directory")
    parser.add_argument("-p", type=str, dest="password", help="password")
    parser.add_argument("-x", type=str, dest="proxy", help="proxy server (format: ip/host:port)")
    parser.add_argument("-i", action="append", dest="includes", help="included files (supporting wildcard *)")
    parser.add_argument("-e", action="append", dest="excludes", help="excluded files (supporting wildcard *)")

    args = parser.parse_args()

    # 1. YAML 로드
    config = load_config(args.config)

    # 2. 설정 병합: CLI 인수가 있으면 YAML 설정을 덮어씁니다.
    final_url = args.url if args.url else config.get("url")
    final_file_input = args.file_input if args.file_input else config.get("file_input")
    final_num_threads = args.num_threads if args.num_threads else config.get("num_threads", 1)
    final_dir = args.output_dir if args.output_dir else config.get("output_dir", "./output")
    final_password = args.password if args.password else config.get("password")
    final_proxy = args.proxy if args.proxy else config.get("proxy")
    
    # 리스트 병합 로직 (CLI가 있으면 CLI 사용, 없으면 YAML 사용)
    final_includes = args.includes if args.includes else config.get("includes", [])
    final_excludes = args.excludes if args.excludes else config.get("excludes", [])

    # 3. 실행 로직
    # file_input이 있으면 목록 파일 처리
    if final_file_input:
        if os.path.exists(final_file_input):
            with open(final_file_input, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line == "" or line.startswith("#"):
                        continue
                    GoFile().execute(
                        dir=final_dir, 
                        url=line, 
                        password=final_password, 
                        proxy=final_proxy, 
                        num_threads=final_num_threads, 
                        includes=final_includes, 
                        excludes=final_excludes)
        else:
            logger.error(f"file not found: {final_file_input}")
    
    # url이 있으면 단일 처리
    elif final_url:
        GoFile().execute(
            dir=final_dir, 
            url=final_url, 
            password=final_password, 
            proxy=final_proxy, 
            num_threads=final_num_threads, 
            includes=final_includes, 
            excludes=final_excludes)
    
    else:
        logger.error("No URL or input file provided in arguments or config file.")
        parser.print_help()
