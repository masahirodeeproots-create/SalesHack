"""HTTP統一レイヤー: requests / ScrapingDog / Playwright の3モード対応"""

import os
import time
import random
import logging
import requests
from dotenv import load_dotenv

from config import (
    MAX_RETRIES,
    RETRY_BACKOFF_BASE,
    REQUEST_INTERVAL,
    USER_AGENT,
    SCRAPINGDOG_SCRAPE_ENDPOINT,
)

load_dotenv()

logger = logging.getLogger(__name__)

SCRAPINGDOG_API_KEY = os.getenv("SCRAPINGDOG_API_KEY")


class HttpClient:
    """3種類のHTTP取得メソッドを統一インターフェースで提供"""

    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": USER_AGENT})

    def fetch_requests(self, url: str, timeout: int = 20) -> str | None:
        """直接HTTPリクエストでHTML取得（analyze_media_structure.py fetch_html準拠）"""
        for attempt in range(MAX_RETRIES + 1):
            try:
                response = self._session.get(url, timeout=timeout)
                if response.status_code in (502, 503, 429) and attempt < MAX_RETRIES:
                    wait = RETRY_BACKOFF_BASE ** attempt
                    logger.warning(
                        f"HTTP {response.status_code} - リトライ {attempt + 1}/{MAX_RETRIES} "
                        f"({wait}秒後): {url}"
                    )
                    time.sleep(wait)
                    continue
                response.raise_for_status()
                response.encoding = response.apparent_encoding
                return response.text
            except requests.exceptions.HTTPError as e:
                if e.response.status_code in (502, 503, 429) and attempt < MAX_RETRIES:
                    wait = RETRY_BACKOFF_BASE ** attempt
                    logger.warning(
                        f"HTTP {e.response.status_code} - リトライ {attempt + 1}/{MAX_RETRIES} "
                        f"({wait}秒後): {url}"
                    )
                    time.sleep(wait)
                    continue
                logger.error(f"HTTP {e.response.status_code} - {url}")
                return None
            except requests.exceptions.RequestException as e:
                logger.error(f"リクエスト失敗 - {url} - {e}")
                return None
        return None

    def fetch_scrapingdog(self, url: str, dynamic: bool = True) -> str | None:
        """ScrapingDog API経由でJSレンダリング済みHTML取得"""
        if not SCRAPINGDOG_API_KEY:
            logger.error("SCRAPINGDOG_API_KEY が設定されていません")
            return None

        params = {
            "api_key": SCRAPINGDOG_API_KEY,
            "url": url,
            "dynamic": str(dynamic).lower(),
        }
        for attempt in range(MAX_RETRIES + 1):
            try:
                response = requests.get(
                    SCRAPINGDOG_SCRAPE_ENDPOINT, params=params, timeout=60
                )
                if response.status_code == 502 and attempt < MAX_RETRIES:
                    wait = RETRY_BACKOFF_BASE ** attempt
                    logger.warning(
                        f"ScrapingDog 502 - リトライ {attempt + 1}/{MAX_RETRIES} "
                        f"({wait}秒後): {url}"
                    )
                    time.sleep(wait)
                    continue
                response.raise_for_status()
                response.encoding = response.apparent_encoding
                return response.text
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 502 and attempt < MAX_RETRIES:
                    wait = RETRY_BACKOFF_BASE ** attempt
                    logger.warning(
                        f"ScrapingDog 502 - リトライ {attempt + 1}/{MAX_RETRIES} "
                        f"({wait}秒後): {url}"
                    )
                    time.sleep(wait)
                    continue
                logger.error(f"ScrapingDog HTTP {e.response.status_code} - {url}")
                return None
            except requests.exceptions.RequestException as e:
                logger.error(f"ScrapingDog リクエスト失敗 - {url} - {e}")
                return None
        return None

    def sleep(self):
        """リクエスト間のランダム遅延（1.0〜1.5秒）"""
        time.sleep(random.uniform(1.0, REQUEST_INTERVAL))
