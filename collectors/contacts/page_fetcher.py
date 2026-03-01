"""
page_fetcher.py
===============
ScrapingDog Google検索でスニペットを取得し、Snippet オブジェクトのリストに変換する。

スニペットとは:
  Google検索結果の title + snippet テキストを連結したもの。
  各スニペットは source_snippet_id (検索結果内の連番) で識別される。
"""

import logging
import os
import time
import requests
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# 連絡先収集用 Google 検索クエリテンプレート
CONTACT_SEARCH_QUERIES = [
    "{company} 採用担当 電話番号",
    "{company} 人事部 採用窓口 連絡先",
    "{company} 採用 メールアドレス",
]

SCRAPINGDOG_ENDPOINT = "https://api.scrapingdog.com/google"
REQUEST_INTERVAL = 3.0  # ScrapingDog API 制限対策（秒）
MIN_SNIPPET_CHARS = 20  # これ未満のテキストは捨てる


@dataclass
class Snippet:
    snippet_id: int    # 検索結果内の連番 (source_snippet_id)
    text: str          # スニペットテキスト（title + snippet）
    source_url: str    # 検索結果の URL
    html_tag: str      # 固定値 "search_result"


def fetch_google_snippets(
    company_name: str,
    api_key: str | None = None,
    request_interval: float = REQUEST_INTERVAL,
) -> list[Snippet]:
    """
    ScrapingDog Google検索で企業の連絡先スニペットを収集する。

    Args:
        company_name: 検索対象の企業名
        api_key: ScrapingDog API キー（省略時は環境変数 SCRAPINGDOG_API_KEY を使用）
        request_interval: リクエスト間隔（秒）

    Returns:
        Snippet オブジェクトのリスト
    """
    key = api_key or os.getenv("SCRAPINGDOG_API_KEY")
    if not key:
        logger.error("SCRAPINGDOG_API_KEY が設定されていません")
        return []

    all_snippets: list[Snippet] = []
    snippet_id = 0

    for query_tmpl in CONTACT_SEARCH_QUERIES:
        query = query_tmpl.format(company=company_name)
        params = {
            "api_key": key,
            "query": query,
            "results": 10,
            "country": "jp",
        }
        try:
            resp = requests.get(SCRAPINGDOG_ENDPOINT, params=params, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                for r in data.get("organic_results", []):
                    title = r.get("title", "")
                    snippet = r.get("snippet", "")
                    url = r.get("link", "")
                    text = f"{title} {snippet}".strip()
                    if len(text) >= MIN_SNIPPET_CHARS:
                        all_snippets.append(Snippet(
                            snippet_id=snippet_id,
                            text=text,
                            source_url=url or "google_search",
                            html_tag="search_result",
                        ))
                        snippet_id += 1
            else:
                logger.warning(f"ScrapingDog {resp.status_code}: {query}")
        except Exception as e:
            logger.warning(f"Google検索エラー [{company_name}]: {e}")
        time.sleep(request_interval)

    logger.info(f"スニペット取得: {company_name} → {len(all_snippets)}件")
    return all_snippets
