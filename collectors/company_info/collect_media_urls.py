import os
import sys
import csv
import threading
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from dotenv import load_dotenv

# プロジェクトルートを sys.path に追加
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config.settings import OUTPUT_DIR

load_dotenv()

SCRAPINGDOG_API_KEY = os.getenv("SCRAPINGDOG_API_KEY")
SCRAPINGDOG_ENDPOINT = "https://api.scrapingdog.com/google"

# 対象企業名リスト（100社）
COMPANIES = [
    "株式会社Ｏｎｅ",
    "株式会社アクシーズ",
    "株式会社ホーブ",
    "株式会社ホクリヨウ",
    "株式会社マース",
    "株式会社秋川牧園",
    "Ｋ＆Ｏエナジーグループ株式会社",
    "株式会社ＩＮＰＥＸ",
    "石油資源開発株式会社",
    "日鉄鉱業株式会社",
    "ＮＥＸＴ ＳＴＡＧＥ株式会社",
    "ａｒｔｉｅｎｃｅ株式会社",
    "株式会社高松コンストラクショングループ",
    "インフロニア・ホールディングス株式会社",
    "エクシオグループ株式会社",
    "オプティマス株式会社",
    "ＡＳＴＩ株式会社",
    "ＢＩＰＲＯＧＹ株式会社",
    "ＣＫＤ株式会社",
    "サンユー建設株式会社",
    "ベース株式会社",
    "ＤＡＩＫＯ ＸＴＥＣＨ株式会社",
    "シンクレイヤ株式会社",
    "ＤＩＣ株式会社",
    "ＤＭ三井製糖株式会社",
    "ＤＯＷＡホールディングス株式会社",
    "ＥＩＺＯ株式会社",
    "ＥＮＥＯＳ株式会社",
    "株式会社ソフトクリエイト",
    "ダイダン株式会社",
    "タマホーム株式会社",
    "テスホールディングス株式会社",
    "ＦＤＫ株式会社",
    "ＧＭＢ株式会社",
    "ピーエス・コンストラクション株式会社",
    "株式会社東名",
    "フクヤ建設株式会社",
    "ＧＭＯペパボ株式会社",
    "ベクトル株式会社",
    "株式会社キューブシステム",
    "ＨＯＹＡ株式会社",
    "メタウォーター株式会社",
    "ＡＩ ＣＲＯＳＳ株式会社",
    "株式会社ＲＯＢＯＴ ＰＡＹＭＥＮＴ",
    "ライト工業株式会社",
    "株式会社ニーズウェル",
    "レイズネクスト株式会社",
    "ＪＢＣＣホールディングス株式会社",
    "ＪＣＲファーマ株式会社",
    "株式会社ジェイテック",
    "株式会社ＣＳＣ",
    "株式会社ＥＴＳ",
    "ＪＦＥシステムズ株式会社",
    "株式会社ＩＰＳ",
    "株式会社ＲＩＳＥ",
    "株式会社ＴＢＳ",
    "株式会社アバント",
    "株式会社アークス",
    "株式会社アートフォースジャパン",
    "株式会社アイダ設計",
    "株式会社アクアライン",
    "株式会社スタメン",
    "株式会社イシン",
    "株式会社ウィル",
    "株式会社ウチヤマ",
    "株式会社エージェント",
    "ＡＧＳ株式会社",
    "ＡＲアドバンストテクノロジ株式会社",
    "ＪＴＰ株式会社",
    "ＪＵＫＩ株式会社",
    "株式会社キムラ",
    "株式会社きんでん",
    "株式会社ナカノフドー建設",
    "ＪＸ金属株式会社",
    "株式会社クラフティア",
    "株式会社グリーンエナジー＆カンパニー",
    "ＫＤＤＩ株式会社",
    "ＫＨネオケム株式会社",
    "株式会社イチケン",
    "ＫＬＡＳＳ株式会社",
    "ＫＯＡ株式会社",
    "ＬＩＮＥヤフー株式会社",
    "株式会社シンカ",
    "株式会社クロップス",
    "株式会社セイノー",
    "株式会社セレコーポレーション",
    "株式会社タスキ",
    "ＮＣＤ株式会社",
    "株式会社トーエネック",
    "株式会社トミタ",
    "株式会社オロ",
    "株式会社ニューテック",
    "株式会社ノバック",
    "ＮＩＳＳＨＡ株式会社",
    "ＮＩＴＴＯＫＵ株式会社",
    "株式会社ファーストステージ",
    "日本電技株式会社",
    "ＮＯＫ株式会社",
    "株式会社クロス・マーケティング",
    "株式会社みらい",
]

# 各媒体の設定
MEDIA_CONFIG = {
    "マイナビ": {
        "query_suffix": "マイナビ 新卒採用",
        "url_prefixes": [
            "https://job.mynavi.jp/26/pc/search/",
            "https://job.mynavi.jp/27/pc/search/",
        ],
    },
    "リクナビ": {
        "query_suffix": "リクナビ 新卒採用",
        "url_prefixes": [
            "https://job.rikunabi.com/2026/company",
            "https://job.rikunabi.com/2027/company",
        ],
    },
    "キャリタス": {
        "query_suffix": "キャリタス就活 採用",
        "url_prefixes": [
            "https://job.career-tasu.jp/corp/",
        ],
    },
    "リクルートエージェント": {
        "query_suffix": "リクルートエージェント 企業情報",
        "url_prefixes": [
            "https://www.r-agent.com/kensaku/companydetail/",
        ],
    },
    "PR TIMES": {
        "query_suffix": "PR TIMES プレスリリース",
        "url_prefixes": [
            "https://prtimes.jp/main/html/indexcorp/",
            "https://prtimes.jp/main/html/searchrlp/",
        ],
    },
}

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_CSV = str(OUTPUT_DIR / "company_media_urls.csv")
REQUEST_INTERVAL = 1.0  # 各スレッド完了後のインターバル（秒）
MAX_RETRIES = 3         # ScrapingDog 502エラー時の最大リトライ回数
RETRY_BACKOFF_BASE = 2  # リトライ待機時間の底（秒）: 2^n 秒ずつ増加
MAX_WORKERS = 5         # 最大並列検索数
_request_semaphore = threading.Semaphore(MAX_WORKERS)  # 同時リクエスト数を制御


def search_google(query: str) -> list:
    """ScrapingDog APIでGoogle検索し、URLリストを返す（502エラー時は指数バックオフでリトライ）"""
    if not SCRAPINGDOG_API_KEY:
        raise ValueError("SCRAPINGDOG_API_KEY が .env に設定されていません")

    params = {
        "api_key": SCRAPINGDOG_API_KEY,
        "query": query,
        "results": 10,
        "country": "jp",
    }
    for attempt in range(MAX_RETRIES + 1):
        try:
            response = requests.get(SCRAPINGDOG_ENDPOINT, params=params, timeout=30)
            if response.status_code == 502 and attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF_BASE ** attempt
                print(f"    [RETRY {attempt + 1}/{MAX_RETRIES}] ScrapingDog 502 - {wait}秒後にリトライ")
                time.sleep(wait)
                continue
            response.raise_for_status()
            data = response.json()

            urls = []
            for result in data.get("organic_results", []):
                url = result.get("link", "")
                if url:
                    urls.append(url)
            return urls

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 502 and attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF_BASE ** attempt
                print(f"    [RETRY {attempt + 1}/{MAX_RETRIES}] ScrapingDog 502 - {wait}秒後にリトライ")
                time.sleep(wait)
                continue
            print(f"    [ERROR] HTTP {e.response.status_code}: {e}")
            return []
        except requests.exceptions.RequestException as e:
            print(f"    [ERROR] リクエスト失敗: {e}")
            return []
        except Exception as e:
            print(f"    [ERROR] 予期しないエラー: {e}")
            return []
    return []


def find_matching_url(urls: list, url_prefixes: list) -> str:
    """URLリストから指定プレフィックスに前方一致する最初のURLを返す"""
    for url in urls:
        for prefix in url_prefixes:
            if url.startswith(prefix):
                return url
    return ""


def _search_task(company: str, media_name: str, config: dict) -> dict:
    """1件の検索タスク（セマフォで同時実行数を制御）"""
    query = f"{company} {config['query_suffix']}"
    with _request_semaphore:
        urls = search_google(query)
        time.sleep(REQUEST_INTERVAL)  # セマフォ内で待機（レート制限）

    matched_url = find_matching_url(urls, config["url_prefixes"])
    status = "found" if matched_url else "not_found"
    marker = "✓" if matched_url else "✗"
    print(f"  [{company}][{media_name}] {marker} {matched_url or 'not found'}")
    return {
        "企業名": company,
        "媒体名": media_name,
        "URL": matched_url,
        "status": status,
    }


def main():
    print("=" * 60)
    print("企業媒体URL収集スクリプト（5並列）")
    print(f"対象企業数: {len(COMPANIES)}社 / 対象媒体数: {len(MEDIA_CONFIG)}媒体")
    print(f"合計検索数: {len(COMPANIES) * len(MEDIA_CONFIG)}件")
    print("=" * 60)

    # 全タスクを作成
    tasks = [
        (company, media_name, config)
        for company in COMPANIES
        for media_name, config in MEDIA_CONFIG.items()
    ]

    results_map: dict[tuple, dict] = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_key = {
            executor.submit(_search_task, company, media_name, config): (company, media_name)
            for company, media_name, config in tasks
        }
        for future in as_completed(future_to_key):
            key = future_to_key[future]
            try:
                results_map[key] = future.result()
            except Exception as e:
                company, media_name = key
                print(f"  [{company}][{media_name}] エラー: {e}")
                results_map[key] = {
                    "企業名": company,
                    "媒体名": media_name,
                    "URL": "",
                    "status": "error",
                }

    # 元の順序に並べ替えて CSV 出力
    results = [results_map[(c, m)] for c, m, _ in tasks if (c, m) in results_map]

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        fieldnames = ["企業名", "媒体名", "URL", "status"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    # サマリー
    found_count = sum(1 for r in results if r["status"] == "found")
    total_count = len(results)
    print("\n" + "=" * 60)
    print(f"完了！結果を {OUTPUT_CSV} に保存しました。")
    print(f"取得成功: {found_count} / {total_count} 件")
    print("=" * 60)


if __name__ == "__main__":
    main()
