import os
import sys
import csv
import time
import requests
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

# 対象企業名リスト（30社 - テスト用）
COMPANIES = [
    "株式会社テレコメディア",
    "株式会社ジェイ・エム・エス",
    "株式会社トーカイ",
    "株式会社トーコー",
    "株式会社トモ",
    "株式会社トヨタエンタプライズ",
    "株式会社トライトエンジニアリング",
    "株式会社トライトキャリア",
    "株式会社日本管財環境サービス",
    "株式会社アントワークス",
    "株式会社カネスエ",
    "株式会社ニコン日総プライム",
    "株式会社にしけい",
    "株式会社ニチイ学館",
    "株式会社ニチダン",
    "株式会社ニッコー",
    "株式会社ニフス",
    "株式会社ネオキャリア",
    "株式会社ネクスコ・サポート北海道",
    "株式会社ネクスコ東日本エンジニアリング",
    "株式会社ネクスコ東日本リテイル",
    "株式会社ノア・ビルサービス",
    "株式会社ノバレーゼ",
    "株式会社ノムラクリーニング",
    "株式会社今仙電機製作所",
    "株式会社パソナ",
    "株式会社パソナＨＳ",
    "株式会社パソナ日本総務部",
    "エルケア株式会社",
    "株式会社カルラ",
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
REQUEST_INTERVAL = 3.0  # API制限対策（秒）
MAX_RETRIES = 3         # ScrapingDog 502エラー時の最大リトライ回数
RETRY_BACKOFF_BASE = 2  # リトライ待機時間の底（秒）: 2^n 秒ずつ増加


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


def main():
    print("=" * 60)
    print("企業媒体URL収集スクリプト")
    print(f"対象企業数: {len(COMPANIES)}社 / 対象媒体数: {len(MEDIA_CONFIG)}媒体")
    print(f"合計検索数: {len(COMPANIES) * len(MEDIA_CONFIG)}件")
    print("=" * 60)

    results = []

    for company in COMPANIES:
        print(f"\n▶ {company}")

        for media_name, config in MEDIA_CONFIG.items():
            query = f"{company} {config['query_suffix']}"
            print(f"  [{media_name}] 検索: {query}")

            urls = search_google(query)
            matched_url = find_matching_url(urls, config["url_prefixes"])

            if matched_url:
                status = "found"
                print(f"    ✓ {matched_url}")
            else:
                status = "not_found"
                print(f"    ✗ not found")

            results.append({
                "企業名": company,
                "媒体名": media_name,
                "URL": matched_url,
                "status": status,
            })

            time.sleep(REQUEST_INTERVAL)

    # CSV出力（BOM付きUTF-8でExcelでも開ける）
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
