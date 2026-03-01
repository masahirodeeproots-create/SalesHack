"""全スクレイパー実行 → マスターDB構築のオーケストレーター"""

import sys
import logging
import argparse
from pathlib import Path

from config import SERVICE_REGISTRY, LOG_DIR

# ログ設定
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "scraper.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# スクレイパーモジュールのインポートマップ
SCRAPER_MAP = {
    "labbase": "scrapers.labbase",
    "talentbook": "scrapers.talentbook",
    "type_shinsotsu": "scrapers.type_shinsotsu",
    "onecareer": "scrapers.onecareer",
    "levtech_rookie": "scrapers.levtech_rookie",
    "bizreach_campus": "scrapers.bizreach_campus",
    "offerbox": "scrapers.offerbox",
    "en_tenshoku": "scrapers.en_tenshoku",
    "kimisuka": "scrapers.kimisuka",
    "caritasu": "scrapers.caritasu",
    "career_ticket": "scrapers.career_ticket",
    "bizreach": "scrapers.bizreach",
    "en_ambi": "scrapers.en_ambi",
    "type_chuto": "scrapers.type_chuto",
}


def import_scraper(key: str):
    """スクレイパーモジュールを動的にインポートしてScraperクラスを返す"""
    module_name = SCRAPER_MAP[key]
    try:
        module = __import__(module_name, fromlist=["Scraper"])
        return module.Scraper
    except (ImportError, AttributeError) as e:
        logger.warning(f"{key}: スクレイパー未実装 ({e})")
        return None


def run_scrapers(targets: list[str] | None = None):
    """指定されたスクレイパーを順次実行する"""
    keys = targets or list(SCRAPER_MAP.keys())
    results = {"success": [], "failed": [], "skipped": []}

    for key in keys:
        if key not in SCRAPER_MAP:
            logger.warning(f"不明なサービス: {key}")
            results["skipped"].append(key)
            continue

        config = SERVICE_REGISTRY.get(key)
        if not config:
            results["skipped"].append(key)
            continue

        ScraperClass = import_scraper(key)
        if ScraperClass is None:
            results["skipped"].append(key)
            continue

        logger.info(f"\n{'=' * 50}")
        logger.info(f"▶ {config['name']}（{key}）")
        logger.info(f"{'=' * 50}")

        try:
            scraper = ScraperClass()
            scraper.run()
            results["success"].append(key)
        except Exception as e:
            logger.error(f"{key} 実行エラー: {e}", exc_info=True)
            results["failed"].append(key)

    return results


def main():
    parser = argparse.ArgumentParser(description="HRサービス使用状況調査スクレイパー")
    parser.add_argument(
        "services",
        nargs="*",
        help="実行するサービスキー（省略時は全サービス）。例: labbase career_ticket",
    )
    parser.add_argument(
        "--build-master",
        action="store_true",
        help="スクレイピング後にマスターDBを構築する",
    )
    parser.add_argument(
        "--master-only",
        action="store_true",
        help="スクレイピングをスキップしてマスターDBのみ構築する",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="利用可能なサービス一覧を表示する",
    )
    args = parser.parse_args()

    if args.list:
        print("\n利用可能なサービス:")
        for key, config in SERVICE_REGISTRY.items():
            status = "✓ 実装済み" if import_scraper(key) else "✗ 未実装"
            print(f"  {key:20s} {config['name']:20s} [{config['category']}] {status}")
        return

    if not args.master_only:
        targets = args.services if args.services else None
        results = run_scrapers(targets)

        # サマリー表示
        print("\n" + "=" * 50)
        print("スクレイピング結果サマリー")
        print(f"  成功: {len(results['success'])}件 {results['success']}")
        print(f"  失敗: {len(results['failed'])}件 {results['failed']}")
        print(f"  スキップ: {len(results['skipped'])}件 {results['skipped']}")
        print("=" * 50)

    if args.build_master or args.master_only:
        from build_master import build_master
        build_master()


if __name__ == "__main__":
    main()
