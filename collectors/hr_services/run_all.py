"""全スクレイパー実行 → rawdata_hr_* テーブルに書き込むオーケストレーター"""

import sys
import logging
import argparse
from pathlib import Path

# HR services ルートを sys.path に追加（from config import ... を解決するため）
_HR_ROOT = str(Path(__file__).resolve().parent)
if _HR_ROOT not in sys.path:
    sys.path.insert(0, _HR_ROOT)

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
    "labbase":         "scrapers.labbase",
    "talentbook":      "scrapers.talentbook",
    "type_shinsotsu":  "scrapers.type_shinsotsu",
    "onecareer":       "scrapers.onecareer",
    "levtech_rookie":  "scrapers.levtech_rookie",
    "bizreach_campus": "scrapers.bizreach_campus",
    "offerbox":        "scrapers.offerbox",
    "en_tenshoku":     "scrapers.en_tenshoku",
    "kimisuka":        "scrapers.kimisuka",
    "caritasu":        "scrapers.caritasu",
    "career_ticket":   "scrapers.career_ticket",
    "bizreach":        "scrapers.bizreach",
    "en_ambi":         "scrapers.en_ambi",
    "type_chuto":      "scrapers.type_chuto",
}

# サービスキー → rawdata モデルクラス名 のマッピング
_RAWDATA_MODEL_MAP = {
    "labbase":         "RawdataHrLabbase",
    "talentbook":      "RawdataHrTalentbook",
    "type_shinsotsu":  "RawdataHrTypeShinsotsu",
    "onecareer":       "RawdataHrOnecareer",
    "levtech_rookie":  "RawdataHrLevtechRookie",
    "bizreach_campus": "RawdataHrBizreachCampus",
    "offerbox":        "RawdataHrOfferbox",
    "en_tenshoku":     "RawdataHrEnTenshoku",
    "kimisuka":        "RawdataHrKimisuka",
    "caritasu":        "RawdataHrCaritasu",
    "career_ticket":   "RawdataHrCareerTicket",
    "bizreach":        "RawdataHrBizreach",
    "en_ambi":         "RawdataHrEnAmbi",
    "type_chuto":      "RawdataHrTypeChuto",
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


def _save_to_rawdata(key: str, rows: list[dict]) -> int:
    """
    スクレイパー結果を rawdata_hr_* テーブルに書き込む。
    original_id は null（中間1でのマッチング後に付与）。
    Returns: 書き込み件数
    """
    if not rows:
        return 0

    _project_root = str(Path(__file__).resolve().parent.parent.parent)
    if _project_root not in sys.path:
        sys.path.insert(0, _project_root)
    # ローカル config.py がキャッシュされている場合は除去
    sys.modules.pop("config", None)

    model_name = _RAWDATA_MODEL_MAP.get(key)
    if not model_name:
        logger.warning(f"{key}: rawdata モデルが未定義")
        return 0

    try:
        import db.models as _models
        from db.connection import get_session
        ModelClass = getattr(_models, model_name)
    except (ImportError, AttributeError) as e:
        logger.error(f"{key}: rawdata モデルのインポート失敗 ({e})")
        return 0

    written = 0
    try:
        with get_session() as session:
            for row in rows:
                record = ModelClass(
                    original_id=None,  # 中間1で付与
                    source_url=row.get("url") or row.get("source_url") or None,
                    企業名_掲載名=row.get("企業名") or None,
                    掲載日=row.get("掲載日") or None,
                )
                session.add(record)
                written += 1
        logger.info(f"  {key}: rawdata {written}件書き込み完了")
    except Exception as e:
        logger.error(f"  {key}: rawdata書き込みエラー: {e}", exc_info=True)

    return written


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
            bq_rows = scraper.get_bq_rows()
            _save_to_rawdata(key, bq_rows)
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
