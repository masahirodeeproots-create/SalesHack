"""
エン カイシャの評判 → rawdata_en_hyouban 同期モジュール
=========================================================
scraping_en_hyouban/data/results.csv を読み込み、
rawdata_en_hyouban テーブルに生データを INSERT する。

実行例:
    python -m collectors.en_hyouban.sync
    python -m collectors.en_hyouban.sync --csv /path/to/results.csv
"""

import argparse
import csv
import logging
import re
import sys
import unicodedata
from datetime import datetime
from pathlib import Path

# ── プロジェクトルートをパスに追加 ──
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import LOG_DIR, OUTPUT_DIR

# ── ログ設定 ──
LOG_DIR.mkdir(parents=True, exist_ok=True)
log_file = LOG_DIR / f"en_hyouban_sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(str(log_file), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# scraping_en_hyouban のデフォルトパス
DEFAULT_RESULTS_CSV = PROJECT_ROOT.parent / "scraping_en_hyouban" / "data" / "results.csv"

_COMPANY_SUFFIXES = re.compile(
    r"(株式会社|有限会社|合同会社|合資会社|一般社団法人|公益社団法人|一般財団法人|公益財団法人|（株）|\(株\))"
)


def _normalize_for_match(name: str) -> str:
    """企業名を比較用に正規化する（全角→半角、株式会社等除去、小文字化）"""
    name = unicodedata.normalize("NFKC", name)
    name = _COMPANY_SUFFIXES.sub("", name)
    return name.strip().lower()


def load_results(csv_path: Path) -> list[dict]:
    if not csv_path.exists():
        logger.error(f"CSVが見つかりません: {csv_path}")
        return []
    rows = []
    with open(csv_path, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            rows.append(row)
    logger.info(f"results.csv 読み込み: {len(rows)}件")
    return rows


def sync_to_db(rows: list[dict]) -> int:
    """rawdata_en_hyouban テーブルに書き込む。Returns: 書き込み行数"""
    try:
        from db.connection import get_session
        from db.models import Company, RawdataEnHyouban
    except ImportError as e:
        logger.warning(f"DB接続不可（{e}）。スキップします。")
        return 0

    written = 0
    skipped = 0

    try:
        with get_session() as session:
            # 正規化済み企業名 → Company の高速マッチング用キャッシュ
            all_companies = session.query(Company).all()
            normalized_company_map: dict[str, Company] = {}
            for c in all_companies:
                key = _normalize_for_match(c.name)
                normalized_company_map[key] = c

            for row in rows:
                company_name = row.get("company_name", "").strip()
                if not company_name:
                    continue

                # 1. 完全一致（name_normalized）
                company = session.query(Company).filter(
                    Company.name_normalized == company_name
                ).first()
                # 2. name 部分一致
                if not company:
                    company = session.query(Company).filter(
                        Company.name.contains(company_name)
                    ).first()
                # 3. 正規化後に一致
                if not company:
                    norm_key = _normalize_for_match(company_name)
                    company = normalized_company_map.get(norm_key)
                if not company:
                    logger.debug(f"企業が見つかりません（スキップ）: {company_name}")
                    skipped += 1
                    continue

                def _val(key: str) -> str | None:
                    v = row.get(key, "").strip()
                    return v if v and v != "None" else None

                session.add(RawdataEnHyouban(
                    original_id=str(company.id),
                    source_url=_val("url"),
                    company_name=company_name,
                    total_score=_val("total_score"),
                    review_count=_val("review_count"),
                    founded_year=_val("founded_year"),
                    employees=_val("employees"),
                    capital=_val("capital"),
                    listed_year=_val("listed_year"),
                    avg_salary=_val("avg_salary"),
                    avg_age=_val("avg_age"),
                    score_growth=_val("score_growth"),
                    score_advantage=_val("score_advantage"),
                    score_meritocracy=_val("score_meritocracy"),
                    score_culture=_val("score_culture"),
                    score_youth=_val("score_youth"),
                    score_contribution=_val("score_contribution"),
                    score_innovation=_val("score_innovation"),
                    score_leadership=_val("score_leadership"),
                    reviews_text=_val("reviews_text"),
                ))
                written += 1

        logger.info(f"rawdata書き込み完了: {written}件 / {skipped}社スキップ（企業未登録）")

    except Exception as e:
        logger.error(f"DB書き込みエラー: {e}", exc_info=True)

    return written


def export_csv(rows: list[dict], out_path: Path):
    """DBなしでもCSVとして出力"""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"CSV出力: {out_path} ({len(rows)}件)")


def run(csv_path: Path = DEFAULT_RESULTS_CSV):
    logger.info("=" * 60)
    logger.info("エン カイシャの評判 同期開始")
    logger.info(f"入力CSV: {csv_path}")
    logger.info("=" * 60)

    start = datetime.now()
    rows = load_results(csv_path)

    if not rows:
        logger.warning("同期対象データなし。終了します。")
        return

    written = sync_to_db(rows)

    # CSVにも出力（data/output/en_hyouban_results.csv）
    out_csv = OUTPUT_DIR / "en_hyouban_results.csv"
    export_csv(rows, out_csv)

    elapsed = (datetime.now() - start).total_seconds()
    logger.info(f"同期完了: {written}件書き込み | 所要時間: {elapsed:.1f}秒")
    logger.info(f"ログ: {log_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="エン評判データを rawdata_en_hyouban に同期")
    parser.add_argument("--csv", type=Path, default=DEFAULT_RESULTS_CSV,
                        help="results.csvのパス")
    args = parser.parse_args()
    run(args.csv)
