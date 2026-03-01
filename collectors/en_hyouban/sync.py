"""
エン カイシャの評判 → 企業情報収集DB 同期モジュール
=====================================================
scraping_en_hyouban/data/results.csv を読み込み、
main DBの company_field_values に口コミ評価データを書き込む。

実行例:
    python -m collectors.en_hyouban.sync
    python -m collectors.en_hyouban.sync --csv /path/to/results.csv
"""

import csv
import logging
import sys
import argparse
from pathlib import Path
from datetime import datetime, timezone

# ── プロジェクトルートをパスに追加 ──
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import LOG_DIR, OUTPUT_DIR

# ── ログ設定（本体プロジェクトの data/logs/ に統一）──
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

# エン評判フィールドのマッピング（results.csv列名 → master_fields canonical名）
FIELD_MAP = {
    "total_score":       "エン評判_総合スコア",
    "review_count":      "エン評判_口コミ件数",
    "score_growth":      "エン評判_成長性",
    "score_advantage":   "エン評判_優位性",
    "score_meritocracy": "エン評判_実力主義",
    "score_culture":     "エン評判_風土",
    "score_youth":       "エン評判_20代成長環境",
    "score_contribution":"エン評判_社会貢献",
    "score_innovation":  "エン評判_イノベーション",
    "score_leadership":  "エン評判_経営陣",
    "reviews_text":      "エン評判_口コミ本文",
}

SOURCE_NAME = "エン カイシャの評判"


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
    """DB（SQLAlchemy）に書き込む"""
    try:
        from db.connection import get_session
        from db.models import (
            Company, FieldDefinition, CompanyFieldValue,
            CompanyFieldValueHistory
        )
    except ImportError as e:
        logger.warning(f"DB接続不可（{e}）。CSV出力のみ実行します。")
        return 0

    written = 0
    skipped = 0

    try:
        with get_session() as session:
            # field_definitions をキャッシュ
            field_defs = {
                fd.canonical_name: fd
                for fd in session.query(FieldDefinition).all()
            }

            for row in rows:
                company_name = row.get("company_name", "").strip()
                if not company_name:
                    continue

                # 企業を name_normalized で検索
                company = session.query(Company).filter(
                    Company.name_normalized == company_name
                ).first()
                if not company:
                    # name_raw でも検索
                    company = session.query(Company).filter(
                        Company.name_raw.contains(company_name)
                    ).first()
                if not company:
                    logger.debug(f"企業が見つかりません（スキップ）: {company_name}")
                    skipped += 1
                    continue

                for csv_col, canonical in FIELD_MAP.items():
                    value = row.get(csv_col, "").strip()
                    if not value or value == "None":
                        continue

                    field_def = field_defs.get(canonical)
                    if not field_def:
                        logger.warning(f"field_definitionが未登録: {canonical}")
                        continue

                    # company_field_values に UPSERT
                    existing = session.query(CompanyFieldValue).filter_by(
                        company_id=company.id,
                        field_id=field_def.id,
                    ).first()

                    now = datetime.now(timezone.utc)
                    if existing:
                        existing.value = value
                        existing.source = SOURCE_NAME
                        existing.scraped_at = now
                    else:
                        session.add(CompanyFieldValue(
                            company_id=company.id,
                            field_id=field_def.id,
                            value=value,
                            source=SOURCE_NAME,
                        ))

                    # 履歴にも追記
                    session.add(CompanyFieldValueHistory(
                        company_id=company.id,
                        field_id=field_def.id,
                        value=value,
                        source=SOURCE_NAME,
                    ))
                    written += 1

        logger.info(f"DB書き込み完了: {written}件書き込み / {skipped}社スキップ（企業未登録）")

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

    # DBへ同期
    written = sync_to_db(rows)

    # 念のためCSVにも出力（data/output/en_hyouban_results.csv）
    out_csv = OUTPUT_DIR / "en_hyouban_results.csv"
    export_csv(rows, out_csv)

    elapsed = (datetime.now() - start).total_seconds()
    logger.info(f"同期完了: {written}フィールド書き込み | 所要時間: {elapsed:.1f}秒")
    logger.info(f"ログ: {log_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="エン評判データをメインDBに同期")
    parser.add_argument("--csv", type=Path, default=DEFAULT_RESULTS_CSV,
                        help="results.csvのパス")
    args = parser.parse_args()
    run(args.csv)
