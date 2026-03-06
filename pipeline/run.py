"""
パイプライン実行オーケストレーター
==================================
DATABASE_DESIGN.md Section 8-3 のデータライフサイクルに準拠:

  [Step 1] rawdata を読み込んで中間1を pandas DataFrame として生成
  [Step 2] 中間1から中間2を pandas DataFrame として生成
  [Step 3] 中間2を BQ に WRITE_APPEND でアップロード
  [Step 4] 中間2を data/output/ に CSV 保存
  [Step 5] rawdata②〜⑬ を PostgreSQL から TRUNCATE（--truncate 指定時）
  [Step 6] 実行ログを BQ にアップロード

実行例:
  python -m pipeline.run                           # dry-run（BQ/TRUNCATE なし）
  python -m pipeline.run --bq-upload --csv-export  # BQ + CSV
  python -m pipeline.run --bq-upload --csv-export --truncate  # 全工程
"""

import argparse
import csv
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config.settings import LOG_DIR, OUTPUT_DIR

# ── ログ設定 ──
LOG_DIR.mkdir(parents=True, exist_ok=True)
_log_file = LOG_DIR / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(str(_log_file), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CSV出力（Section 5: data/output/ に指定ファイル名で保存）
# ---------------------------------------------------------------------------

# Section 5 のファイル名マッピング
_CSV_FILES = {
    "company_master": "company_master.csv",
    "company_info": "company_info.csv",
    "phones": "phones.csv",
    "persons": "persons.csv",
    "emails": "emails.csv",
    "phone_person_relation": "phone_person_relation.csv",
    "competitors": "competitors.csv",
    "call_logs": "call_logs.csv",
}


def _export_csvs(i2: dict, output_dir: Path) -> None:
    """中間データ2を CSV に出力する。"""
    output_dir.mkdir(parents=True, exist_ok=True)

    for key, filename in _CSV_FILES.items():
        df = i2.get(key)
        if df is None or df.empty:
            logger.info(f"  CSV スキップ（空）: {filename}")
            continue
        path = output_dir / filename
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info(f"  CSV 出力: {path} ({len(df)}行)")


# ---------------------------------------------------------------------------
# 実行ログ生成（Section 4-9）
# ---------------------------------------------------------------------------


def _build_fill_rate_log(run_id: str, i2: dict) -> list[dict]:
    """各テーブルの各カラムの充填率を計算する。"""
    import pandas as pd

    rows = []
    now = datetime.now(timezone.utc)

    for table_name, df in i2.items():
        if df is None or df.empty:
            continue
        total = len(df)
        for col in df.columns:
            if col in ("scraped_at", "original_id"):
                continue
            filled = int(df[col].notna().sum())
            # 空文字列も未充填として扱う
            if df[col].dtype == object:
                filled = int((df[col].notna() & (df[col] != "")).sum())
            rows.append({
                "run_id": run_id,
                "run_at": now,
                "table_name": table_name,
                "field_name": col,
                "filled_count": filled,
                "total_count": total,
                "fill_rate": round(filled / total, 4) if total > 0 else 0.0,
            })

    return rows


# ---------------------------------------------------------------------------
# rawdata TRUNCATE（Section 8-3 後処理）
# ---------------------------------------------------------------------------

# TRUNCATE 対象テーブル（rawdata②〜⑬。①登録データは消去しない）
_TRUNCATE_TABLES = [
    "rawdata_ra_company",
    "rawdata_ra_kyujin",
    "rawdata_mynavi",
    "rawdata_rikunabi_company",
    "rawdata_rikunabi_employ",
    "rawdata_caritasu",
    "rawdata_prtimes",
    "rawdata_minkabu",
    "rawdata_en_hyouban",
    "rawdata_phones",
    "rawdata_persons",
    "rawdata_emails",
    "rawdata_competitors",
    "rawdata_call_logs",
    "rawdata_hr_labbase",
    "rawdata_hr_talentbook",
    "rawdata_hr_type_shinsotsu",
    "rawdata_hr_onecareer",
    "rawdata_hr_levtech_rookie",
    "rawdata_hr_bizreach_campus",
    "rawdata_hr_offerbox",
    "rawdata_hr_en_tenshoku",
    "rawdata_hr_kimisuka",
    "rawdata_hr_caritasu",
    "rawdata_hr_career_ticket",
    "rawdata_hr_bizreach",
    "rawdata_hr_en_ambi",
    "rawdata_hr_type_chuto",
]


# ---------------------------------------------------------------------------
# メインパイプライン
# ---------------------------------------------------------------------------


def run(
    *,
    bq_upload: bool = False,
    csv_export: bool = False,
    truncate: bool = False,
    output_dir: Path | None = None,
) -> None:
    """パイプラインを実行する。"""
    import pandas as pd

    from db.connection import get_session
    from pipeline.intermediate1 import load_all as load_i1
    from pipeline.intermediate2 import build_all as build_i2

    run_id = str(uuid.uuid4())[:8]
    start = datetime.now()

    logger.info("=" * 60)
    logger.info(f"パイプライン開始 [run_id={run_id}]")
    logger.info(f"  BQ upload: {bq_upload}")
    logger.info(f"  CSV export: {csv_export}")
    logger.info(f"  TRUNCATE: {truncate}")
    logger.info("=" * 60)

    # ------------------------------------------------------------------
    # Step 1: rawdata → 中間データ1
    # ------------------------------------------------------------------
    with get_session() as session:
        i1 = load_i1(session)

    # ------------------------------------------------------------------
    # Step 2: 中間データ1 → 中間データ2
    # ------------------------------------------------------------------
    i2 = build_i2(i1)

    # ------------------------------------------------------------------
    # Step 3: BQ アップロード
    # ------------------------------------------------------------------
    if bq_upload:
        logger.info("=" * 50)
        logger.info("BQ アップロード開始")
        logger.info("=" * 50)

        from db.bigquery import (
            upload_call_logs,
            upload_company_info,
            upload_company_master,
            upload_competitors,
            upload_emails,
            upload_hr_services,
            upload_logs,
            upload_persons,
            upload_phone_person_relation,
            upload_phones,
        )

        # 企業マスター: WRITE_TRUNCATE
        upload_company_master(i2["company_master"])

        # その他: WRITE_APPEND
        upload_company_info(i2["company_info"])
        upload_phones(i2["phones"])
        upload_persons(i2["persons"])
        upload_emails(i2["emails"])
        upload_phone_person_relation(i2["phone_person_relation"])
        upload_competitors(i2["competitors"])
        upload_hr_services(i2["hr_services"])
        upload_call_logs(i2["call_logs"])

        # 実行ログ
        fill_rows = _build_fill_rate_log(run_id, i2)
        if fill_rows:
            upload_logs(pd.DataFrame(fill_rows))

        logger.info("BQ アップロード完了")

    # ------------------------------------------------------------------
    # Step 4: CSV 出力（Section 5: data/output/ に保存）
    # ------------------------------------------------------------------
    if csv_export:
        logger.info("=" * 50)
        logger.info("CSV エクスポート開始")
        logger.info("=" * 50)

        out = output_dir or OUTPUT_DIR
        _export_csvs(i2, out)

        # ログ情報も CSV に保存
        fill_rows = _build_fill_rate_log(run_id, i2)
        if fill_rows:
            logs_path = out / "logs.csv"
            pd.DataFrame(fill_rows).to_csv(logs_path, index=False, encoding="utf-8-sig")
            logger.info(f"  CSV 出力: {logs_path}")

        logger.info("CSV エクスポート完了")

    # ------------------------------------------------------------------
    # Step 5: rawdata TRUNCATE（BQアップロード確認後）
    # ------------------------------------------------------------------
    if truncate:
        if not bq_upload:
            logger.warning("--truncate は --bq-upload と併用してください。スキップします。")
        else:
            logger.info("=" * 50)
            logger.info("rawdata TRUNCATE 開始")
            logger.info("=" * 50)
            from sqlalchemy import text
            with get_session() as session:
                for table in _TRUNCATE_TABLES:
                    try:
                        session.execute(text(f"TRUNCATE TABLE {table}"))
                        logger.info(f"  TRUNCATE: {table}")
                    except Exception as e:
                        logger.warning(f"  TRUNCATE失敗: {table} - {e}")
                session.commit()
            logger.info("rawdata TRUNCATE 完了")

    # ------------------------------------------------------------------
    # 完了
    # ------------------------------------------------------------------
    elapsed = (datetime.now() - start).total_seconds()
    logger.info("=" * 60)
    logger.info(f"パイプライン完了 [run_id={run_id}] 所要時間: {elapsed:.1f}秒")
    logger.info(f"ログ: {_log_file}")
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="データパイプライン実行")
    parser.add_argument("--bq-upload", action="store_true",
                        help="BigQuery にアップロードする")
    parser.add_argument("--csv-export", action="store_true",
                        help="CSV にエクスポートする")
    parser.add_argument("--truncate", action="store_true",
                        help="BQアップロード確認後に rawdata②〜⑬ を TRUNCATE する")
    parser.add_argument("--output-dir", type=Path, default=None,
                        help="CSV 出力先（デフォルト: data/output/）")
    args = parser.parse_args()

    run(
        bq_upload=args.bq_upload,
        csv_export=args.csv_export,
        truncate=args.truncate,
        output_dir=args.output_dir,
    )


if __name__ == "__main__":
    main()
