"""
collectors/call_data/run.py
===========================
架電データCSVをDBにインポートするエントリポイント。

使い方:
  # CSVファイルを指定してインポート
  python -m collectors.call_data.run --csv path/to/call_logs.csv

  # バリデーションのみ（DBに書き込まない）
  python -m collectors.call_data.run --csv path/to/call_logs.csv --dry-run

  # エラー行を別ファイルに出力
  python -m collectors.call_data.run --csv path/to/call_logs.csv --error-out errors.csv
"""

import argparse
import csv
import logging
import os
import sys
from pathlib import Path

# プロジェクトルートを sys.path に追加
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config.settings import LOG_DIR
from db.connection import get_session
from collectors.call_data.csv_importer import parse_csv, ALL_COLUMNS
from collectors.call_data.db_writer import write_import_result

# ---------------------------------------------------------------------------
# ロギング設定
# ---------------------------------------------------------------------------
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(str(LOG_DIR / "call_data.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def export_errors(errors: list[dict], output_path: str) -> None:
    """バリデーションエラーをCSVに出力する。"""
    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["row", "message"])
        writer.writeheader()
        writer.writerows(errors)
    logger.info(f"エラー出力: {output_path} ({len(errors)}件)")


def main() -> None:
    parser = argparse.ArgumentParser(description="架電データCSVインポート")
    parser.add_argument("--csv", required=True, help="インポートするCSVファイルのパス")
    parser.add_argument("--dry-run", action="store_true", help="DBに書き込まずバリデーションのみ")
    parser.add_argument("--error-out", help="エラー行を出力するCSVファイルのパス")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        logger.error(f"ファイルが見つかりません: {csv_path}")
        sys.exit(1)

    # CSVパース・バリデーション
    logger.info(f"CSVインポート開始: {csv_path}")
    import_result = parse_csv(csv_path)

    logger.info(
        f"バリデーション結果: "
        f"{import_result.success_count}件OK / {import_result.error_count}件エラー"
    )

    # エラー出力
    if import_result.errors:
        if args.error_out:
            export_errors(import_result.errors, args.error_out)
        else:
            for e in import_result.errors:
                logger.warning(f"  行{e['row']}: {e['message']}")

    if not import_result.valid_rows:
        logger.warning("有効な行がありません。終了します。")
        sys.exit(0)

    if args.dry_run:
        logger.info("[DRY RUN] DBへの書き込みをスキップします")
        for row in import_result.valid_rows[:5]:
            logger.info(
                f"  サンプル: {row.company_name} / {row.sales_rep_name} / "
                f"{row.called_at} / {row.phone_status}"
            )
        sys.exit(0)

    # DB書き込み
    with get_session() as session:
        success, skip = write_import_result(session, import_result)

    logger.info(f"インポート完了: {success}件成功 / {skip}件スキップ")

    # BigQuery アップロード（UPLOAD_TO_BIGQUERY=true の場合）
    if os.getenv("UPLOAD_TO_BIGQUERY", "").lower() == "true":
        try:
            from db.bigquery import upload_call_logs
            bq_rows = [
                {
                    "company_name": row.company_name,
                    "sales_rep_name": row.sales_rep_name,
                    "called_at": row.called_at,
                    "phone_number": row.phone_number,
                    "phone_status": row.phone_status,
                    "product_name": row.product_name,
                    "phone_status_memo": row.phone_status_memo,
                    "discovered_number": row.discovered_number,
                    "discovered_number_memo": row.discovered_number_memo,
                    "call_result": row.call_result,
                    "spoke_with": row.spoke_with,
                    "discovered_person_chuto": row.discovered_person_chuto,
                    "discovered_person_shinsotsu": row.discovered_person_shinsotsu,
                    "notes": row.notes,
                }
                for row in import_result.valid_rows
            ]
            upload_call_logs(bq_rows)
        except Exception as e:
            logger.error(f"BigQuery アップロード失敗: {e}")


if __name__ == "__main__":
    main()
