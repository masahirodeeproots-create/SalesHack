"""
reset_bigquery.py
=================
BQ テーブルの全データを削除する（テストデータのリセット用）。

対象テーブル:
  - company_data.companies
  - company_data.contacts
  - company_data.hr_service_usages
  - company_data.en_hyouban_reviews
  ※ call_logs は削除しない（架電履歴は消したくない場合が多いため --include-call-logs で明示指定）

実行方法:
  python scripts/reset_bigquery.py               # 上記4テーブル削除
  python scripts/reset_bigquery.py --include-call-logs  # call_logsも削除
  python scripts/reset_bigquery.py --dry-run     # 削除せず確認のみ
"""

import argparse
import sys
from pathlib import Path

_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config.settings import GCP_PROJECT_ID, BQ_DATASET


def reset_tables(include_call_logs: bool = False, dry_run: bool = False) -> None:
    try:
        from google.cloud import bigquery
    except ImportError:
        print("ERROR: google-cloud-bigquery がインストールされていません")
        print("  pip install google-cloud-bigquery")
        sys.exit(1)

    client = bigquery.Client(project=GCP_PROJECT_ID)

    tables = [
        "companies",
        "contacts",
        "hr_service_usages",
        "en_hyouban_reviews",
    ]
    if include_call_logs:
        tables.append("call_logs")

    print(f"BigQuery プロジェクト: {GCP_PROJECT_ID}")
    print(f"データセット:         {BQ_DATASET}")
    print(f"対象テーブル:         {tables}")
    print(f"モード:               {'DRY RUN（削除しません）' if dry_run else '本番（削除します）'}")
    print()

    for table_name in tables:
        table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}"
        try:
            table = client.get_table(table_id)
            row_count = table.num_rows
        except Exception:
            print(f"  [{table_name}] テーブルが存在しないためスキップ")
            continue

        if dry_run:
            print(f"  [{table_name}] {row_count:,}行 → 削除対象（dry-run）")
            continue

        confirm = input(f"  [{table_name}] {row_count:,}行を削除しますか？ (y/N): ").strip().lower()
        if confirm != "y":
            print(f"  [{table_name}] スキップ")
            continue

        client.delete_table(table_id, not_found_ok=True)
        print(f"  [{table_name}] 削除完了（{row_count:,}行）")

    print("\n完了！")


def main():
    parser = argparse.ArgumentParser(description="BigQuery テーブルリセット")
    parser.add_argument("--include-call-logs", action="store_true",
                        help="call_logs テーブルも削除する（通常は除外）")
    parser.add_argument("--dry-run", action="store_true",
                        help="削除せず確認のみ")
    args = parser.parse_args()
    reset_tables(include_call_logs=args.include_call_logs, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
