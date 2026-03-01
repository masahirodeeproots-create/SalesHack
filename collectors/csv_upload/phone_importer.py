"""
phone_importer.py
=================
電話番号データCSVを一括インポートする。

テンプレート: data/templates/phone_numbers_template.csv

使い方:
  python -m collectors.csv_upload.phone_importer --csv phone_numbers.csv
"""

import argparse
import csv
import logging
import sys
from pathlib import Path

_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config.settings import LOG_DIR
from db.connection import get_session
from db.models import Company, PhoneNumber

logger = logging.getLogger(__name__)

VALID_STATUSES = {
    "未確認", "該当", "使われてない", "AI対応", "別会社・別拠点・別事業部",
}


def _import_row(session, row: dict) -> str:
    """Returns: 'inserted' / 'updated' / 'skipped' / 'error'"""
    company_name = row.get("company_name", "").strip()
    phone_number = row.get("phone_number", "").strip()

    if not company_name or not phone_number:
        return "skipped"

    company = session.query(Company).filter_by(name_normalized=company_name).first()
    if not company:
        logger.warning(f"  企業未登録: {company_name}")
        return "skipped"

    status = row.get("status", "未確認").strip()
    if status not in VALID_STATUSES:
        status = "未確認"

    existing = (
        session.query(PhoneNumber)
        .filter_by(company_id=company.id, number=phone_number)
        .first()
    )

    if existing:
        # 既存レコードをアップデート（statusが「未確認」以外に変更された場合のみ）
        if status != "未確認" and existing.status == "未確認":
            existing.status = status
            existing.status_detail = row.get("status_detail", "").strip() or None
            return "updated"
        return "skipped"

    phone = PhoneNumber(
        company_id=company.id,
        number=phone_number,
        label=row.get("label", "").strip() or None,
        status=status,
        status_detail=row.get("status_detail", "").strip() or None,
        source=row.get("source", "CSVインポート").strip() or "CSVインポート",
    )
    session.add(phone)
    return "inserted"


def import_phones(csv_path: str | Path, encoding: str = "utf-8-sig", batch_size: int = 500) -> dict:
    stats = {"total": 0, "inserted": 0, "updated": 0, "skipped": 0, "errors": 0}
    csv_path = Path(csv_path)
    batch: list[dict] = []

    with open(csv_path, encoding=encoding, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            stats["total"] += 1
            batch.append(dict(row))
            if len(batch) >= batch_size:
                _flush(batch, stats)
                batch = []
                logger.info(f"  進捗: {stats['total']}行")
        if batch:
            _flush(batch, stats)

    logger.info(f"完了: {stats}")
    return stats


def _flush(batch: list[dict], stats: dict) -> None:
    with get_session() as session:
        for row in batch:
            try:
                result = _import_row(session, row)
                stats[result] = stats.get(result, 0) + 1
            except Exception as e:
                logger.error(f"  エラー: {e}")
                stats["errors"] += 1


def main() -> None:
    parser = argparse.ArgumentParser(description="電話番号CSV一括インポート")
    parser.add_argument("--csv", required=True)
    parser.add_argument("--encoding", default="utf-8-sig")
    args = parser.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[logging.FileHandler(str(LOG_DIR / "phone_import.log"), encoding="utf-8"),
                                  logging.StreamHandler()])

    stats = import_phones(args.csv, args.encoding)
    print(f"\n結果: {stats}")


if __name__ == "__main__":
    main()
