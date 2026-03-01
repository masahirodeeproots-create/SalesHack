"""
persons_importer.py
===================
担当者データCSVを一括インポートする。

テンプレート: data/templates/persons_template.csv

使い方:
  python -m collectors.csv_upload.persons_importer --csv persons.csv
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
from db.models import Company, CompanyPerson

logger = logging.getLogger(__name__)


def _import_row(session, row: dict) -> str:
    company_name = row.get("company_name", "").strip()
    person_name = row.get("person_name", "").strip()
    if not company_name or not person_name:
        return "skipped"

    company = session.query(Company).filter_by(name_normalized=company_name).first()
    if not company:
        logger.warning(f"  企業未登録: {company_name}")
        return "skipped"

    department = row.get("department", "").strip() or None
    existing = (
        session.query(CompanyPerson)
        .filter_by(company_id=company.id, name=person_name, department=department)
        .first()
    )
    if existing:
        return "skipped"

    person = CompanyPerson(
        company_id=company.id,
        name=person_name,
        department=department,
        role=row.get("role", "").strip() or None,
        email=row.get("email", "").strip() or None,
        notes=row.get("notes", "").strip() or None,
        source=row.get("source", "CSVインポート").strip() or "CSVインポート",
    )
    session.add(person)
    return "inserted"


def import_persons(csv_path: str | Path, encoding: str = "utf-8-sig", batch_size: int = 500) -> dict:
    stats = {"total": 0, "inserted": 0, "skipped": 0, "errors": 0}
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
    parser = argparse.ArgumentParser(description="担当者CSV一括インポート")
    parser.add_argument("--csv", required=True)
    parser.add_argument("--encoding", default="utf-8-sig")
    args = parser.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[logging.FileHandler(str(LOG_DIR / "persons_import.log"), encoding="utf-8"),
                                  logging.StreamHandler()])

    stats = import_persons(args.csv, args.encoding)
    print(f"\n結果: {stats}")


if __name__ == "__main__":
    main()
