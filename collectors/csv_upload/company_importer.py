"""
company_importer.py
===================
既存の企業データCSVを一括インポートする（13万件対応）。

対応フォーマット:
  - パイプ区切り (|): 企業名|本社都道府県|代表者名|従業員数|企業規模|業種|業種詳細|代表電話番号
  - カンマ区切り (.csv): 上記と同じ列順
  - ヘッダー行あり・なし両対応（ヘッダーがあれば列名で判断）

処理フロー:
  1. CSV行を読み込む
  2. 企業名を name_normalized として companies に UPSERT
  3. 各フィールドを field_definitions と照合して company_field_values に UPSERT
  4. 代表電話番号は phone_numbers に追加（label="代表"）
  5. バッチサイズ単位でコミット（メモリ効率）

使い方:
  python -m collectors.csv_upload.company_importer --csv data.csv
  python -m collectors.csv_upload.company_importer --csv data.csv --delimiter '|' --batch-size 1000
"""

import argparse
import csv
import json
import logging
import sys
from pathlib import Path
from typing import Optional

# プロジェクトルートを sys.path に追加
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config.settings import LOG_DIR
from db.connection import get_session
from db.models import (
    Company,
    CompanyFieldValue,
    FieldDefinition,
    PhoneNumber,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 列名の定義
# ---------------------------------------------------------------------------

# パイプ区切りの既存データの列順 (ヘッダーなしの場合)
DEFAULT_COLUMNS = [
    "企業名",
    "本社都道府県",
    "代表者名",
    "従業員数",
    "企業規模",
    "業種",
    "業種詳細",
    "代表電話番号",
]

# DB フィールドへのマッピング (CSV列名 → canonical名)
COLUMN_TO_CANONICAL: dict[str, str] = {
    "本社都道府県": "本社所在地",
    "代表者名": "代表者",
    "従業員数": "従業員数",
    "企業規模": "企業規模",
    "業種": "業種",
    "業種詳細": "業種",        # 業種と同じフィールドに入れる（詳細優先）
    "代表電話番号": "__phone__",   # 特別処理
}

# 文字列を正規化（企業名マッチング用）
def _normalize_name(name: str) -> str:
    name = name.strip()
    for suffix in ["株式会社", "有限会社", "合同会社", "合資会社", "社団法人", "財団法人"]:
        name = name.replace(suffix, "").strip()
    return name


# ---------------------------------------------------------------------------
# フィールド定義キャッシュ
# ---------------------------------------------------------------------------

_field_cache: dict[str, Optional[int]] = {}  # canonical_name → field_id


def _get_field_id(session, canonical_name: str) -> Optional[int]:
    if canonical_name in _field_cache:
        return _field_cache[canonical_name]
    fd = session.query(FieldDefinition).filter_by(canonical_name=canonical_name).first()
    fid = fd.id if fd else None
    _field_cache[canonical_name] = fid
    return fid


# ---------------------------------------------------------------------------
# 1行の処理
# ---------------------------------------------------------------------------

def _process_row(session, row: dict, source_name: str = "CSVインポート") -> bool:
    """
    1行分の企業データをDBに書き込む。
    Returns: True if success.
    """
    company_name = row.get("企業名", "").strip()
    if not company_name:
        return False

    # companies UPSERT
    company = session.query(Company).filter_by(name_normalized=company_name).first()
    if not company:
        company = Company(name_raw=company_name, name_normalized=company_name)
        session.add(company)
        session.flush()

    # 各フィールドを処理
    for csv_col, canonical in COLUMN_TO_CANONICAL.items():
        value = row.get(csv_col, "").strip()
        if not value:
            continue

        if canonical == "__phone__":
            # 代表電話番号 → phone_numbers
            existing = (
                session.query(PhoneNumber)
                .filter_by(company_id=company.id, number=value)
                .first()
            )
            if not existing:
                phone = PhoneNumber(
                    company_id=company.id,
                    number=value,
                    label="代表",
                    status="未確認",
                    source=source_name,
                )
                session.add(phone)
            continue

        # field_definitions に対応するフィールドがあれば保存
        field_id = _get_field_id(session, canonical)
        if not field_id:
            continue

        existing_val = (
            session.query(CompanyFieldValue)
            .filter_by(company_id=company.id, field_id=field_id)
            .first()
        )
        if existing_val:
            # 既存値があればスキップ（上書きしない）
            continue
        cfv = CompanyFieldValue(
            company_id=company.id,
            field_id=field_id,
            value=value,
            source=source_name,
        )
        session.add(cfv)

    return True


# ---------------------------------------------------------------------------
# CSV読み込み
# ---------------------------------------------------------------------------

def import_companies(
    csv_path: str | Path,
    delimiter: str = "|",
    has_header: bool = True,
    batch_size: int = 500,
    encoding: str = "utf-8-sig",
    source_name: str = "CSVインポート",
) -> dict:
    """
    企業データCSVを一括インポートする。

    Args:
        csv_path: CSVファイルパス
        delimiter: 区切り文字 ('|' or ',')
        has_header: ヘッダー行があるか
        batch_size: 一度にコミットする行数
        encoding: 文字コード
        source_name: データソース名（DB記録用）

    Returns:
        {"total": N, "inserted": N, "skipped": N, "errors": N}
    """
    _field_cache.clear()  # セッション跨ぎのキャッシュをリセット

    stats = {"total": 0, "inserted": 0, "skipped": 0, "errors": 0}
    csv_path = Path(csv_path)

    with open(csv_path, encoding=encoding, newline="") as f:
        if has_header:
            reader = csv.DictReader(f, delimiter=delimiter)
        else:
            reader = csv.DictReader(f, fieldnames=DEFAULT_COLUMNS, delimiter=delimiter)

        batch: list[dict] = []
        for row in reader:
            stats["total"] += 1
            batch.append(dict(row))

            if len(batch) >= batch_size:
                inserted, errors = _flush_batch(batch, source_name)
                stats["inserted"] += inserted
                stats["errors"] += errors
                stats["skipped"] += len(batch) - inserted - errors
                batch = []
                logger.info(f"  進捗: {stats['total']}行処理済み")

        if batch:
            inserted, errors = _flush_batch(batch, source_name)
            stats["inserted"] += inserted
            stats["errors"] += errors
            stats["skipped"] += len(batch) - inserted - errors

    logger.info(
        f"インポート完了: "
        f"total={stats['total']} inserted={stats['inserted']} "
        f"skipped={stats['skipped']} errors={stats['errors']}"
    )
    return stats


def _flush_batch(batch: list[dict], source_name: str) -> tuple[int, int]:
    """バッチをDBに書き込む。Returns: (inserted, errors)"""
    inserted = 0
    errors = 0
    with get_session() as session:
        for row in batch:
            try:
                ok = _process_row(session, row, source_name)
                if ok:
                    inserted += 1
                else:
                    errors += 1
            except Exception as e:
                logger.error(f"  行処理エラー: {row.get('企業名', '?')} - {e}")
                errors += 1
    return inserted, errors


# ---------------------------------------------------------------------------
# CLI エントリポイント
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="企業データCSV一括インポート")
    parser.add_argument("--csv", required=True, help="インポートするCSVファイル")
    parser.add_argument("--delimiter", default="|", help="区切り文字 (デフォルト: |)")
    parser.add_argument("--no-header", action="store_true", help="ヘッダー行がない場合に指定")
    parser.add_argument("--batch-size", type=int, default=500, help="バッチサイズ")
    parser.add_argument("--encoding", default="utf-8-sig", help="文字コード")
    args = parser.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(str(LOG_DIR / "company_import.log"), encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

    stats = import_companies(
        csv_path=args.csv,
        delimiter=args.delimiter,
        has_header=not args.no_header,
        batch_size=args.batch_size,
        encoding=args.encoding,
    )
    print(
        f"\n結果: {stats['total']}行 | "
        f"新規登録: {stats['inserted']} | "
        f"スキップ: {stats['skipped']} | "
        f"エラー: {stats['errors']}"
    )


if __name__ == "__main__":
    main()
