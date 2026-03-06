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
  3. 全フィールドを rawdata_company_info に INSERT
  4. バッチサイズ単位でコミット（メモリ効率）

使い方:
  python -m collectors.csv_upload.company_importer --csv data.csv
  python -m collectors.csv_upload.company_importer --csv data.csv --delimiter '|' --batch-size 1000
"""

import argparse
import csv
import logging
import re
import sys
from pathlib import Path

# プロジェクトルートを sys.path に追加
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config.settings import LOG_DIR
from db.connection import get_session
from db.models import Company, RawdataCompanyInfo

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


def _clean_value(col: str, value: str) -> str:
    """列ごとに値を正規化する。"""
    if col == "本社都道府県":
        # "09:栃木県" → "栃木県" のようなコード付きプレフィックスを除去
        value = re.sub(r"^\d{2}:", "", value.strip())
    return value.strip()


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
    stock_code = row.get("証券コード", "").strip() or None
    if not company:
        company = Company(
            name=company_name,
            name_normalized=company_name,
            stock_code=stock_code,
        )
        session.add(company)
        session.flush()
    elif stock_code and not company.stock_code:
        company.stock_code = stock_code
        session.flush()

    # rawdata_company_info に INSERT
    rawdata = RawdataCompanyInfo(
        original_id=str(company.id),
        企業名=company_name,
        本社都道府県=_clean_value("本社都道府県", row.get("本社都道府県", "")),
        代表者名=row.get("代表者名", "").strip() or None,
        従業員数=row.get("従業員数", "").strip() or None,
        企業規模=row.get("企業規模", "").strip() or None,
        業種=row.get("業種", "").strip() or None,
        業種詳細=row.get("業種詳細", "").strip() or None,
        代表電話番号=row.get("代表電話番号", "").strip() or None,
    )
    session.add(rawdata)

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
