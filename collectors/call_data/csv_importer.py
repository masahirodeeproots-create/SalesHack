"""
csv_importer.py
===============
営業マンが記録した架電データCSVを読み込み・バリデーションする。

CSVフォーマット (UTF-8 BOM付き / カンマ区切り):
  必須列:
    company_name        - 企業名（name_normalized と突合）
    sales_rep_name      - 営業担当者名
    called_at           - 架電日時 (YYYY-MM-DD HH:MM 形式)
    phone_number        - かけた電話番号
    phone_status        - 番号ステータス (該当/使われてない/AI対応/別会社・別拠点・別事業部)
  任意列:
    product_name        - 営業商品名 (省略可)
    phone_status_memo   - 別会社等の場合の詳細
    discovered_number   - 架電中に新しく判明した電話番号
    discovered_number_memo - 新番号メモ
    call_result         - 通話結果 (不在/受付ブロック/着電NG/獲得見込み/資料請求/アポ/架電NG)
    spoke_with          - 実際に話した相手
    discovered_person_chuto   - 判明した中途採用担当者名
    discovered_person_shinsotsu - 判明した新卒採用担当者名
    notes               - メモ

テンプレートCSVは data/templates/call_logs_template.csv を参照。
"""

import csv
import io
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 許容値の定義
# ---------------------------------------------------------------------------

VALID_PHONE_STATUSES = {
    "該当",
    "使われてない",
    "AI対応",
    "別会社・別拠点・別事業部",
}

VALID_CALL_RESULTS = {
    "不在",
    "受付ブロック",
    "着電NG",
    "獲得見込み",
    "資料請求",
    "アポ",
    "架電NG",
}

REQUIRED_COLUMNS = {
    "company_name",
    "sales_rep_name",
    "called_at",
    "phone_number",
    "phone_status",
}

ALL_COLUMNS = list(REQUIRED_COLUMNS) + [
    "product_name",
    "phone_status_memo",
    "discovered_number",
    "discovered_number_memo",
    "call_result",
    "spoke_with",
    "discovered_person_chuto",
    "discovered_person_shinsotsu",
    "notes",
]


# ---------------------------------------------------------------------------
# データクラス
# ---------------------------------------------------------------------------

@dataclass
class CallRow:
    """バリデーション済みの1行分データ。"""
    company_name: str
    sales_rep_name: str
    called_at: datetime
    phone_number: str
    phone_status: str
    product_name: Optional[str] = None
    phone_status_memo: Optional[str] = None
    discovered_number: Optional[str] = None
    discovered_number_memo: Optional[str] = None
    call_result: Optional[str] = None
    spoke_with: Optional[str] = None
    discovered_person_chuto: Optional[str] = None
    discovered_person_shinsotsu: Optional[str] = None
    notes: Optional[str] = None
    _row_number: int = 0  # エラー報告用


@dataclass
class ImportResult:
    """インポート結果サマリー。"""
    valid_rows: list[CallRow] = field(default_factory=list)
    errors: list[dict] = field(default_factory=list)  # {"row": N, "message": "..."}

    @property
    def total(self) -> int:
        return len(self.valid_rows) + len(self.errors)

    @property
    def success_count(self) -> int:
        return len(self.valid_rows)

    @property
    def error_count(self) -> int:
        return len(self.errors)


# ---------------------------------------------------------------------------
# バリデーション
# ---------------------------------------------------------------------------

_DATE_FORMATS = [
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
    "%Y/%m/%d %H:%M:%S",
    "%Y-%m-%d",
    "%Y/%m/%d",
]


def _parse_datetime(raw: str) -> Optional[datetime]:
    raw = raw.strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def _clean(val: Optional[str]) -> Optional[str]:
    if val is None:
        return None
    val = val.strip()
    return val if val else None


def _validate_row(row: dict, row_number: int) -> tuple[Optional[CallRow], Optional[str]]:
    """
    1行をバリデーションする。
    Returns: (CallRow, None) if valid, (None, error_message) if invalid.
    """
    # 必須列チェック
    for col in REQUIRED_COLUMNS:
        if not _clean(row.get(col)):
            return None, f"必須列 '{col}' が空です"

    # 架電日時
    called_at = _parse_datetime(row["called_at"])
    if not called_at:
        return None, f"called_at の日時形式が不正です: '{row['called_at']}' (例: 2025-01-15 14:30)"

    # phone_status
    phone_status = _clean(row["phone_status"])
    if phone_status not in VALID_PHONE_STATUSES:
        return None, f"phone_status が不正です: '{phone_status}' (許容値: {', '.join(VALID_PHONE_STATUSES)})"

    # call_result (任意だが値があれば検証)
    call_result = _clean(row.get("call_result"))
    if call_result and call_result not in VALID_CALL_RESULTS:
        return None, f"call_result が不正です: '{call_result}' (許容値: {', '.join(VALID_CALL_RESULTS)})"

    return CallRow(
        company_name=_clean(row["company_name"]),
        sales_rep_name=_clean(row["sales_rep_name"]),
        called_at=called_at,
        phone_number=_clean(row["phone_number"]),
        phone_status=phone_status,
        product_name=_clean(row.get("product_name")),
        phone_status_memo=_clean(row.get("phone_status_memo")),
        discovered_number=_clean(row.get("discovered_number")),
        discovered_number_memo=_clean(row.get("discovered_number_memo")),
        call_result=call_result,
        spoke_with=_clean(row.get("spoke_with")),
        discovered_person_chuto=_clean(row.get("discovered_person_chuto")),
        discovered_person_shinsotsu=_clean(row.get("discovered_person_shinsotsu")),
        notes=_clean(row.get("notes")),
        _row_number=row_number,
    ), None


# ---------------------------------------------------------------------------
# CSVパース
# ---------------------------------------------------------------------------

def parse_csv(
    source: str | Path | io.IOBase,
    encoding: str = "utf-8-sig",
) -> ImportResult:
    """
    CSVを読み込みバリデーションする。

    Args:
        source: ファイルパス または ファイルオブジェクト
        encoding: 文字コード (デフォルト: BOM付きUTF-8)

    Returns:
        ImportResult
    """
    result = ImportResult()

    # ファイルオープン
    if isinstance(source, (str, Path)):
        f = open(source, encoding=encoding, newline="")
        close_after = True
    else:
        f = source
        close_after = False

    try:
        reader = csv.DictReader(f)

        # ヘッダー確認
        if reader.fieldnames is None:
            result.errors.append({"row": 0, "message": "CSVが空です"})
            return result

        missing = REQUIRED_COLUMNS - set(reader.fieldnames)
        if missing:
            result.errors.append({
                "row": 0,
                "message": f"必須列が不足しています: {', '.join(sorted(missing))}",
            })
            return result

        for row_number, row in enumerate(reader, start=2):  # ヘッダーが1行目
            call_row, error = _validate_row(dict(row), row_number)
            if error:
                result.errors.append({"row": row_number, "message": error})
                logger.warning(f"行{row_number} バリデーションエラー: {error}")
            else:
                result.valid_rows.append(call_row)

    finally:
        if close_after:
            f.close()

    logger.info(
        f"CSV読み込み完了: {result.success_count}件OK / {result.error_count}件エラー"
    )
    return result
