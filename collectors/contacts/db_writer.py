"""
db_writer.py
============
Gemini解析結果を rawdata テーブルに書き込む。

書き込み先:
  - rawdata_phones   : 電話番号 & 付随データ
  - rawdata_persons  : 担当者名 & 付随データ
  - rawdata_emails   : メールアドレス

重複制御:
  - rawdata_phones: (original_id, 電話番号) が既存 → スキップ（既存IDを返却）
  - rawdata_persons: (original_id, 担当者名, 事業部) が既存 → スキップ
  - rawdata_emails: (original_id, メールアドレス) が既存 → スキップ
"""

import logging
import sys
from pathlib import Path
from uuid import UUID

# プロジェクトルートを sys.path に追加
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from sqlalchemy.orm import Session

from db.models import RawdataEmails, RawdataPersons, RawdataPhones

logger = logging.getLogger(__name__)

# priority → ラベル マッピング
_PRIORITY_LABEL: dict[int, str] = {
    1: "採用/人事直通",
    2: "採用部署代表",
    3: "会社代表",
    4: "その他",
}


def write_phone_numbers(
    session: Session,
    company_id: UUID,
    phone_items: list[dict],
    source_url: str | None = None,
) -> dict[str, str]:
    """
    phone_db を rawdata_phones テーブルに書き込む。
    Returns: {phone_number: rawdata_phones.id (str)} — 担当者リレーション用
    """
    phone_id_map: dict[str, str] = {}
    original_id = str(company_id)

    for item in phone_items:
        number = item.get("phone_number", "").strip()
        if not number:
            continue

        # 重複チェック: (original_id, 電話番号) が既存ならスキップ
        existing = (
            session.query(RawdataPhones)
            .filter_by(original_id=original_id, 電話番号=number)
            .first()
        )
        if existing:
            phone_id_map[number] = str(existing.id)
            logger.debug(f"  rawdata_phones SKIP (既存): {number}")
            continue

        priority = item.get("priority", 4)
        label = _PRIORITY_LABEL.get(priority, "その他")

        row = RawdataPhones(
            original_id=original_id,
            source_url=source_url,
            拠点=item.get("office_name") or None,
            事業部=item.get("department_name") or None,
            ラベル=label,
            電話番号=number,
        )
        session.add(row)
        session.flush()  # id を確定させる
        phone_id_map[number] = str(row.id)
        logger.debug(f"  rawdata_phones INSERT: {number}")

    return phone_id_map


def write_persons(
    session: Session,
    company_id: UUID,
    person_items: list[dict],
    phone_id_map: dict[str, str],
    source_url: str | None = None,
) -> None:
    """
    person_db を rawdata_persons テーブルに書き込む。
    relation_phone_number があれば 電話番号リレーションキー にセットする。
    """
    original_id = str(company_id)

    for item in person_items:
        name = item.get("person_name", "").strip()
        if not name:
            continue

        department = item.get("department_name") or None

        # 重複チェック: (original_id, 担当者名, 事業部) が既存ならスキップ
        existing = (
            session.query(RawdataPersons)
            .filter_by(original_id=original_id, 担当者名=name, 事業部=department)
            .first()
        )
        if existing:
            logger.debug(f"  rawdata_persons SKIP (既存): {name}")
            continue

        rel_phone_number = item.get("relation_phone_number")
        rel_phone_id = phone_id_map.get(rel_phone_number) if rel_phone_number else None

        row = RawdataPersons(
            original_id=original_id,
            source_url=source_url,
            拠点=item.get("office_name") or None,
            事業部=department,
            担当者名=name,
            電話番号リレーションキー=rel_phone_id,
        )
        session.add(row)
        logger.debug(f"  rawdata_persons INSERT: {name}")


def write_emails(
    session: Session,
    company_id: UUID,
    email_items: list[dict],
) -> None:
    """
    email_db を rawdata_emails テーブルに書き込む（1行1メールアドレス）。
    """
    original_id = str(company_id)

    for item in email_items:
        address = item.get("email_address", "").strip()
        if not address:
            continue

        # 重複チェック: (original_id, メールアドレス) が既存ならスキップ
        existing = (
            session.query(RawdataEmails)
            .filter_by(original_id=original_id, メールアドレス=address)
            .first()
        )
        if existing:
            logger.debug(f"  rawdata_emails SKIP (既存): {address}")
            continue

        row = RawdataEmails(
            original_id=original_id,
            事業部=item.get("type") or None,
            メールアドレス=address,
        )
        session.add(row)
        logger.debug(f"  rawdata_emails INSERT: {address}")


def write_contact_results(
    session: Session,
    company_id: UUID,
    merged_result: dict,
    source_url: str | None = None,
) -> None:
    """
    merge_results() の出力を rawdata テーブルに一括書き込みする。

    Args:
        session: SQLAlchemy セッション
        company_id: 対象企業のUUID
        merged_result: {"phone_db": [...], "person_db": [...], "email_db": [...]}
        source_url: スニペット出典URL（省略可）
    """
    phone_id_map = write_phone_numbers(
        session, company_id, merged_result.get("phone_db", []), source_url
    )

    write_persons(
        session, company_id, merged_result.get("person_db", []), phone_id_map, source_url
    )

    write_emails(
        session, company_id, merged_result.get("email_db", [])
    )

    logger.info(
        f"rawdata書き込み完了: company_id={company_id} "
        f"phones={len(merged_result.get('phone_db', []))} "
        f"persons={len(merged_result.get('person_db', []))} "
        f"emails={len(merged_result.get('email_db', []))}"
    )
