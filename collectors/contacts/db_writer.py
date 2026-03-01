"""
db_writer.py
============
Gemini解析結果を DB の各テーブルに書き込む。

書き込み先:
  - phone_numbers     : 電話番号 (phone_db)
  - company_persons   : 担当者 (person_db)
  - company_field_values : 企業メール (email_db → field "企業メールアドレス")
  - person_phone_numbers : 担当者と電話番号の紐付け (person_db.relation_phone_number)

重複制御:
  - phone_numbers: (company_id, number) が UNIQUE → ON CONFLICT SKIP
  - company_persons: (company_id, name, department) で存在確認
  - company_field_values: (company_id, field_id) が UNIQUE → ON CONFLICT UPDATE
"""

import logging
import sys
from pathlib import Path
from typing import Optional
from uuid import UUID

# プロジェクトルートを sys.path に追加
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from db.models import (
    Company,
    CompanyFieldValue,
    CompanyPerson,
    FieldDefinition,
    PersonPhoneNumber,
    PhoneNumber,
)

logger = logging.getLogger(__name__)

_EMAIL_FIELD_CANONICAL = "企業メールアドレス"

# priority → label マッピング
_PRIORITY_LABEL: dict[int, str] = {
    1: "採用/人事直通",
    2: "採用部署代表",
    3: "会社代表",
    4: "その他",
}


def _get_or_create_email_field(session: Session) -> Optional[int]:
    """「企業メールアドレス」フィールド定義のIDを取得する。なければNone。"""
    fd = session.query(FieldDefinition).filter_by(canonical_name=_EMAIL_FIELD_CANONICAL).first()
    if fd:
        return fd.id
    # なければ動的に作成
    fd = FieldDefinition(
        canonical_name=_EMAIL_FIELD_CANONICAL,
        category="基本企業情報",
        data_type="text",
        aliases=["企業メール", "メールアドレス", "email"],
        media_presence={"Web収集": "optional"},
        source_priority=["Web収集"],
        display_order=99,
    )
    session.add(fd)
    session.flush()
    return fd.id


def write_phone_numbers(
    session: Session,
    company_id: UUID,
    phone_items: list[dict],
) -> dict[str, UUID]:
    """
    phone_db を phone_numbers テーブルに書き込む。
    Returns: {normalized_number: phone_number_id}
    """
    phone_id_map: dict[str, UUID] = {}

    for item in phone_items:
        number = item.get("phone_number", "").strip()
        if not number:
            continue

        priority = item.get("priority", 4)
        label = item.get("department_name") or _PRIORITY_LABEL.get(priority, "その他")
        if item.get("office_name"):
            label = f"{item['office_name']} {label}".strip()

        # ON CONFLICT DO NOTHING で重複スキップ
        stmt = (
            pg_insert(PhoneNumber)
            .values(
                company_id=company_id,
                number=number,
                label=label,
                status="未確認",
                source="Web収集",
            )
            .on_conflict_do_nothing(constraint="uq_company_phone")
            .returning(PhoneNumber.id)
        )
        result = session.execute(stmt)
        row = result.fetchone()

        if row:
            phone_id_map[number] = row[0]
            logger.debug(f"  phone INSERT: {number}")
        else:
            # 既存レコードのIDを取得
            existing = (
                session.query(PhoneNumber)
                .filter_by(company_id=company_id, number=number)
                .first()
            )
            if existing:
                phone_id_map[number] = existing.id

    return phone_id_map


def write_persons(
    session: Session,
    company_id: UUID,
    person_items: list[dict],
    phone_id_map: dict[str, UUID],
) -> None:
    """
    person_db を company_persons テーブルに書き込み、
    relation_phone_number があれば person_phone_numbers にも書き込む。
    """
    for item in person_items:
        name = item.get("person_name", "").strip()
        if not name:
            continue

        department = item.get("department_name") or None
        office = item.get("office_name") or None

        # 既存確認 (同名・同部署)
        existing = (
            session.query(CompanyPerson)
            .filter_by(company_id=company_id, name=name, department=department)
            .first()
        )
        if existing:
            person_obj = existing
        else:
            person_obj = CompanyPerson(
                company_id=company_id,
                name=name,
                department=department,
                notes=f"拠点: {office}" if office else None,
                source="Web収集",
            )
            session.add(person_obj)
            session.flush()
            logger.debug(f"  person INSERT: {name}")

        # 電話番号との紐付け
        rel_phone = item.get("relation_phone_number")
        if rel_phone and rel_phone in phone_id_map:
            phone_id = phone_id_map[rel_phone]
            existing_link = (
                session.query(PersonPhoneNumber)
                .filter_by(person_id=person_obj.id, phone_number_id=phone_id)
                .first()
            )
            if not existing_link:
                link = PersonPhoneNumber(
                    person_id=person_obj.id,
                    phone_number_id=phone_id,
                )
                session.add(link)


def write_emails(
    session: Session,
    company_id: UUID,
    email_items: list[dict],
    field_id: int,
) -> None:
    """
    email_db を company_field_values テーブルに書き込む。
    複数メールは JSON 配列として1フィールドに保存。
    """
    if not email_items:
        return

    import json

    # 既存値をロード
    existing = (
        session.query(CompanyFieldValue)
        .filter_by(company_id=company_id, field_id=field_id)
        .first()
    )

    new_emails = [
        {"address": item["email_address"], "type": item.get("type", "other")}
        for item in email_items
        if item.get("email_address")
    ]

    if existing:
        try:
            current = json.loads(existing.value)
            if not isinstance(current, list):
                current = []
        except (json.JSONDecodeError, TypeError):
            current = []
        # 重複除去してマージ
        existing_addrs = {e["address"] for e in current}
        for e in new_emails:
            if e["address"] not in existing_addrs:
                current.append(e)
        existing.value = json.dumps(current, ensure_ascii=False)
    else:
        cfv = CompanyFieldValue(
            company_id=company_id,
            field_id=field_id,
            value=json.dumps(new_emails, ensure_ascii=False),
            source="Web収集",
        )
        session.add(cfv)


def write_contact_results(
    session: Session,
    company_id: UUID,
    merged_result: dict,
) -> None:
    """
    merge_results() の出力をDBに一括書き込みする。

    Args:
        session: SQLAlchemy セッション
        company_id: 対象企業のUUID
        merged_result: {"phone_db": [...], "person_db": [...], "email_db": [...]}
    """
    phone_id_map = write_phone_numbers(
        session, company_id, merged_result.get("phone_db", [])
    )

    write_persons(
        session, company_id, merged_result.get("person_db", []), phone_id_map
    )

    email_field_id = _get_or_create_email_field(session)
    if email_field_id:
        write_emails(
            session, company_id, merged_result.get("email_db", []), email_field_id
        )

    logger.info(
        f"DB書き込み完了: company_id={company_id} "
        f"phones={len(merged_result.get('phone_db', []))} "
        f"persons={len(merged_result.get('person_db', []))} "
        f"emails={len(merged_result.get('email_db', []))}"
    )
