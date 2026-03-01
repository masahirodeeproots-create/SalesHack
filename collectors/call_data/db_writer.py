"""
db_writer.py
============
バリデーション済みの架電データをDBに書き込む。

処理内容:
  1. company_name で companies を検索（未登録なら警告しスキップ）
  2. sales_rep_name で sales_reps を検索（未登録なら自動作成）
  3. product_name で products を検索（省略時はNULL）
  4. phone_number で phone_numbers を検索（未登録なら新規作成）
  5. call_logs に INSERT
  6. phone_numbers.status を phone_status で更新
  7. discovered_number があれば phone_numbers に追加
  8. discovered_person_chuto/shinsotsu があれば company_persons に追加/更新
"""

import logging
import sys
from datetime import timedelta, timezone

JST = timezone(timedelta(hours=9))
from pathlib import Path
from typing import Optional
from uuid import UUID

# プロジェクトルートを sys.path に追加
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from sqlalchemy.orm import Session

from db.models import (
    CallLog,
    Company,
    CompanyPerson,
    PhoneNumber,
    Product,
    SalesRep,
)
from collectors.call_data.csv_importer import CallRow, ImportResult

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ルックアップヘルパー
# ---------------------------------------------------------------------------

def _find_company(session: Session, name: str) -> Optional[Company]:
    """name_normalized で企業を検索。前方一致も試みる。"""
    company = session.query(Company).filter_by(name_normalized=name).first()
    if company:
        return company
    # 部分一致（前方一致）
    company = (
        session.query(Company)
        .filter(Company.name_normalized.like(f"{name}%"))
        .first()
    )
    return company


def _get_or_create_sales_rep(session: Session, name: str) -> SalesRep:
    rep = session.query(SalesRep).filter_by(name=name).first()
    if not rep:
        rep = SalesRep(name=name, active=True)
        session.add(rep)
        session.flush()
        logger.info(f"  SalesRep 新規作成: {name}")
    return rep


def _find_product(session: Session, name: Optional[str]) -> Optional[Product]:
    if not name:
        return None
    return session.query(Product).filter_by(name=name).first()


def _get_or_create_phone(
    session: Session,
    company_id: UUID,
    number: str,
    source: str = "架電",
) -> PhoneNumber:
    phone = (
        session.query(PhoneNumber)
        .filter_by(company_id=company_id, number=number)
        .first()
    )
    if not phone:
        phone = PhoneNumber(
            company_id=company_id,
            number=number,
            status="未確認",
            source=source,
        )
        session.add(phone)
        session.flush()
        logger.debug(f"  PhoneNumber 新規作成: {number}")
    return phone


def _upsert_person(
    session: Session,
    company_id: UUID,
    name: str,
    role: Optional[str] = None,
) -> CompanyPerson:
    person = (
        session.query(CompanyPerson)
        .filter_by(company_id=company_id, name=name)
        .first()
    )
    if not person:
        person = CompanyPerson(
            company_id=company_id,
            name=name,
            role=role,
            source="架電判明",
        )
        session.add(person)
        session.flush()
        logger.debug(f"  CompanyPerson 新規作成: {name} ({role})")
    elif role:
        person.role = role
    return person


# ---------------------------------------------------------------------------
# メイン書き込み
# ---------------------------------------------------------------------------

def write_call_row(session: Session, row: CallRow) -> bool:
    """
    1行分の架電データをDBに書き込む。
    Returns: True if success, False if skipped.
    """
    # 企業検索
    company = _find_company(session, row.company_name)
    if not company:
        logger.warning(f"  企業未登録のためスキップ: '{row.company_name}'")
        return False

    # 営業担当者
    sales_rep = _get_or_create_sales_rep(session, row.sales_rep_name)

    # 商品
    product = _find_product(session, row.product_name)

    # 電話番号
    phone = _get_or_create_phone(session, company.id, row.phone_number)

    # called_at をタイムゾーン付きに（入力値は日本時間として扱う）
    called_at = row.called_at
    if called_at.tzinfo is None:
        called_at = called_at.replace(tzinfo=JST)

    # call_logs に INSERT
    call_log = CallLog(
        company_id=company.id,
        phone_number_id=phone.id,
        sales_rep_id=sales_rep.id,
        product_id=product.id if product else None,
        called_at=called_at,
        phone_status=row.phone_status,
        phone_status_memo=row.phone_status_memo,
        discovered_number=row.discovered_number,
        discovered_number_memo=row.discovered_number_memo,
        call_result=row.call_result,
        spoke_with=row.spoke_with,
        discovered_person_chuto=row.discovered_person_chuto,
        discovered_person_shinsotsu=row.discovered_person_shinsotsu,
        notes=row.notes,
    )
    session.add(call_log)

    # phone_numbers.status を更新
    if row.phone_status != "未確認":
        phone.status = row.phone_status
        if row.phone_status_memo:
            phone.status_detail = row.phone_status_memo

    # 新規発見番号を登録
    if row.discovered_number:
        _get_or_create_phone(
            session, company.id, row.discovered_number, source="架電判明"
        )

    # 担当者を登録（同一人物が中途・新卒両方に記載された場合はロールを結合）
    chuto = row.discovered_person_chuto
    shinsotsu = row.discovered_person_shinsotsu
    if chuto and shinsotsu and chuto == shinsotsu:
        _upsert_person(session, company.id, chuto, role="中途・新卒採用担当")
    else:
        if chuto:
            _upsert_person(session, company.id, chuto, role="中途採用担当")
        if shinsotsu:
            _upsert_person(session, company.id, shinsotsu, role="新卒採用担当")

    return True


def write_import_result(
    session: Session,
    import_result: ImportResult,
) -> tuple[int, int]:
    """
    ImportResult の全有効行をDBに書き込む。
    Returns: (success_count, skip_count)
    """
    success = 0
    skip = 0

    for row in import_result.valid_rows:
        try:
            ok = write_call_row(session, row)
            if ok:
                success += 1
            else:
                skip += 1
        except Exception as e:
            logger.error(f"  行{row._row_number} 書き込みエラー: {e}", exc_info=True)
            skip += 1

    logger.info(f"DB書き込み完了: {success}件成功 / {skip}件スキップ")
    return success, skip
