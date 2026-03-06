"""
analytics/csv_exporter.py
=========================
DBの各テーブルをCSVにエクスポートする。

エクスポート対象:
  - companies_export.csv     : 企業マスタ＋rawdata基本情報＋エン評判
  - phone_numbers_export.csv : 電話番号一覧（営業管理テーブル）
  - persons_export.csv       : 担当者一覧（営業管理テーブル）
  - call_logs_export.csv     : 架電ログ一覧（営業管理テーブル）
  - deals_export.csv         : 商談一覧（営業管理テーブル）

使い方:
  # 全テーブルをエクスポート
  python -m analytics.csv_exporter

  # 特定テーブルのみ
  python -m analytics.csv_exporter --tables companies phones

  # 出力先を指定
  python -m analytics.csv_exporter --output-dir data/export/2025-04-01
"""

import argparse
import csv
import logging
import sys
from datetime import datetime
from pathlib import Path

_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config.settings import OUTPUT_DIR, LOG_DIR
from db.connection import get_session
from db.models import (
    CallLog,
    Company,
    CompanyPerson,
    Deal,
    PhoneNumber,
    Product,
    RawdataCompanyInfo,
    RawdataEnHyouban,
    SalesRep,
)

logger = logging.getLogger(__name__)

DEFAULT_EXPORT_DIR = OUTPUT_DIR / "exports"


# ---------------------------------------------------------------------------
# エクスポート関数
# ---------------------------------------------------------------------------

def export_companies(session, output_path: Path) -> int:
    """
    企業マスタ＋rawdata_company_info＋rawdata_en_hyouban を横展開してCSVに出力する。
    各rawdataテーブルから original_id ごとに最新1件を取得する。
    """
    from sqlalchemy import func

    companies: list[Company] = session.query(Company).order_by(Company.name_normalized).all()

    # rawdata_company_info: original_id ごとに最新1件
    ci_subq = (
        session.query(
            RawdataCompanyInfo.original_id,
            func.max(RawdataCompanyInfo.scraped_at).label("max_scraped"),
        )
        .group_by(RawdataCompanyInfo.original_id)
        .subquery()
    )
    ci_rows = (
        session.query(RawdataCompanyInfo)
        .join(ci_subq, (RawdataCompanyInfo.original_id == ci_subq.c.original_id)
              & (RawdataCompanyInfo.scraped_at == ci_subq.c.max_scraped))
        .all()
    )
    ci_map = {r.original_id: r for r in ci_rows}

    # rawdata_en_hyouban: original_id ごとに最新1件
    eh_subq = (
        session.query(
            RawdataEnHyouban.original_id,
            func.max(RawdataEnHyouban.scraped_at).label("max_scraped"),
        )
        .group_by(RawdataEnHyouban.original_id)
        .subquery()
    )
    eh_rows = (
        session.query(RawdataEnHyouban)
        .join(eh_subq, (RawdataEnHyouban.original_id == eh_subq.c.original_id)
              & (RawdataEnHyouban.scraped_at == eh_subq.c.max_scraped))
        .all()
    )
    eh_map = {r.original_id: r for r in eh_rows}

    columns = [
        "company_id", "企業名", "証券コード",
        "本社都道府県", "代表者名", "従業員数", "企業規模", "業種", "業種詳細", "代表電話番号",
        "en_total_score", "en_review_count", "en_avg_salary", "en_avg_age",
        "en_score_growth", "en_score_advantage", "en_score_meritocracy", "en_score_culture",
        "en_score_youth", "en_score_contribution", "en_score_innovation", "en_score_leadership",
        "en_reviews_text",
        "登録日時",
    ]

    rows = []
    for company in companies:
        cid = str(company.id)
        ci = ci_map.get(cid)
        eh = eh_map.get(cid)

        row = {
            "company_id": cid,
            "企業名": company.name_normalized,
            "証券コード": company.stock_code or "",
            "本社都道府県": getattr(ci, "本社都道府県", "") or "" if ci else "",
            "代表者名": getattr(ci, "代表者名", "") or "" if ci else "",
            "従業員数": getattr(ci, "従業員数", "") or "" if ci else "",
            "企業規模": getattr(ci, "企業規模", "") or "" if ci else "",
            "業種": getattr(ci, "業種", "") or "" if ci else "",
            "業種詳細": getattr(ci, "業種詳細", "") or "" if ci else "",
            "代表電話番号": getattr(ci, "代表電話番号", "") or "" if ci else "",
            "en_total_score": getattr(eh, "total_score", "") or "" if eh else "",
            "en_review_count": getattr(eh, "review_count", "") or "" if eh else "",
            "en_avg_salary": getattr(eh, "avg_salary", "") or "" if eh else "",
            "en_avg_age": getattr(eh, "avg_age", "") or "" if eh else "",
            "en_score_growth": getattr(eh, "score_growth", "") or "" if eh else "",
            "en_score_advantage": getattr(eh, "score_advantage", "") or "" if eh else "",
            "en_score_meritocracy": getattr(eh, "score_meritocracy", "") or "" if eh else "",
            "en_score_culture": getattr(eh, "score_culture", "") or "" if eh else "",
            "en_score_youth": getattr(eh, "score_youth", "") or "" if eh else "",
            "en_score_contribution": getattr(eh, "score_contribution", "") or "" if eh else "",
            "en_score_innovation": getattr(eh, "score_innovation", "") or "" if eh else "",
            "en_score_leadership": getattr(eh, "score_leadership", "") or "" if eh else "",
            "en_reviews_text": getattr(eh, "reviews_text", "") or "" if eh else "",
            "登録日時": company.created_at.strftime("%Y-%m-%d %H:%M") if company.created_at else "",
        }
        rows.append(row)

    _write_csv(output_path, columns, rows)
    return len(rows)


def export_phone_numbers(session, output_path: Path) -> int:
    phones = (
        session.query(PhoneNumber, Company.name_normalized)
        .join(Company, PhoneNumber.company_id == Company.id)
        .order_by(Company.name_normalized, PhoneNumber.number)
        .all()
    )
    columns = [
        "phone_id", "company_name", "phone_number", "label",
        "status", "status_detail", "source", "created_at",
    ]
    rows = [
        {
            "phone_id": str(p.id),
            "company_name": name,
            "phone_number": p.number,
            "label": p.label or "",
            "status": p.status,
            "status_detail": p.status_detail or "",
            "source": p.source or "",
            "created_at": p.created_at.strftime("%Y-%m-%d %H:%M") if p.created_at else "",
        }
        for p, name in phones
    ]
    _write_csv(output_path, columns, rows)
    return len(rows)


def export_persons(session, output_path: Path) -> int:
    persons = (
        session.query(CompanyPerson, Company.name_normalized)
        .join(Company, CompanyPerson.company_id == Company.id)
        .order_by(Company.name_normalized, CompanyPerson.name)
        .all()
    )
    columns = [
        "person_id", "company_name", "person_name", "department",
        "role", "email", "is_decision_maker", "notes", "source", "created_at",
    ]
    rows = [
        {
            "person_id": str(p.id),
            "company_name": name,
            "person_name": p.name,
            "department": p.department or "",
            "role": p.role or "",
            "email": p.email or "",
            "is_decision_maker": "TRUE" if p.is_decision_maker else "FALSE",
            "notes": p.notes or "",
            "source": p.source or "",
            "created_at": p.created_at.strftime("%Y-%m-%d %H:%M") if p.created_at else "",
        }
        for p, name in persons
    ]
    _write_csv(output_path, columns, rows)
    return len(rows)


def export_call_logs(session, output_path: Path) -> int:
    logs = (
        session.query(
            CallLog,
            Company.name_normalized.label("company_name"),
            SalesRep.name.label("rep_name"),
            PhoneNumber.number.label("phone_number"),
            Product.name.label("product_name"),
        )
        .join(Company, CallLog.company_id == Company.id)
        .join(SalesRep, CallLog.sales_rep_id == SalesRep.id)
        .outerjoin(PhoneNumber, CallLog.phone_number_id == PhoneNumber.id)
        .outerjoin(Product, CallLog.product_id == Product.id)
        .order_by(CallLog.called_at.desc())
        .all()
    )
    columns = [
        "call_id", "company_name", "sales_rep_name", "product_name",
        "called_at", "phone_number", "phone_status", "phone_status_memo",
        "discovered_number", "discovered_number_memo",
        "call_result", "spoke_with",
        "discovered_person_chuto", "discovered_person_shinsotsu", "notes",
    ]
    rows = [
        {
            "call_id": str(log.id),
            "company_name": cname,
            "sales_rep_name": rname,
            "product_name": pname or "",
            "called_at": log.called_at.strftime("%Y-%m-%d %H:%M") if log.called_at else "",
            "phone_number": phone or "",
            "phone_status": log.phone_status or "",
            "phone_status_memo": log.phone_status_memo or "",
            "discovered_number": log.discovered_number or "",
            "discovered_number_memo": log.discovered_number_memo or "",
            "call_result": log.call_result or "",
            "spoke_with": log.spoke_with or "",
            "discovered_person_chuto": log.discovered_person_chuto or "",
            "discovered_person_shinsotsu": log.discovered_person_shinsotsu or "",
            "notes": log.notes or "",
        }
        for log, cname, rname, phone, pname in logs
    ]
    _write_csv(output_path, columns, rows)
    return len(rows)


def export_deals(session, output_path: Path) -> int:
    deals = (
        session.query(
            Deal,
            Company.name_normalized.label("company_name"),
            SalesRep.name.label("rep_name"),
            Product.name.label("product_name"),
        )
        .join(Company, Deal.company_id == Company.id)
        .outerjoin(SalesRep, Deal.assigned_rep_id == SalesRep.id)
        .outerjoin(Product, Deal.product_id == Product.id)
        .order_by(Deal.status, Company.name_normalized)
        .all()
    )
    columns = [
        "deal_id", "company_name", "product_name", "assigned_rep",
        "status", "priority", "expected_revenue", "notes", "created_at",
    ]
    rows = [
        {
            "deal_id": str(deal.id),
            "company_name": cname,
            "product_name": pname or "",
            "assigned_rep": rname or "",
            "status": deal.status,
            "priority": str(deal.priority) if deal.priority else "",
            "expected_revenue": str(deal.expected_revenue) if deal.expected_revenue else "",
            "notes": deal.notes or "",
            "created_at": deal.created_at.strftime("%Y-%m-%d %H:%M") if deal.created_at else "",
        }
        for deal, cname, rname, pname in deals
    ]
    _write_csv(output_path, columns, rows)
    return len(rows)


# ---------------------------------------------------------------------------
# 共通CSV書き込み
# ---------------------------------------------------------------------------

def _write_csv(output_path: Path, columns: list[str], rows: list[dict]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"出力: {output_path} ({len(rows)}件)")


# ---------------------------------------------------------------------------
# エントリポイント
# ---------------------------------------------------------------------------

EXPORT_MAP = {
    "companies": ("companies_export.csv", export_companies),
    "phones": ("phone_numbers_export.csv", export_phone_numbers),
    "persons": ("persons_export.csv", export_persons),
    "call_logs": ("call_logs_export.csv", export_call_logs),
    "deals": ("deals_export.csv", export_deals),
}


def main() -> None:
    parser = argparse.ArgumentParser(description="DBデータCSVエクスポート")
    parser.add_argument(
        "--tables",
        nargs="*",
        choices=list(EXPORT_MAP.keys()),
        default=list(EXPORT_MAP.keys()),
        help="エクスポートするテーブル (デフォルト: 全て)",
    )
    parser.add_argument("--output-dir", help="出力先ディレクトリ")
    args = parser.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(str(LOG_DIR / "csv_export.log"), encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(args.output_dir) if args.output_dir else DEFAULT_EXPORT_DIR / timestamp
    output_dir.mkdir(parents=True, exist_ok=True)

    with get_session() as session:
        for key in args.tables:
            filename, func = EXPORT_MAP[key]
            output_path = output_dir / filename
            count = func(session, output_path)
            print(f"  {filename}: {count}件")

    print(f"\nエクスポート完了: {output_dir}")


if __name__ == "__main__":
    main()
