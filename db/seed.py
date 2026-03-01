"""
初期データ投入スクリプト
=======================
1. master_fields.json → field_definitions テーブル
2. HRサービス使用状況調査/config.py の SERVICE_REGISTRY → hr_services テーブル

使い方:
    python -m db.seed
"""

import json
import sys
from pathlib import Path

from db.connection import get_session, init_db
from db.models import FieldDefinition, HrService

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def seed_field_definitions() -> int:
    """master_fields.json を field_definitions テーブルに投入"""
    json_path = PROJECT_ROOT / "schemas" / "master_fields.json"
    if not json_path.exists():
        print(f"[SKIP] {json_path} が見つかりません")
        return 0

    with open(json_path, encoding="utf-8") as f:
        schema = json.load(f)

    count = 0
    with get_session() as session:
        for i, field in enumerate(schema["fields"]):
            canonical = field["canonical"]
            exists = (
                session.query(FieldDefinition)
                .filter_by(canonical_name=canonical)
                .first()
            )
            if exists:
                print(f"  [EXISTS] {canonical}")
                continue

            fd = FieldDefinition(
                canonical_name=canonical,
                category=field.get("category", "未分類"),
                data_type="json" if canonical == "プレスリリース" else "text",
                aliases=field.get("aliases", []),
                media_presence=field.get("media_presence", {}),
                source_priority=field.get("source_priority", []),
                note=field.get("note"),
                display_order=i,
            )
            session.add(fd)
            count += 1
            print(f"  [ADD] {canonical}")

    print(f"field_definitions: {count}件追加")
    return count


def seed_hr_services() -> int:
    """SERVICE_REGISTRY の定義を hr_services テーブルに投入"""
    # config.py を直接import せず、定義をここに持つ
    # (将来的に config.py が DB参照に切り替わるため)
    services = {
        "labbase": {"name": "Labbase", "category": "新卒", "base_url": "https://compass.labbase.jp/search", "method": "requests"},
        "talentbook": {"name": "タレントブック", "category": "新卒", "base_url": "https://www.talent-book.jp/companies", "method": "requests"},
        "type_shinsotsu": {"name": "type就活", "category": "新卒", "base_url": "https://typeshukatsu.jp/company/", "method": "requests"},
        "onecareer": {"name": "ワンキャリア", "category": "新卒", "base_url": "https://www.onecareer.jp/events/seminar", "method": "scrapingdog"},
        "levtech_rookie": {"name": "レバテックルーキー", "category": "新卒", "base_url": "https://rookie.levtech.jp/company/", "method": "requests"},
        "bizreach_campus": {"name": "ビズリーチキャンパス", "category": "新卒", "base_url": "https://br-campus.jp/events", "method": "requests"},
        "offerbox": {"name": "オファーボックス", "category": "新卒", "base_url": "https://app.offerbox.jp/v2/scompany", "method": "playwright"},
        "en_tenshoku": {"name": "EN転職", "category": "中途", "base_url": "https://employment.en-japan.com/", "method": "requests"},
        "kimisuka": {"name": "キミスカ", "category": "新卒", "base_url": "https://kimisuka.com/company/case", "method": "requests"},
        "caritasu": {"name": "キャリタス", "category": "新卒", "base_url": "https://job.career-tasu.jp/", "method": "requests"},
        "career_ticket": {"name": "キャリアチケット", "category": "新卒", "base_url": "https://careerticket.jp/industry/", "method": "requests"},
        "bizreach": {"name": "ビズリーチ", "category": "中途", "base_url": "https://www.bizreach.jp/job/", "method": "playwright"},
        "en_ambi": {"name": "アンビ", "category": "中途", "base_url": "https://en-ambi.com/search/", "method": "requests"},
        "type_chuto": {"name": "type中途", "category": "中途", "base_url": "https://type.jp/job/search/", "method": "requests"},
    }

    count = 0
    with get_session() as session:
        for key, cfg in services.items():
            exists = session.query(HrService).filter_by(key=key).first()
            if exists:
                print(f"  [EXISTS] {key}")
                continue

            svc = HrService(
                key=key,
                display_name=cfg["name"],
                category=cfg["category"],
                base_url=cfg["base_url"],
                scrape_method=cfg["method"],
                active=True,
            )
            session.add(svc)
            count += 1
            print(f"  [ADD] {key} ({cfg['name']})")

    print(f"hr_services: {count}件追加")
    return count


def main():
    print("=" * 50)
    print("DB初期化 & シードデータ投入")
    print("=" * 50)

    print("\n--- テーブル作成 ---")
    init_db()
    print("完了")

    print("\n--- field_definitions ---")
    seed_field_definitions()

    print("\n--- hr_services ---")
    seed_hr_services()

    print("\n" + "=" * 50)
    print("完了!")


if __name__ == "__main__":
    main()
