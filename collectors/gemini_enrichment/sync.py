"""
Gemini エンリッチメント → 企業情報収集DB 同期モジュール
=========================================================
各企業の基本情報（業種・所在地・従業員数）をもとに Gemini API へ問い合わせ、
「類似企業①②③（同規模・同エリア・同業）」「直接競合企業①②③」を取得して
company_field_values に書き込む（1フィールド1社名）。

実行例:
    python -m collectors.gemini_enrichment.sync
    python -m collectors.gemini_enrichment.sync --limit 50
    python -m collectors.gemini_enrichment.sync --company "株式会社フリー"
"""

import json
import logging
import sys
import time
import argparse
from pathlib import Path
from datetime import datetime, timezone

# ── プロジェクトルートをパスに追加 ──
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import LOG_DIR, GEMINI_API_KEY

# ── ログ設定 ──
LOG_DIR.mkdir(parents=True, exist_ok=True)
log_file = LOG_DIR / f"gemini_enrichment_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(str(log_file), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# ── 定数 ──
SOURCE_NAME = "Gemini"
NUM = ["①", "②", "③"]

FIELDS_SIMILAR = [f"類似企業{n}（同規模・同エリア・同業）" for n in NUM]
FIELDS_COMPETITOR = [f"直接競合企業{n}" for n in NUM]

# Gemini API のレート制限対策（APIキー3つのラウンドロビン運用: 実質45 RPM）
WAIT_BETWEEN_CALLS = 2.0  # 秒


def _build_prompt(company_name: str, industry: str, location: str, employees: str, business: str) -> str:
    """Geminiへ送るプロンプトを構築"""
    context_parts = [f"企業名: {company_name}"]
    if industry:
        context_parts.append(f"業種: {industry}")
    if location:
        context_parts.append(f"本社所在地: {location}")
    if employees:
        context_parts.append(f"従業員数: {employees}")
    if business:
        context_parts.append(f"事業内容: {business[:300]}")  # 長すぎる場合は先頭300字
    context = "\n".join(context_parts)

    return f"""{context}

上記の企業について、以下の2項目を日本企業の正式名称で答えてください。
各項目は必ず3社ずつ、実在する企業名のみを返してください。

{{
  "similar": ["同規模・同エリア・同業の類似企業1", "類似企業2", "類似企業3"],
  "competitors": ["直接競合の上位企業1", "競合企業2", "競合企業3"]
}}

JSONのみを返してください。説明文は不要です。"""


def _call_gemini(prompt: str) -> dict | None:
    """Gemini API を呼び出して similar/competitors を返す"""
    try:
        import google.generativeai as genai
    except ImportError:
        logger.error("google-generativeai がインストールされていません: pip install google-generativeai")
        return None

    if not GEMINI_API_KEY:
        logger.error("GEMINI_API_KEY が設定されていません")
        return None

    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel("gemini-2.0-flash")

    try:
        response = model.generate_content(
            prompt,
            generation_config=genai.GenerationConfig(
                response_mime_type="application/json",
                temperature=0.2,
            ),
        )
        raw = json.loads(response.text)
        # レスポンスがリスト形式の場合は先頭要素を取得
        result = raw[0] if isinstance(raw, list) else raw

        similar = result.get("similar", [])
        competitors = result.get("competitors", [])

        # 3社に揃える（多すぎる場合は切り捨て、空文字除去）
        similar = [str(s).strip() for s in similar if str(s).strip()][:3]
        competitors = [str(c).strip() for c in competitors if str(c).strip()][:3]

        return {"similar": similar, "competitors": competitors}

    except json.JSONDecodeError as e:
        logger.warning(f"Gemini レスポンスのJSON解析失敗: {e} / raw: {response.text[:200]}")
        return None
    except Exception as e:
        logger.error(f"Gemini API エラー: {e}")
        return None


def _upsert_field_value(session, company_id, field_def, value: str):
    """company_field_values に UPSERT し、履歴も追記する"""
    from db.models import CompanyFieldValue, CompanyFieldValueHistory

    now = datetime.now(timezone.utc)
    existing = session.query(CompanyFieldValue).filter_by(
        company_id=company_id,
        field_id=field_def.id,
    ).first()

    if existing:
        existing.value = value
        existing.source = SOURCE_NAME
        existing.scraped_at = now
    else:
        session.add(CompanyFieldValue(
            company_id=company_id,
            field_id=field_def.id,
            value=value,
            source=SOURCE_NAME,
        ))

    session.add(CompanyFieldValueHistory(
        company_id=company_id,
        field_id=field_def.id,
        value=value,
        source=SOURCE_NAME,
    ))


def sync_to_db(limit: int = 100, target_company: str | None = None) -> int:
    """DBから企業を取得して Gemini でエンリッチし、結果をDBに書き込む"""
    try:
        from db.connection import get_session
        from db.models import Company, FieldDefinition, CompanyFieldValue
    except ImportError as e:
        logger.error(f"DB接続不可: {e}")
        return 0

    written = 0
    skipped = 0

    with get_session() as session:
        # field_definitions をキャッシュ（6フィールド）
        fd_similar = []
        fd_competitor = []
        for canonical in FIELDS_SIMILAR:
            fd = session.query(FieldDefinition).filter_by(canonical_name=canonical).first()
            if not fd:
                logger.error(f"field_definitions 未登録: {canonical} / `python -m db.seed` を実行してください。")
                return 0
            fd_similar.append(fd)
        for canonical in FIELDS_COMPETITOR:
            fd = session.query(FieldDefinition).filter_by(canonical_name=canonical).first()
            if not fd:
                logger.error(f"field_definitions 未登録: {canonical} / `python -m db.seed` を実行してください。")
                return 0
            fd_competitor.append(fd)

        # 取得対象企業を絞り込む
        q = session.query(Company)
        if target_company:
            q = q.filter(Company.name_normalized == target_company)
        else:
            # 未取得の企業のみ（類似企業①が埋まっている企業はスキップ）
            already_done = session.query(CompanyFieldValue.company_id).filter(
                CompanyFieldValue.field_id == fd_similar[0].id
            ).subquery()
            q = q.filter(~Company.id.in_(already_done))

        companies = q.limit(limit).all()
        logger.info(f"エンリッチ対象: {len(companies)}社")

        for i, company in enumerate(companies, 1):
            logger.info(f"[{i}/{len(companies)}] {company.name_normalized}")

            # 既存フィールド値から業種・所在地・従業員数を取得
            def get_field_value(canonical: str) -> str:
                fd = session.query(FieldDefinition).filter_by(canonical_name=canonical).first()
                if not fd:
                    return ""
                cfv = session.query(CompanyFieldValue).filter_by(
                    company_id=company.id, field_id=fd.id
                ).first()
                return cfv.value if cfv else ""

            industry  = get_field_value("業種")
            location  = get_field_value("本社所在地")
            employees = get_field_value("従業員数")
            business  = get_field_value("事業内容")

            prompt = _build_prompt(
                company_name=company.name_normalized,
                industry=industry,
                location=location,
                employees=employees,
                business=business,
            )

            result = _call_gemini(prompt)
            if not result:
                logger.warning("  → Gemini 取得失敗。スキップ。")
                skipped += 1
                time.sleep(WAIT_BETWEEN_CALLS)
                continue

            # 類似企業①②③ を個別フィールドに書き込む
            for j, name in enumerate(result["similar"]):
                _upsert_field_value(session, company.id, fd_similar[j], name)
                written += 1
                logger.info(f"  類似企業{NUM[j]}: {name}")

            # 直接競合企業①②③ を個別フィールドに書き込む
            for j, name in enumerate(result["competitors"]):
                _upsert_field_value(session, company.id, fd_competitor[j], name)
                written += 1
                logger.info(f"  競合企業{NUM[j]}: {name}")

            # Gemini レート制限対策
            if i < len(companies):
                time.sleep(WAIT_BETWEEN_CALLS)

    logger.info(f"完了: {written}件書き込み / {skipped}社スキップ")
    return written


def run(limit: int = 100, target_company: str | None = None):
    logger.info("=" * 60)
    logger.info("Gemini エンリッチメント 同期開始")
    if target_company:
        logger.info(f"対象企業: {target_company}")
    else:
        logger.info(f"上限: {limit}社")
    logger.info("=" * 60)

    start = datetime.now()
    written = sync_to_db(limit=limit, target_company=target_company)

    elapsed = (datetime.now() - start).total_seconds()
    logger.info(f"同期完了: {written}フィールド書き込み | 所要時間: {elapsed:.1f}秒")
    logger.info(f"ログ: {log_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Geminiで類似・競合企業をDBに同期")
    parser.add_argument("--limit", type=int, default=100,
                        help="1回の実行で処理する企業数の上限（デフォルト: 100）")
    parser.add_argument("--company", type=str, default=None,
                        help="特定企業のみ処理（name_normalized で一致）")
    args = parser.parse_args()
    run(limit=args.limit, target_company=args.company)
