"""
Gemini エンリッチメント → rawdata_competitors 同期モジュール
==============================================================
各企業の基本情報（業種・所在地・従業員数）をもとに Gemini API へ問い合わせ、
「類似企業①②③」「直接競合企業①②③」を取得して rawdata_competitors に書き込む。

コンテキスト取得元: rawdata_company_info（CSV インポート済みデータ）
rawdata_company_info が存在しない企業はスキップする。

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
from datetime import datetime

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
WAIT_BETWEEN_CALLS = 2.0  # 秒

# ── API使用量トラッカー ──

class _Stats:
    def __init__(self):
        self.gemini_calls: int = 0
        self.gemini_in_tokens: int = 0
        self.gemini_out_tokens: int = 0

    @property
    def gemini_total_tokens(self) -> int:
        return self.gemini_in_tokens + self.gemini_out_tokens


STATS = _Stats()


def _apply_patches():
    """Gemini APIの呼び出し回数・トークン数を計測するパッチを適用する"""
    try:
        import google.generativeai as genai
        _orig_gen = genai.GenerativeModel.generate_content

        def _patched_gen(self, *args, **kwargs):
            resp = _orig_gen(self, *args, **kwargs)
            try:
                um = resp.usage_metadata
                STATS.gemini_in_tokens += um.prompt_token_count or 0
                STATS.gemini_out_tokens += um.candidates_token_count or 0
            except Exception:
                pass
            STATS.gemini_calls += 1
            return resp

        genai.GenerativeModel.generate_content = _patched_gen
        logger.info("API計測パッチ適用（Gemini）")
    except ImportError:
        pass


def _build_prompt(company_name: str, industry: str, location: str, employees: str) -> str:
    """Geminiへ送るプロンプトを構築"""
    context_parts = [f"企業名: {company_name}"]
    if industry:
        context_parts.append(f"業種: {industry}")
    if location:
        context_parts.append(f"本社所在地: {location}")
    if employees:
        context_parts.append(f"従業員数: {employees}")
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
    model = genai.GenerativeModel("gemini-2.5-flash-lite")

    try:
        response = model.generate_content(
            prompt,
            generation_config=genai.GenerationConfig(
                response_mime_type="application/json",
                temperature=0.2,
            ),
        )
        raw = json.loads(response.text)
        result = raw[0] if isinstance(raw, list) else raw

        similar = [str(s).strip() for s in result.get("similar", []) if str(s).strip()][:3]
        competitors = [str(c).strip() for c in result.get("competitors", []) if str(c).strip()][:3]

        return {"similar": similar, "competitors": competitors}

    except json.JSONDecodeError as e:
        logger.warning(f"Gemini レスポンスのJSON解析失敗: {e} / raw: {response.text[:200]}")
        return None
    except Exception as e:
        logger.error(f"Gemini API エラー: {e}")
        return None


def sync_to_db(limit: int = 100, target_company: str | None = None) -> int:
    """
    rawdata_company_info からコンテキストを取得し Gemini でエンリッチ、
    結果を rawdata_competitors に書き込む。
    rawdata_company_info が存在しない企業はスキップ。
    """
    try:
        from db.connection import get_session
        from db.models import Company, RawdataCompanyInfo, RawdataCompetitors
    except ImportError as e:
        logger.error(f"DB接続不可: {e}")
        return 0

    written = 0
    skipped_no_rawdata = 0
    skipped_api_error = 0

    with get_session() as session:
        # 処理済み企業（rawdata_competitors に既に記録あり）を除外
        already_done_ids = {
            row[0]
            for row in session.query(RawdataCompetitors.original_id).all()
        }

        q = session.query(Company)
        if target_company:
            q = q.filter(Company.name_normalized == target_company)

        companies = q.limit(limit * 2).all()  # スキップ分を考慮して多めに取得

        processed = 0
        for company in companies:
            if processed >= limit:
                break

            cid_str = str(company.id)
            if cid_str in already_done_ids and not target_company:
                continue

            logger.info(f"[{processed + 1}] {company.name_normalized}")

            # rawdata_company_info からコンテキスト取得（最新1件）
            rawdata = (
                session.query(RawdataCompanyInfo)
                .filter_by(original_id=cid_str)
                .order_by(RawdataCompanyInfo.scraped_at.desc())
                .first()
            )
            if not rawdata:
                logger.debug(f"  rawdata_company_info なし。スキップ: {company.name_normalized}")
                skipped_no_rawdata += 1
                continue

            industry = rawdata.業種詳細 or rawdata.業種 or ""
            location = rawdata.本社都道府県 or ""
            employees = rawdata.従業員数 or ""

            prompt = _build_prompt(
                company_name=company.name_normalized,
                industry=industry,
                location=location,
                employees=employees,
            )

            result = _call_gemini(prompt)
            if not result:
                logger.warning("  → Gemini 取得失敗。スキップ。")
                skipped_api_error += 1
                time.sleep(WAIT_BETWEEN_CALLS)
                processed += 1
                continue

            similar = result["similar"]
            competitors = result["competitors"]

            session.add(RawdataCompetitors(
                original_id=cid_str,
                類似企業1=similar[0] if len(similar) > 0 else None,
                類似企業2=similar[1] if len(similar) > 1 else None,
                類似企業3=similar[2] if len(similar) > 2 else None,
                競合企業1=competitors[0] if len(competitors) > 0 else None,
                競合企業2=competitors[1] if len(competitors) > 1 else None,
                競合企業3=competitors[2] if len(competitors) > 2 else None,
            ))
            session.flush()
            written += 1

            logger.info(f"  類似: {similar}")
            logger.info(f"  競合: {competitors}")

            processed += 1
            if processed < limit:
                time.sleep(WAIT_BETWEEN_CALLS)

    logger.info(
        f"完了: {written}社書き込み / "
        f"{skipped_no_rawdata}社スキップ(rawdataなし) / "
        f"{skipped_api_error}社スキップ(APIエラー)"
    )
    return written


def run(limit: int = 100, target_company: str | None = None):
    logger.info("=" * 60)
    logger.info("Gemini エンリッチメント 同期開始")
    if target_company:
        logger.info(f"対象企業: {target_company}")
    else:
        logger.info(f"上限: {limit}社")
    logger.info("=" * 60)

    _apply_patches()

    start = datetime.now()
    written = sync_to_db(limit=limit, target_company=target_company)

    elapsed = (datetime.now() - start).total_seconds()
    logger.info(f"同期完了: {written}社書き込み | 所要時間: {elapsed:.1f}秒")
    logger.info("─" * 50)
    logger.info("【API使用量】")
    logger.info(f"  Gemini 呼び出し回数: {STATS.gemini_calls}回")
    logger.info(f"  Gemini 入力トークン: {STATS.gemini_in_tokens:,}")
    logger.info(f"  Gemini 出力トークン: {STATS.gemini_out_tokens:,}")
    logger.info(f"  Gemini 合計トークン: {STATS.gemini_total_tokens:,}")
    logger.info("─" * 50)
    logger.info(f"ログ: {log_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Geminiで類似・競合企業を rawdata_competitors に同期")
    parser.add_argument("--limit", type=int, default=100,
                        help="1回の実行で処理する企業数の上限（デフォルト: 100）")
    parser.add_argument("--company", type=str, default=None,
                        help="特定企業のみ処理（name_normalized で一致）")
    args = parser.parse_args()
    run(limit=args.limit, target_company=args.company)
