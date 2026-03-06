"""
collectors/contacts/run.py
==========================
連絡先情報収集のエントリポイント。

処理フロー:
  1. DB から企業名を取得
  2. ScrapingDog Google検索でスニペット取得
  3. 正規表現で電話番号・メール候補を抽出
  4. 連絡先シグナルがあるスニペットのみ Gemini に投げる
  5. 結果をDBに書き込む
  6. チェックポイントで中断/再開に対応

使い方:
  # 全企業を処理
  python -m collectors.contacts.run

  # 特定企業のみ
  python -m collectors.contacts.run --company-id <UUID>

  # 処理件数を制限
  python -m collectors.contacts.run --limit 50

  # チェックポイントをリセット
  python -m collectors.contacts.run --reset-checkpoint
"""

import argparse
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import UUID

# プロジェクトルートを sys.path に追加
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config.settings import CHECKPOINT_DIR, LOG_DIR
from db.connection import get_session
from db.models import Company
from collectors.contacts.page_fetcher import fetch_google_snippets
from collectors.contacts.regex_extractor import extract_phones, extract_emails, has_contact_signals
from collectors.contacts.gemini_analyzer import (
    analyze_snippet, analyze_snippets_batch, merge_results, GeminiKeyPool,
)
from collectors.contacts.db_writer import write_contact_results

# ---------------------------------------------------------------------------
# ロギング設定
# ---------------------------------------------------------------------------
LOG_DIR.mkdir(parents=True, exist_ok=True)
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(str(LOG_DIR / "contacts.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

CHECKPOINT_FILE = CHECKPOINT_DIR / "contacts_checkpoint.json"
GEMINI_INTERVAL = 1.5  # Gemini APIの呼び出し間隔（秒）
BATCH_SIZE = 3          # 1回のGemini呼び出しで処理するスニペット数

# ---------------------------------------------------------------------------
# API使用量トラッカー
# ---------------------------------------------------------------------------

class _Stats:
    def __init__(self):
        self.sd_google: int = 0
        self.gemini_calls: int = 0
        self.gemini_in_tokens: int = 0
        self.gemini_out_tokens: int = 0

    @property
    def gemini_total_tokens(self) -> int:
        return self.gemini_in_tokens + self.gemini_out_tokens


STATS = _Stats()


def _apply_patches():
    """ScrapingDog・Gemini APIの呼び出し回数・トークン数を計測するパッチを適用する"""
    import requests as _req
    _orig = _req.Session.request

    def _patched(self, method, url, **kwargs):
        if "scrapingdog.com/google" in str(url):
            STATS.sd_google += 1
        return _orig(self, method, url, **kwargs)

    _req.Session.request = _patched

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
        logger.info("API計測パッチ適用（ScrapingDog + Gemini）")
    except ImportError:
        pass

# GeminiキープールはモジュールロードI時に初期化（キーが1つでも動作）
def _build_key_pool() -> GeminiKeyPool:
    from config.settings import GEMINI_API_KEY, GEMINI_API_KEY_2, GEMINI_API_KEY_3
    return GeminiKeyPool([k for k in [GEMINI_API_KEY, GEMINI_API_KEY_2, GEMINI_API_KEY_3] if k])

_KEY_POOL: GeminiKeyPool = _build_key_pool()


# ---------------------------------------------------------------------------
# チェックポイント管理
# ---------------------------------------------------------------------------

def load_checkpoint() -> set[str]:
    """処理済み企業IDのセットを読み込む。"""
    if not CHECKPOINT_FILE.exists():
        return set()
    try:
        data = json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8"))
        return set(data.get("done", []))
    except Exception:
        return set()


def save_checkpoint(done: set[str]) -> None:
    CHECKPOINT_FILE.write_text(
        json.dumps({"done": list(done)}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# 企業一覧の取得
# ---------------------------------------------------------------------------

def _deduplicate_snippets(snippets):
    """
    電話番号が1つだけで同一番号のスニペットを1件に絞る。
    複数電話番号・電話番号なしのスニペットはすべて残す。
    """
    from collectors.contacts.regex_extractor import extract_phones
    single_phone: dict[str, object] = {}
    others = []
    for s in snippets:
        phones = extract_phones(s.text)
        if len(phones) == 1:
            key = phones[0].normalized
            if key not in single_phone:
                single_phone[key] = s
        else:
            others.append(s)
    result = list(single_phone.values())
    result.extend(others)
    return result


def get_companies(session, limit: int | None = None) -> list[tuple[UUID, str]]:
    """(company_id, company_name) のリストを返す。"""
    q = session.query(Company.id, Company.name_normalized)
    if limit:
        q = q.limit(limit)
    return [(row[0], row[1]) for row in q.all()]


# ---------------------------------------------------------------------------
# 1社の処理
# ---------------------------------------------------------------------------

def process_company(
    company_id: UUID,
    company_name: str,
    dry_run: bool = False,
) -> bool:
    """
    1社分の連絡先収集を実行する。
    Returns: 成功なら True
    """
    logger.info(f"処理開始: {company_name}")

    # Google検索でスニペット取得
    snippets = fetch_google_snippets(company_name)
    if not snippets:
        logger.warning(f"  スニペット取得失敗: {company_name}")
        return False

    # 連絡先シグナルがあるスニペットだけ抽出
    target_snippets = [s for s in snippets if has_contact_signals(s.text)]
    logger.info(f"  対象スニペット: {len(target_snippets)}/{len(snippets)}")

    if not target_snippets:
        logger.info(f"  連絡先情報なし: {company_name}")
        return True

    # 重複除去
    deduped = _deduplicate_snippets(target_snippets)
    logger.info(f"  重複除去後: {len(deduped)}件 (元: {len(target_snippets)}件)")

    # 3件ずつバッチに分割して並列Gemini呼び出し
    batches = []
    for i in range(0, len(deduped), BATCH_SIZE):
        b = deduped[i:i + BATCH_SIZE]
        batches.append((
            b,
            [extract_phones(s.text) for s in b],
            [extract_emails(s.text) for s in b],
        ))

    def _worker(args):
        batch, phones_list, emails_list, api_key = args
        return analyze_snippets_batch(batch, phones_list, emails_list, api_key=api_key)

    all_results: list[dict] = []
    with ThreadPoolExecutor(max_workers=max(1, _KEY_POOL.count)) as executor:
        futures = [
            executor.submit(_worker, (batch, phones, emails, _KEY_POOL.get_key()))
            for batch, phones, emails in batches
        ]
        for f in futures:
            try:
                all_results.append(f.result())
            except Exception as e:
                logger.error(f"バッチ処理エラー: {e}")

    time.sleep(GEMINI_INTERVAL)  # 企業間インターバル

    # 結果マージ
    merged = merge_results(all_results)
    logger.info(
        f"  解析完了: phones={len(merged['phone_db'])} "
        f"persons={len(merged['person_db'])} "
        f"emails={len(merged['email_db'])}"
    )

    if dry_run:
        import json as _json
        logger.info(f"  [DRY RUN] 結果:\n{_json.dumps(merged, ensure_ascii=False, indent=2)}")
        return True

    # DB書き込み
    with get_session() as session:
        write_contact_results(session, company_id, merged)

    return True


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="連絡先情報収集")
    parser.add_argument("--company-id", help="特定企業のUUIDを指定")
    parser.add_argument("--limit", type=int, default=None, help="処理件数の上限")
    parser.add_argument("--reset-checkpoint", action="store_true", help="チェックポイントをリセット")
    parser.add_argument("--dry-run", action="store_true", help="DBに書き込まずログ出力のみ")
    args = parser.parse_args()

    _apply_patches()

    if args.reset_checkpoint:
        CHECKPOINT_FILE.unlink(missing_ok=True)
        logger.info("チェックポイントをリセットしました")

    done = load_checkpoint()

    # 処理対象の取得
    if args.company_id:
        with get_session() as session:
            company = session.query(Company).filter_by(id=args.company_id).first()
            if not company:
                logger.error(f"企業が見つかりません: {args.company_id}")
                sys.exit(1)
        targets = [(UUID(args.company_id), company.name_normalized)]
    else:
        with get_session() as session:
            targets = get_companies(session, limit=args.limit)

    logger.info(f"処理対象: {len(targets)} 社")

    success_count = 0
    for company_id, name in targets:
        cid_str = str(company_id)
        if cid_str in done:
            logger.debug(f"スキップ (処理済み): {name}")
            continue

        try:
            ok = process_company(company_id, name, dry_run=args.dry_run)
            if ok:
                done.add(cid_str)
                save_checkpoint(done)
                success_count += 1
        except Exception as e:
            logger.error(f"エラー ({name}): {e}", exc_info=True)

    logger.info(f"完了: {success_count}/{len(targets)} 社")
    logger.info("─" * 50)
    logger.info("【API使用量】")
    logger.info(f"  ScrapingDog Google検索: {STATS.sd_google}回")
    logger.info(f"  Gemini 呼び出し回数:    {STATS.gemini_calls}回")
    logger.info(f"  Gemini 入力トークン:    {STATS.gemini_in_tokens:,}")
    logger.info(f"  Gemini 出力トークン:    {STATS.gemini_out_tokens:,}")
    logger.info(f"  Gemini 合計トークン:    {STATS.gemini_total_tokens:,}")
    logger.info("─" * 50)



if __name__ == "__main__":
    main()
