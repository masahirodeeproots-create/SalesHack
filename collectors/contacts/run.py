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
import os
import sys
import time
from pathlib import Path
from uuid import UUID

# プロジェクトルートを sys.path に追加
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config.settings import CHECKPOINT_DIR, LOG_DIR
from db.connection import get_session
from db.models import Company, PhoneNumber
from collectors.contacts.page_fetcher import fetch_google_snippets
from collectors.contacts.regex_extractor import extract_phones, extract_emails, has_contact_signals
from collectors.contacts.gemini_analyzer import analyze_snippet, merge_results
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

    # Gemini解析
    all_results: list[dict] = []
    for snippet in target_snippets:
        phones = extract_phones(snippet.text)
        emails = extract_emails(snippet.text)

        result = analyze_snippet(snippet, phones, emails)
        all_results.append(result)
        time.sleep(GEMINI_INTERVAL)

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

    # BigQuery アップロード（UPLOAD_TO_BIGQUERY=true の場合）
    if os.getenv("UPLOAD_TO_BIGQUERY", "").lower() == "true":
        try:
            from db.bigquery import upload_contacts
            with get_session() as session:
                phone_rows = session.query(
                    PhoneNumber.company_id,
                    PhoneNumber.number,
                    PhoneNumber.label,
                    PhoneNumber.status,
                    PhoneNumber.source,
                ).all()
            # 企業名を company_id から解決
            with get_session() as session:
                companies = {str(c.id): c.name_normalized for c in session.query(Company).all()}
            bq_rows = [
                {
                    "企業名": companies.get(str(row.company_id), ""),
                    "電話番号": row.number,
                    "ラベル": row.label or "",
                    "status": row.status or "",
                    "source": row.source or "",
                }
                for row in phone_rows
            ]
            upload_contacts(bq_rows)
        except Exception as e:
            logger.error(f"BigQuery アップロード失敗: {e}")


if __name__ == "__main__":
    main()
