#!/usr/bin/env python3
"""
run_experiment.py
=================
50社 企業情報収集 実験ランナー

計測項目:
  - 実行時間（全体・ステップ別）
  - ScrapingDog API 使用回数（Google検索 / スクレイピング）
  - Gemini API 使用回数・消費トークン数
  - エラー発生率・発生場所
  - フィールド充填率（ステップ2完了後）
  - 連絡先収集結果（ステップ3）

出力ファイル:
  data/output/company_media_urls.csv      Step1: 媒体URL収集結果
  data/output/company_data_master.csv     Step2: 企業データ収集結果
  data/output/contacts_experiment.csv    Step3: 連絡先収集結果（DB不要）
  data/output/experiment_report.txt      実験レポート
  data/logs/experiment.log               実行ログ

実行方法:
  cd /Users/masahiromatsuyama/Product/企業情報収集
  source venv/bin/activate
  python scripts/run_experiment.py
"""

import csv
import json
import logging
import os
import requests
import sys
import time
from datetime import datetime
from pathlib import Path

# プロジェクトルートを sys.path に追加（scripts/ の親ディレクトリ）
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config.settings import OUTPUT_DIR, LOG_DIR, CHECKPOINT_DIR

LOG_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

# ロギングをパイプラインモジュールのimport前に設定
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(str(LOG_DIR / "experiment.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# 統計トラッカー
# ─────────────────────────────────────────────────────────────────────────────

class _Stats:
    def __init__(self):
        self.sd_google: int = 0         # ScrapingDog Google検索API 呼び出し回数
        self.sd_scrape: int = 0         # ScrapingDog スクレイピングAPI 呼び出し回数
        self.gemini_calls: int = 0      # Gemini generate_content 呼び出し回数
        self.gemini_in_tokens: int = 0  # Gemini 入力トークン合計
        self.gemini_out_tokens: int = 0 # Gemini 出力トークン合計
        self.errors: list = []          # [(step, context, message)]
        self.timings: dict = {}         # step名 -> 秒

    @property
    def sd_total(self) -> int:
        return self.sd_google + self.sd_scrape

    @property
    def gemini_total_tokens(self) -> int:
        return self.gemini_in_tokens + self.gemini_out_tokens


STATS = _Stats()


# ─────────────────────────────────────────────────────────────────────────────
# インスツルメンテーション（APIコール・トークン計測用モンキーパッチ）
# ※ パイプラインモジュールのimportより前に実行すること
# ─────────────────────────────────────────────────────────────────────────────

def _apply_patches():
    # requests.Session.request をパッチ（ScrapingDog APIコール計測）
    import requests as _req
    _orig_session_request = _req.Session.request

    def _patched_session_request(self, method, url, **kwargs):
        url_s = str(url)
        if "scrapingdog.com/google" in url_s:
            STATS.sd_google += 1
        elif "scrapingdog.com/scrape" in url_s:
            STATS.sd_scrape += 1
        return _orig_session_request(self, method, url, **kwargs)

    _req.Session.request = _patched_session_request
    logger.info("requests.Session.request にScrapingDog計測パッチを適用")

    # Gemini GenerativeModel.generate_content をパッチ（トークン計測）
    try:
        import google.generativeai as genai
        _orig_generate = genai.GenerativeModel.generate_content

        def _patched_generate(self, *args, **kwargs):
            resp = _orig_generate(self, *args, **kwargs)
            try:
                um = resp.usage_metadata
                STATS.gemini_in_tokens += um.prompt_token_count or 0
                STATS.gemini_out_tokens += um.candidates_token_count or 0
            except Exception:
                pass
            STATS.gemini_calls += 1
            return resp

        genai.GenerativeModel.generate_content = _patched_generate
        logger.info("Gemini GenerativeModel.generate_content にトークン計測パッチを適用")
    except ImportError:
        logger.warning("google-generativeai が見つかりません。Geminiトークン計測をスキップします。")


# ← パッチを先に適用してからパイプラインモジュールをimport
_apply_patches()

# ─────────────────────────────────────────────────────────────────────────────
# パイプラインモジュール import（パッチ適用後）
# ─────────────────────────────────────────────────────────────────────────────
from collectors.company_info import collect_media_urls as _step1_mod
from collectors.company_info import collect_company_data as _step2_mod
from collectors.contacts.page_fetcher import Snippet
from collectors.contacts.regex_extractor import (
    extract_phones, extract_emails, has_contact_signals,
)
from collectors.contacts.gemini_analyzer import analyze_snippet, merge_results


# ─────────────────────────────────────────────────────────────────────────────
# チェックポイントのクリア（新規実験のため）
# ─────────────────────────────────────────────────────────────────────────────

def _clear_checkpoints():
    for fname in ["company_data_checkpoint.json", "contacts_checkpoint.json"]:
        p = CHECKPOINT_DIR / fname
        if p.exists():
            p.unlink()
            logger.info(f"チェックポイント削除: {fname}")


# ─────────────────────────────────────────────────────────────────────────────
# ステップ 1: 媒体URL収集
# ─────────────────────────────────────────────────────────────────────────────

def run_step1():
    print("\n" + "=" * 70)
    print("【ステップ 1】 媒体URL収集")
    print(f"  対象企業: {len(_step1_mod.COMPANIES)}社")
    print(f"  対象媒体: {list(_step1_mod.MEDIA_CONFIG.keys())}")
    print(f"  合計検索数: {len(_step1_mod.COMPANIES) * len(_step1_mod.MEDIA_CONFIG)}件")
    print("=" * 70)

    t0 = time.time()
    try:
        _step1_mod.main()
    except SystemExit:
        pass
    except Exception as e:
        logger.error(f"ステップ1 例外: {e}", exc_info=True)
        STATS.errors.append(("step1", "main()", str(e)))

    elapsed = time.time() - t0
    STATS.timings["step1_url_collection"] = elapsed
    logger.info(f"ステップ1 完了: {elapsed:.1f}秒")


# ─────────────────────────────────────────────────────────────────────────────
# ステップ 2: 企業データ収集
# ─────────────────────────────────────────────────────────────────────────────

def run_step2():
    print("\n" + "=" * 70)
    print("【ステップ 2】 企業データ収集")
    print("=" * 70)

    t0 = time.time()
    try:
        _step2_mod.main()
    except SystemExit:
        pass
    except Exception as e:
        logger.error(f"ステップ2 例外: {e}", exc_info=True)
        STATS.errors.append(("step2", "main()", str(e)))

    elapsed = time.time() - t0
    STATS.timings["step2_data_collection"] = elapsed
    logger.info(f"ステップ2 完了: {elapsed:.1f}秒")


# ─────────────────────────────────────────────────────────────────────────────
# ステップ 3: 連絡先収集（ScrapingDog Google検索 → スニペット抽出）
# ─────────────────────────────────────────────────────────────────────────────

_CONTACTS_OUTPUT_CSV = str(OUTPUT_DIR / "contacts_experiment.csv")
_CONTACTS_REQUEST_INTERVAL = 3.0  # ScrapingDog API制限対策（秒）
_GEMINI_INTERVAL = 1.5            # Gemini API呼び出し間隔（秒）

# 連絡先収集用Google検索クエリテンプレート
_CONTACT_SEARCH_QUERIES = [
    "{company} 採用担当 電話番号",
    "{company} 人事部 採用窓口 連絡先",
    "{company} 採用 メールアドレス",
]


def _fetch_google_snippets(company_name: str) -> list[Snippet]:
    """ScrapingDog Google検索で1ページ目のスニペットをSnippetリストで返す"""
    api_key = os.getenv("SCRAPINGDOG_API_KEY")
    if not api_key:
        logger.error("SCRAPINGDOG_API_KEY が設定されていません")
        return []

    all_snippets: list[Snippet] = []
    snippet_id = 0

    for query_tmpl in _CONTACT_SEARCH_QUERIES:
        query = query_tmpl.format(company=company_name)
        params = {
            "api_key": api_key,
            "query": query,
            "results": 10,  # 1ページ目全件
            "country": "jp",
        }
        try:
            resp = requests.get(
                "https://api.scrapingdog.com/google",
                params=params, timeout=30,
            )
            if resp.status_code == 200:
                data = resp.json()
                for r in data.get("organic_results", []):
                    title = r.get("title", "")
                    snippet = r.get("snippet", "")
                    url = r.get("link", "")
                    text = f"{title} {snippet}".strip()
                    if len(text) >= 20:
                        all_snippets.append(Snippet(
                            snippet_id=snippet_id,
                            text=text,
                            source_url=url or "google_search",
                            html_tag="search_result",
                        ))
                        snippet_id += 1
            else:
                logger.warning(f"ScrapingDog {resp.status_code}: {query}")
        except Exception as e:
            logger.warning(f"連絡先検索エラー [{company_name}]: {e}")
        time.sleep(_CONTACTS_REQUEST_INTERVAL)

    return all_snippets


def run_step3():
    print("\n" + "=" * 70)
    print("【ステップ 3】 連絡先収集（電話番号・担当者名・メールアドレス）")
    print("  方式: ScrapingDog Google検索 → スニペット抽出（全50社対象）")
    print("=" * 70)

    t0 = time.time()

    companies = _step1_mod.COMPANIES
    logger.info(f"連絡先収集対象: {len(companies)}社（全企業）")
    print(f"  対象企業数: {len(companies)}社")

    all_contacts: list[dict] = []

    for idx, company_name in enumerate(companies, 1):
        print(f"\n  [{idx}/{len(companies)}] {company_name}")
        logger.info(f"連絡先収集: {company_name}")

        try:
            snippets = _fetch_google_snippets(company_name)
            target_snippets = [s for s in snippets if has_contact_signals(s.text)]
            logger.info(
                f"  スニペット: 合計{len(snippets)}件 → "
                f"連絡先シグナルあり: {len(target_snippets)}件"
            )

            if not target_snippets:
                print(f"    → 連絡先情報なし（スニペット{len(snippets)}件中0件に連絡先シグナル）")
                all_contacts.append({
                    "企業名": company_name,
                    "電話番号": "",
                    "担当者名": "",
                    "メールアドレス": "",
                    "電話件数": 0,
                    "担当者件数": 0,
                    "メール件数": 0,
                    "スニペット総数": len(snippets),
                    "連絡先スニペット数": 0,
                    "備考": "連絡先情報なし",
                })
                continue

            # 各スニペットを個別にGeminiで解析
            results = []
            for snippet in target_snippets:
                phones = extract_phones(snippet.text)
                emails = extract_emails(snippet.text)
                result = analyze_snippet(snippet, phones, emails)
                results.append(result)
                time.sleep(_GEMINI_INTERVAL)

            merged = merge_results(results)

            phones_str = "; ".join(
                "{num}({office}{dept})".format(
                    num=p["phone_number"],
                    office=p.get("office_name") or "",
                    dept=p.get("department_name") or "",
                )
                for p in merged["phone_db"]
            )
            persons_str = "; ".join(
                "{name}({dept})".format(
                    name=p["person_name"],
                    dept=p.get("department_name") or "",
                )
                for p in merged["person_db"]
            )
            emails_str = "; ".join(e["email_address"] for e in merged["email_db"])

            n_phones = len(merged["phone_db"])
            n_persons = len(merged["person_db"])
            n_emails = len(merged["email_db"])

            print(f"    → 電話:{n_phones}件 / 担当者:{n_persons}件 / メール:{n_emails}件")

            all_contacts.append({
                "企業名": company_name,
                "電話番号": phones_str,
                "担当者名": persons_str,
                "メールアドレス": emails_str,
                "電話件数": n_phones,
                "担当者件数": n_persons,
                "メール件数": n_emails,
                "スニペット総数": len(snippets),
                "連絡先スニペット数": len(target_snippets),
                "備考": "",
            })

        except Exception as e:
            logger.error(f"  {company_name} 連絡先収集エラー: {e}", exc_info=True)
            STATS.errors.append(("step3", company_name, str(e)))
            all_contacts.append({
                "企業名": company_name,
                "電話番号": "", "担当者名": "", "メールアドレス": "",
                "電話件数": 0, "担当者件数": 0, "メール件数": 0,
                "スニペット総数": 0, "連絡先スニペット数": 0,
                "備考": f"エラー: {e}",
            })

    # CSV 出力
    if all_contacts:
        fieldnames = [
            "企業名", "電話番号", "担当者名", "メールアドレス",
            "電話件数", "担当者件数", "メール件数",
            "スニペット総数", "連絡先スニペット数", "備考",
        ]
        with open(_CONTACTS_OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_contacts)
        logger.info(f"連絡先データ保存: {_CONTACTS_OUTPUT_CSV}")

    elapsed = time.time() - t0
    STATS.timings["step3_contacts"] = elapsed
    logger.info(f"ステップ3 完了: {elapsed:.1f}秒")
    return all_contacts


# ─────────────────────────────────────────────────────────────────────────────
# フィールド充填率の分析
# ─────────────────────────────────────────────────────────────────────────────

def analyze_fill_rates() -> dict:
    master_csv = str(OUTPUT_DIR / "company_data_master.csv")
    if not Path(master_csv).exists():
        return {}

    rows = []
    fieldnames = []
    with open(master_csv, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = [fn for fn in (reader.fieldnames or []) if fn != "企業名"]
        for row in reader:
            rows.append(row)

    if not rows:
        return {}

    total = len(rows)
    fill_rates = {}
    for field in fieldnames:
        filled = sum(1 for row in rows if row.get(field, "").strip())
        fill_rates[field] = {
            "filled": filled,
            "total": total,
            "rate": filled / total,
        }
    return fill_rates


# ─────────────────────────────────────────────────────────────────────────────
# 実験レポート生成
# ─────────────────────────────────────────────────────────────────────────────

def generate_report(fill_rates: dict, contacts: list):
    report_path = str(OUTPUT_DIR / "experiment_report.txt")
    total_elapsed = sum(STATS.timings.values())
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines = [
        "=" * 70,
        "  企業情報収集 実験レポート",
        f"  実行日時: {now_str}",
        f"  対象企業数: {len(_step1_mod.COMPANIES)}社",
        "=" * 70,
        "",
    ]

    # ── 実行時間 ──────────────────────────────────────────────────────────────
    lines.append("【実行時間】")
    step_labels = {
        "step1_url_collection": "Step1 媒体URL収集",
        "step2_data_collection": "Step2 企業データ収集",
        "step3_contacts":       "Step3 連絡先収集",
    }
    for key, label in step_labels.items():
        sec = STATS.timings.get(key, 0)
        m, s = divmod(int(sec), 60)
        lines.append(f"  {label:<25} {m}分{s:02d}秒  ({sec:.1f}秒)")
    m, s = divmod(int(total_elapsed), 60)
    lines.append(f"  {'合計':<25} {m}分{s:02d}秒  ({total_elapsed:.1f}秒)")
    lines.append("")

    # ── API 使用状況 ──────────────────────────────────────────────────────────
    lines.append("【ScrapingDog API 使用回数】")
    lines.append(f"  Google検索API:     {STATS.sd_google}回")
    lines.append(f"  スクレイピングAPI: {STATS.sd_scrape}回")
    lines.append(f"  合計:              {STATS.sd_total}回")
    lines.append("")
    lines.append("【Gemini API 使用状況】")
    lines.append(f"  呼び出し回数: {STATS.gemini_calls}回")
    lines.append(f"  入力トークン: {STATS.gemini_in_tokens:,}")
    lines.append(f"  出力トークン: {STATS.gemini_out_tokens:,}")
    lines.append(f"  合計トークン: {STATS.gemini_total_tokens:,}")
    lines.append("")

    # ── エラー ────────────────────────────────────────────────────────────────
    lines.append("【エラー】")
    if STATS.errors:
        lines.append(f"  エラー件数: {len(STATS.errors)}件")
        for step, ctx, msg in STATS.errors:
            lines.append(f"  [{step}] {ctx}: {msg[:120]}")
    else:
        lines.append("  エラーなし")
    lines.append("")

    # ── Step1 URL収集サマリー ──────────────────────────────────────────────────
    url_csv = str(OUTPUT_DIR / "company_media_urls.csv")
    if Path(url_csv).exists():
        lines.append("【Step1 媒体URL収集サマリー】")
        url_rows = []
        with open(url_csv, encoding="utf-8-sig") as f:
            url_rows = list(csv.DictReader(f))
        found = sum(1 for r in url_rows if r.get("status") == "found")
        total_searches = len(url_rows)
        lines.append(f"  検索総数: {total_searches}件")
        lines.append(f"  URL取得成功: {found}件 / {total_searches}件 ({found/total_searches*100:.1f}%)")

        # 媒体別取得率
        media_stats: dict[str, dict] = {}
        for r in url_rows:
            m = r.get("媒体名", "")
            if m not in media_stats:
                media_stats[m] = {"found": 0, "total": 0}
            media_stats[m]["total"] += 1
            if r.get("status") == "found":
                media_stats[m]["found"] += 1
        for m, s in media_stats.items():
            rate = s["found"] / s["total"] * 100 if s["total"] else 0
            lines.append(f"  {m:<20} {s['found']:>2}/{s['total']} ({rate:.0f}%)")
        lines.append("")

    # ── Step2 フィールド充填率 ─────────────────────────────────────────────────
    if fill_rates:
        lines.append("【Step2 フィールド充填率】")
        lines.append(f"  集計企業数: {list(fill_rates.values())[0]['total']}社")
        lines.append("")

        # カテゴリ別に表示（充填率降順）
        sorted_fields = sorted(fill_rates.items(), key=lambda x: -x[1]["rate"])
        for field_name, data in sorted_fields:
            rate_pct = data["rate"] * 100
            bar_len = int(rate_pct / 5)
            bar = "█" * bar_len + "░" * (20 - bar_len)
            lines.append(
                f"  {field_name:<30} {bar} {data['filled']:>2}/{data['total']} ({rate_pct:5.1f}%)"
            )
        lines.append("")

        # 充填率が低いフィールド（50%未満）
        low_fill = [(f, d) for f, d in fill_rates.items() if d["rate"] < 0.5]
        if low_fill:
            lines.append(f"  ▼ 充填率50%未満のフィールド（要検討: {len(low_fill)}件）")
            for f, d in sorted(low_fill, key=lambda x: x[1]["rate"]):
                lines.append(f"    - {f}: {d['rate']*100:.0f}%")
        lines.append("")

    # ── Step3 連絡先収集サマリー ───────────────────────────────────────────────
    if contacts:
        lines.append("【Step3 連絡先収集サマリー】")
        total_c = len(contacts)
        with_phone = sum(1 for c in contacts if c.get("電話件数", 0) > 0)
        with_person = sum(1 for c in contacts if c.get("担当者件数", 0) > 0)
        with_email = sum(1 for c in contacts if c.get("メール件数", 0) > 0)
        any_contact = sum(
            1 for c in contacts
            if c.get("電話件数", 0) + c.get("担当者件数", 0) + c.get("メール件数", 0) > 0
        )
        lines.append(f"  処理企業数:       {total_c}社")
        lines.append(f"  電話番号取得:     {with_phone}社 ({with_phone/total_c*100:.0f}%)")
        lines.append(f"  担当者名取得:     {with_person}社 ({with_person/total_c*100:.0f}%)")
        lines.append(f"  メール取得:       {with_email}社 ({with_email/total_c*100:.0f}%)")
        lines.append(f"  何らか取得:       {any_contact}社 ({any_contact/total_c*100:.0f}%)")
        lines.append("")

    lines += ["=" * 70, f"  レポート保存: {report_path}", "=" * 70]

    report_text = "\n".join(lines)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_text)

    print("\n" + report_text)
    logger.info(f"実験レポート保存: {report_path}")


# ─────────────────────────────────────────────────────────────────────────────
# メイン
# ─────────────────────────────────────────────────────────────────────────────

def main():
    experiment_start = time.time()

    print("=" * 70)
    print("  企業情報収集 実験ランナー")
    print(f"  対象企業: {len(_step1_mod.COMPANIES)}社")
    print(f"  対象媒体: {len(_step1_mod.MEDIA_CONFIG)}媒体")
    print(f"  開始時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # 新規実験のためチェックポイントをクリア
    _clear_checkpoints()

    # ステップ実行
    run_step1()
    run_step2()
    contacts = run_step3()

    # 分析・レポート生成
    fill_rates = analyze_fill_rates()
    generate_report(fill_rates, contacts)

    total_sec = time.time() - experiment_start
    m, s = divmod(int(total_sec), 60)
    print(f"\n実験完了！ 総実行時間: {m}分{s:02d}秒")


if __name__ == "__main__":
    main()
