"""
collect_company_data.py
=======================
企業媒体URLから実データを収集し、field_mapper で正規化・統合してCSVに出力するパイプライン。

処理フロー:
  1. company_media_urls.csv から found のURL一覧をロード
  2. 各URLをスクレイピングし、構造化フィールドを抽出
  3. field_mapper で canonical 名にマッピング
  4. 複数媒体の結果を source_priority に従って統合
  5. 企業×フィールドのマトリクスCSVを出力
"""

import os
import sys
import csv
import json
import re
import time
import logging
import requests
from pathlib import Path
from collections import defaultdict
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, Browser

# プロジェクトルートを sys.path に追加
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config.settings import OUTPUT_DIR, LOG_DIR, SCHEMAS_DIR, CHECKPOINT_DIR as _CHECKPOINT_DIR_SETTING, USE_SECRET_MANAGER
from collectors.company_info.field_mapper import map_fields_with_gemini_fallback, merge_multi_source, parse_prtimes_press_releases

load_dotenv()

SCRAPINGDOG_API_KEY = os.getenv("SCRAPINGDOG_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
SCRAPINGDOG_SCRAPE_ENDPOINT = "https://api.scrapingdog.com/scrape"
INPUT_CSV = str(OUTPUT_DIR / "company_media_urls.csv")
OUTPUT_CSV = str(OUTPUT_DIR / "company_data_master.csv")
CHECKPOINT_DIR = str(_CHECKPOINT_DIR_SETTING)
REQUEST_INTERVAL = 2.0
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2

# JSレンダリングが必要な媒体（ScrapingDog dynamic使用）
JS_RENDERED_MEDIA = {"マイナビ", "リクルートエージェント"}
# React SPA等でPlaywrightが必要な媒体
PLAYWRIGHT_MEDIA = {"PR TIMES"}

# ディレクトリが存在しない場合は作成
LOG_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(str(LOG_DIR / "collect_data.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HTML取得
# ---------------------------------------------------------------------------

def fetch_html(url: str) -> str | None:
    """直接HTTPでHTML取得"""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    try:
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
        response.encoding = response.apparent_encoding
        return response.text
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP {e.response.status_code} - {url}")
    except requests.exceptions.RequestException as e:
        logger.error(f"リクエスト失敗 - {url} - {e}")
    return None


def fetch_html_scrapingdog(url: str) -> str | None:
    """ScrapingDog API経由でJSレンダリング済みHTML取得"""
    params = {
        "api_key": SCRAPINGDOG_API_KEY,
        "url": url,
        "dynamic": "true",
    }
    for attempt in range(MAX_RETRIES + 1):
        try:
            response = requests.get(SCRAPINGDOG_SCRAPE_ENDPOINT, params=params, timeout=60)
            if response.status_code == 502 and attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF_BASE ** attempt
                logger.warning(f"ScrapingDog 502 - リトライ {attempt + 1}/{MAX_RETRIES} ({wait}秒後): {url}")
                time.sleep(wait)
                continue
            response.raise_for_status()
            response.encoding = response.apparent_encoding
            return response.text
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 502 and attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF_BASE ** attempt
                logger.warning(f"ScrapingDog 502 - リトライ {attempt + 1}/{MAX_RETRIES} ({wait}秒後): {url}")
                time.sleep(wait)
                continue
            logger.error(f"ScrapingDog HTTP {e.response.status_code} - {url}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"ScrapingDog リクエスト失敗 - {url} - {e}")
            return None
    return None


def fetch_html_playwright(url: str, browser: Browser) -> str | None:
    """Playwrightでブラウザレンダリング済みHTMLを取得（React SPA対応）"""
    try:
        page = browser.new_page()
        page.goto(url, wait_until="networkidle", timeout=30000)
        html = page.content()
        page.close()
        return html
    except Exception as e:
        logger.error(f"Playwright取得失敗 - {url} - {e}")
        try:
            page.close()
        except Exception:
            pass
        return None


# ---------------------------------------------------------------------------
# フィールド抽出
# ---------------------------------------------------------------------------

def _find_content_root(soup: BeautifulSoup) -> "Tag | None":
    """
    ページのメインコンテンツ領域を特定する。
    候補要素にdl/dt/ddまたはth/tdが含まれない場合はフォールバックする。
    """
    candidates = [
        soup.find("main"),
        soup.find("article"),
        soup.find(id="main"),
        soup.find(id="content"),
        soup.find(id="wrapper"),       # マイナビ対応
        soup.find(class_="main"),
        soup.find(class_="content"),
    ]
    for el in candidates:
        if el is None:
            continue
        # 実際にdl/dt/ddまたはth/tdを持つ要素のみ採用
        if el.find("dl") or el.find("th"):
            return el
    # 全候補にデータがなければbodyにフォールバック
    return soup.body


def extract_structured_fields(html: str) -> dict[str, str]:
    """HTMLからdl/dt/ddペアおよびth/tdペアを辞書形式で抽出"""
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "form", "aside"]):
        tag.decompose()

    main = _find_content_root(soup)
    if main is None:
        return {}

    fields = {}

    # dl/dt/dd ペア
    for dl in main.find_all("dl"):
        terms = dl.find_all("dt")
        descs = dl.find_all("dd")
        for dt, dd in zip(terms, descs):
            label = dt.get_text(strip=True)
            value = dd.get_text(separator=" ", strip=True)
            if label and value:
                fields[label] = value

    # テーブルの th/td ペア
    for table in main.find_all("table"):
        for row in table.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                label = th.get_text(strip=True)
                value = td.get_text(separator=" ", strip=True)
                if label and value:
                    fields[label] = value

    return fields


def extract_prtimes_fields(html: str) -> dict[str, str]:
    """PR TIMESの企業ページから企業情報とプレスリリースを抽出"""
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "noscript", "header", "footer", "nav"]):
        tag.decompose()

    fields = {}

    # 企業情報: dl/dt/dd ペア
    for dl in soup.find_all("dl"):
        terms = dl.find_all("dt")
        descs = dl.find_all("dd")
        for dt, dd in zip(terms, descs):
            label = dt.get_text(strip=True)
            value = dd.get_text(separator=" ", strip=True)
            if label and value:
                fields[label] = value

    # 企業情報: th/td テーブル
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                label = th.get_text(strip=True)
                value = td.get_text(separator=" ", strip=True)
                if label and value:
                    fields[label] = value

    # プレスリリース一覧（直近3件）
    press_releases = []
    pr_count = 0
    for article in soup.find_all("article"):
        if pr_count >= 3:
            break
        title_el = article.find(["h2", "h3", "h4"])
        date_el = article.find("time") or article.find(class_=lambda c: c and "date" in c.lower())
        if title_el:
            title = title_el.get_text(strip=True)
            date = date_el.get_text(strip=True) if date_el else ""
            press_releases.append({"title": title, "date": date})
            pr_count += 1

    if press_releases:
        fields["__press_releases__"] = json.dumps(press_releases, ensure_ascii=False)

    return fields


def extract_kyujin_count(html: str) -> str | None:
    """リクルートエージェント企業ページから公開求人数テキストを抽出。
    例: '募集している求人16件' → '16件'。見つからなければ None。"""
    import re
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text()
    patterns = [
        r'募集している求人(\d+)件',
        r'公開求人数[^\d]*(\d+)\s*件',
        r'公開中の求人[^\d]*(\d+)\s*件',
        r'求人数[^\d]*(\d+)\s*件',
    ]
    for pattern in patterns:
        m = re.search(pattern, text)
        if m:
            return m.group(1) + "件"
    return None


def extract_kyujin_urls(html: str) -> list[str]:
    """リクルートエージェント企業ページから求人URLを抽出。
    旧形式 /kensaku/kyujin/ と新形式 /viewjob/ の両方に対応。"""
    soup = BeautifulSoup(html, "html.parser")
    base_url = "https://www.r-agent.com"
    urls = []
    seen = set()
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        is_old = "/kensaku/kyujin/" in href and href.endswith(".html")
        is_new = "/viewjob/" in href
        if is_old or is_new:
            if href.startswith("/"):
                full_url = base_url + href
            elif href.startswith("http"):
                full_url = href
            else:
                continue
            # クエリパラメータを除去して重複チェック
            clean = full_url.split("?")[0].rstrip("/")
            if clean not in seen:
                seen.add(clean)
                urls.append(full_url)
    return urls


def extract_similar_search_fields(html: str) -> dict[str, str]:
    """リクルートエージェント求人ページ（/viewjob/）から求人情報を抽出。

    新サイト構造対応:
    - 想定年収: 給与セル内のテキストから正規表現で抽出
    - 仕事の特徴: span タグのタグリスト（カンマ区切り）
    """
    import re
    soup = BeautifulSoup(html, "html.parser")
    result = {}

    # h3 ラベルの行を持つテーブル（table[0]）を探す
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            h3 = row.find("h3")
            if not h3:
                continue
            label = h3.get_text(strip=True)
            tds = row.find_all("td")
            content_td = tds[1] if len(tds) > 1 else (tds[0] if tds else None)
            if not content_td:
                continue

            # 給与 → 想定年収を抽出
            if label == "給与":
                text = content_td.get_text()
                m = re.search(r"想定年収\s*\n?\s*([\d,]+万円[～〜\-－][\d,]+万円)", text)
                if m:
                    result["想定年収"] = m.group(1)

            # 仕事の特徴 → span タグをカンマ区切り
            if label == "仕事の特徴":
                spans = content_td.find_all("span")
                tags = [s.get_text(strip=True) for s in spans if s.get_text(strip=True)]
                if tags:
                    result["仕事の特徴"] = ", ".join(tags)

    return result


# ---------------------------------------------------------------------------
# みんかぶ（Minkabu）財務データ抽出
# ---------------------------------------------------------------------------

def _load_stock_codes_from_db() -> dict[str, str]:
    """
    DB の companies テーブルから証券コード付き企業を取得する。
    Returns: {name_normalized: stock_code}
    DB が利用できない場合は空辞書を返す。
    """
    try:
        from db.connection import get_session
        from db.models import Company
        with get_session() as session:
            rows = session.query(Company.name_normalized, Company.stock_code).filter(
                Company.stock_code.isnot(None),
                Company.stock_code != "",
            ).all()
        result = {name: code for name, code in rows}
        if result:
            logger.info(f"証券コード: DB から {len(result)} 社取得")
        return result
    except Exception as e:
        logger.debug(f"証券コードのDB取得スキップ: {e}")
        return {}


def _parse_minkabu_table(table, target_metrics: list[str], max_periods: int = 3) -> dict[str, list[tuple[str, str]]]:
    """
    みんかぶのテーブルを解析して、指定メトリクスの値を期別に返す。

    Returns:
        {metric_name: [(period, value), ...]}  ※最新期が先頭
    """
    rows = table.find_all("tr")
    if not rows:
        return {}

    # ヘッダー行からメトリクス名のインデックスを取得
    header_cells = rows[0].find_all("th")
    header_names = [th.get_text(strip=True) for th in header_cells]

    metric_indices: dict[str, int] = {}
    for i, name in enumerate(header_names):
        for target in target_metrics:
            if target in name:
                metric_indices[target] = i
                break

    if not metric_indices:
        return {}

    # データ行（th=期名, td=値）
    result: dict[str, list[tuple[str, str]]] = {m: [] for m in metric_indices}

    for row in rows[1:]:
        th = row.find("th")
        tds = row.find_all("td")
        if not th or not tds:
            continue

        period_text = th.get_text(strip=True)
        # 「2024年6月期(2024/08/14)」→ 「2024年6月期」
        period = re.sub(r"\(.*?\)", "", period_text).strip()

        for metric, idx in metric_indices.items():
            if len(result[metric]) >= max_periods:
                continue  # 3期分取得済みならスキップ
            td_idx = idx - 1  # thが最初の列なのでtdインデックスは-1
            if 0 <= td_idx < len(tds):
                val = tds[td_idx].get_text(strip=True)
                result[metric].append((period, val))

    return result


def extract_minkabu_financial(html: str) -> dict[str, str]:
    """
    みんかぶの決算ページから財務指標を抽出する。
    売上高・営業CF・フリーCF・自己資本率・ROE を複数期分取得し、
    売上成長率も算出する。
    """
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    result = {}

    if len(tables) < 4:
        logger.warning(f"みんかぶ: テーブル数不足 ({len(tables)})")
        return result

    # Table 0: 売上高・営業利益・経常利益・純利益
    revenue_data = _parse_minkabu_table(tables[0], ["売上高", "営業利益"])
    for metric, values in revenue_data.items():
        for period, val in values:
            result[f"minkabu_{metric}_{period}"] = val

    # 売上成長率を算出
    if "売上高" in revenue_data:
        revenues = revenue_data["売上高"]
        for i in range(len(revenues) - 1):
            cur_period, cur_val = revenues[i]
            prev_period, prev_val = revenues[i + 1]
            try:
                cur_num = float(cur_val.replace(",", "").replace("―", "0"))
                prev_num = float(prev_val.replace(",", "").replace("―", "0"))
                if prev_num != 0:
                    growth = (cur_num - prev_num) / abs(prev_num) * 100
                    result[f"minkabu_売上成長率_{cur_period}"] = f"{growth:.1f}%"
            except (ValueError, ZeroDivisionError):
                pass

    # Table 1: 自己資本率
    balance_data = _parse_minkabu_table(tables[1], ["自己資本率"])
    for metric, values in balance_data.items():
        for period, val in values:
            result[f"minkabu_{metric}_{period}"] = val

    # Table 2: ROE
    roe_data = _parse_minkabu_table(tables[2], ["ROE"])
    for metric, values in roe_data.items():
        for period, val in values:
            result[f"minkabu_{metric}_{period}"] = val

    # Table 3: 営業CF・フリーCF
    cf_data = _parse_minkabu_table(tables[3], ["営業CF", "フリーCF"])
    for metric, values in cf_data.items():
        for period, val in values:
            result[f"minkabu_{metric}_{period}"] = val

    return result


# ---------------------------------------------------------------------------
# チェックポイント管理
# ---------------------------------------------------------------------------

def load_checkpoint() -> dict:
    """保存済みチェックポイントをロード"""
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    cp_path = os.path.join(CHECKPOINT_DIR, "company_data_checkpoint.json")
    if os.path.exists(cp_path):
        with open(cp_path, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_checkpoint(data: dict):
    """チェックポイントを保存"""
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    cp_path = os.path.join(CHECKPOINT_DIR, "company_data_checkpoint.json")
    with open(cp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# メインパイプライン
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("企業データ収集パイプライン")
    print("=" * 60)

    # Geminiモデルの準備
    gemini_model = None
    if GEMINI_API_KEY:
        import google.generativeai as genai
        genai.configure(api_key=GEMINI_API_KEY)
        gemini_model = genai.GenerativeModel("gemini-2.5-flash-lite")
        print("Gemini API: 有効（未解決フィールドのフォールバック用）")
    else:
        print("Gemini API: 無効（ルールベースのみ）")

    # CSVロード
    rows = []
    with open(INPUT_CSV, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if row["status"] == "found":
                rows.append(row)
    print(f"対象URL数: {len(rows)}件（found）")

    # 企業→媒体→URL のマッピング
    company_media_urls: dict[str, dict[str, str]] = defaultdict(dict)
    for row in rows:
        company_media_urls[row["企業名"]][row["媒体名"]] = row["URL"]

    print(f"対象企業数: {len(company_media_urls)}社")

    # Playwright ブラウザの起動（PR TIMES用）
    playwright_ctx = None
    pw_browser: Browser | None = None
    need_playwright = any(
        media in PLAYWRIGHT_MEDIA
        for media_urls in company_media_urls.values()
        for media in media_urls
    )
    if need_playwright:
        playwright_ctx = sync_playwright().start()
        pw_browser = playwright_ctx.chromium.launch(headless=True)
        print("Playwright: 起動済み（PR TIMES React SPA対応）")

    # チェックポイントロード
    checkpoint = load_checkpoint()
    # checkpoint structure: { "企業名": { "媒体名": { canonical: value } } }

    try:
        _run_pipeline(company_media_urls, checkpoint, gemini_model, pw_browser)
    finally:
        # Playwright クリーンアップ
        if pw_browser:
            pw_browser.close()
        if playwright_ctx:
            playwright_ctx.stop()
            print("Playwright: 終了")


def _run_pipeline(
    company_media_urls: dict[str, dict[str, str]],
    checkpoint: dict,
    gemini_model,
    pw_browser: Browser | None,
):
    """メインパイプラインの実行（Playwrightライフサイクルから分離）"""

    # 各企業×媒体のスクレイピングとフィールド抽出
    all_company_data: dict[str, dict[str, dict[str, str]]] = {}  # 企業名 → 媒体名 → {canonical: value}

    for company, media_urls in company_media_urls.items():
        print(f"\n{'─' * 50}")
        print(f"▶ {company}（{len(media_urls)}媒体）")

        company_sources = {}

        for media_name, url in media_urls.items():
            # チェックポイント確認
            cp_key = f"{company}|{media_name}"
            if cp_key in checkpoint:
                print(f"  [{media_name}] チェックポイントから復元")
                company_sources[media_name] = checkpoint[cp_key]
                continue

            use_scrapingdog = media_name in JS_RENDERED_MEDIA
            use_playwright = media_name in PLAYWRIGHT_MEDIA
            method = "(Playwright)" if use_playwright else "(ScrapingDog)" if use_scrapingdog else ""
            print(f"  [{media_name}] HTML取得{method}: {url}")

            if use_playwright and pw_browser:
                html = fetch_html_playwright(url, pw_browser)
            elif use_scrapingdog:
                html = fetch_html_scrapingdog(url)
            else:
                html = fetch_html(url)
            time.sleep(REQUEST_INTERVAL)

            if html is None:
                logger.warning(f"  スキップ: {company}/{media_name}")
                continue

            # フィールド抽出
            if media_name == "PR TIMES":
                raw_fields = extract_prtimes_fields(html)
                # プレスリリースは別途処理
                press_releases_json = raw_fields.pop("__press_releases__", "")
            else:
                raw_fields = extract_structured_fields(html)
                press_releases_json = ""

            if not raw_fields:
                logger.warning(f"  フィールド抽出失敗: {company}/{media_name}")
                continue

            print(f"    抽出フィールド数: {len(raw_fields)}件")

            # field_mapper で正規化
            mapped = map_fields_with_gemini_fallback(
                raw_fields, media_name, gemini_model
            )

            # PR TIMESのプレスリリースを追加
            if press_releases_json:
                mapped["プレスリリース"] = press_releases_json

            print(f"    マッピング済み: {len(mapped)}件")

            # リクナビ: 採用情報ページ（給与・福利厚生等）を追加取得
            if media_name == "リクナビ":
                employ_url = url.rstrip("/") + "/employ/"
                print(f"    [リクナビ] 採用情報ページ取得: {employ_url}")
                employ_html = fetch_html(employ_url)
                time.sleep(REQUEST_INTERVAL)
                if employ_html:
                    extra_fields = extract_structured_fields(employ_html)
                    if extra_fields:
                        extra_mapped = map_fields_with_gemini_fallback(extra_fields, media_name, gemini_model)
                        added = 0
                        for k, v in extra_mapped.items():
                            if v and (k not in mapped or not mapped[k]):
                                mapped[k] = v
                                added += 1
                        print(f"    採用情報ページから追加: {added}件")

            company_sources[media_name] = mapped

            # チェックポイント保存
            checkpoint[cp_key] = mapped
            save_checkpoint(checkpoint)

            # リクルートエージェント: 公開求人数を抽出 & 求人情報の追加取得（最初の1件のみ）
            if media_name == "リクルートエージェント":
                # 公開求人数
                kyujin_count = extract_kyujin_count(html)
                if kyujin_count:
                    mapped["リクルートエージェント公開求人数"] = kyujin_count
                    print(f"    公開求人数: {kyujin_count}")

                kyujin_urls = extract_kyujin_urls(html)
                if kyujin_urls:
                    # ページにカウントテキストがなければURL件数で補完
                    if not kyujin_count:
                        mapped["リクルートエージェント公開求人数"] = f"{len(kyujin_urls)}件"
                        print(f"    公開求人数(URLカウント): {len(kyujin_urls)}件")
                    print(f"    求人URL: {len(kyujin_urls)}件 → 最初の1件を取得")
                    kyujin_html = fetch_html_scrapingdog(kyujin_urls[0])
                    time.sleep(REQUEST_INTERVAL)
                    if kyujin_html:
                        kyujin_fields = extract_similar_search_fields(kyujin_html)
                        if kyujin_fields:
                            # 求人フィールドは既にcanonical名なのでそのままマージ
                            for k, v in kyujin_fields.items():
                                if v and k not in mapped:
                                    mapped[k] = v
                            print(f"    求人フィールド追加: {len(kyujin_fields)}件")

                            # チェックポイント更新
                            checkpoint[cp_key] = mapped
                            save_checkpoint(checkpoint)

        all_company_data[company] = company_sources

    # --- みんかぶ財務データ収集 ---
    print(f"\n{'=' * 60}")
    print("みんかぶ 財務データ収集")
    print("=" * 60)

    # DB から証券コードを取得（company_importer.py でインポートされたデータを使用）
    stock_codes = _load_stock_codes_from_db()

    minkabu_data: dict[str, dict[str, str]] = {}
    for company in company_media_urls.keys():
        stock_code = stock_codes.get(company)
        if not stock_code:
            continue

        cp_key = f"{company}|みんかぶ"
        if cp_key in checkpoint:
            print(f"  [{company}] チェックポイントから復元")
            minkabu_data[company] = checkpoint[cp_key]
            continue

        url = f"https://minkabu.jp/stock/{stock_code}/settlement"
        print(f"  [{company}] ({stock_code}) {url}")

        html = fetch_html(url)
        time.sleep(REQUEST_INTERVAL)

        if html is None:
            logger.warning(f"  みんかぶ取得失敗: {company}")
            continue

        financial = extract_minkabu_financial(html)
        if financial:
            minkabu_data[company] = financial
            checkpoint[cp_key] = financial
            save_checkpoint(checkpoint)
            print(f"    ✓ {len(financial)}項目取得")
        else:
            logger.warning(f"  みんかぶ抽出失敗: {company}")

    # 統合: source_priority に従って各企業の最終データを作成
    print(f"\n{'=' * 60}")
    print("データ統合中...")

    final_data: dict[str, dict[str, str]] = {}
    for company, sources in all_company_data.items():
        merged = merge_multi_source(sources)
        # みんかぶ財務データをマージ
        if company in minkabu_data:
            merged.update(minkabu_data[company])
        final_data[company] = merged
        print(f"  {company}: {len(merged)}フィールド")

    # 既存CSVをロード（過去に収集した企業データと今回分をマージするため）
    existing_data: dict[str, dict[str, str]] = {}
    if os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                company_name = row.pop("企業名", "")
                if company_name:
                    existing_data[company_name] = dict(row)
        print(f"既存CSV読み込み: {len(existing_data)}社（蓄積済み）")

    # 今回収集分で既存データを上書きマージ（同名企業は今回収集データが優先）
    merged_all: dict[str, dict[str, str]] = {**existing_data, **final_data}
    new_companies = set(final_data.keys()) - set(existing_data.keys())
    print(f"マージ後合計: {len(merged_all)}社（既存{len(existing_data)}社 + 新規{len(new_companies)}社）")

    # 全企業で使用されているcanonicalフィールドを収集（出力列として使用）
    all_canonicals: set[str] = set()
    for data in merged_all.values():
        all_canonicals.update(data.keys())

    # master_fields.json のカテゴリ順にソート
    schema_path = str(SCHEMAS_DIR / "master_fields.json")
    with open(schema_path, encoding="utf-8") as f:
        master_schema = json.load(f)

    canonical_order = [field["canonical"] for field in master_schema["fields"]]
    # スキーマにある順にソートし、スキーマにないものは末尾に
    sorted_canonicals = [c for c in canonical_order if c in all_canonicals]
    remaining = sorted(all_canonicals - set(sorted_canonicals))
    sorted_canonicals.extend(remaining)

    # CSV出力（蓄積済み全社分を書き込む）
    fieldnames = ["企業名"] + sorted_canonicals
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for company in merged_all.keys():
            row = {"企業名": company}
            for canonical in sorted_canonicals:
                row[canonical] = merged_all[company].get(canonical, "")
            writer.writerow(row)

    # BigQuery アップロード（GCP環境の場合）—蓄積済み全社分を WRITE_TRUNCATE で同期
    if USE_SECRET_MANAGER or os.getenv("UPLOAD_TO_BIGQUERY", "").lower() == "true":
        try:
            from db.bigquery import upload_company_data
            from db.company_resolver import resolve_company_ids
            print(f"\n{'=' * 60}")
            print("BigQuery アップロード中...")
            company_id_map = resolve_company_ids(list(merged_all.keys()))
            upload_company_data(merged_all, sorted_canonicals, list(merged_all.keys()), company_id_map)
        except Exception as e:
            logger.error(f"BigQuery アップロード失敗: {e}")
            print(f"BigQuery アップロード失敗: {e}")
    else:
        print("\nBigQuery アップロード: スキップ（ローカル実行）")

    # サマリー
    print(f"\n{'=' * 60}")
    print(f"完了！結果を {OUTPUT_CSV} に保存しました。")
    print(f"企業数: {len(merged_all)}社（今回収集: {len(final_data)}社）")
    print(f"フィールド数: {len(sorted_canonicals)}項目")

    # フィールドごとの充填率（今回収集分のみ）
    print(f"\n--- フィールド充填率（今回収集分: {len(final_data)}社）---")
    total_companies = len(final_data)
    if total_companies > 0:
        for canonical in sorted_canonicals:
            filled = sum(1 for data in final_data.values() if data.get(canonical))
            rate = filled / total_companies * 100
            print(f"  {canonical}: {filled}/{total_companies} ({rate:.0f}%)")

    print("=" * 60)


if __name__ == "__main__":
    main()
