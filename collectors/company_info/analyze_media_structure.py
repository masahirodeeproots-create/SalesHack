import os
import sys
import csv
import json
import time
import logging
import requests
from pathlib import Path
from collections import defaultdict
from bs4 import BeautifulSoup
import google.generativeai as genai
from dotenv import load_dotenv

# プロジェクトルートを sys.path に追加
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config.settings import OUTPUT_DIR, LOG_DIR, DEBUG_DIR

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
SCRAPINGDOG_API_KEY = os.getenv("SCRAPINGDOG_API_KEY")
SCRAPINGDOG_SCRAPE_ENDPOINT = "https://api.scrapingdog.com/scrape"
INPUT_CSV = str(OUTPUT_DIR / "company_media_urls.csv")
REQUEST_INTERVAL = 1.0  # 各HTTPリクエスト間のsleep（秒）
MAX_RETRIES = 3         # ScrapingDog 502エラー時の最大リトライ回数
RETRY_BACKOFF_BASE = 2  # リトライ待機時間の底（秒）: 2^n 秒ずつ増加

# JSレンダリングが必要な媒体（ScrapingDog経由でfetch）
JS_RENDERED_MEDIA = {"マイナビ", "リクルートエージェント", "PR TIMES"}

# ディレクトリが存在しない場合は作成
LOG_DIR.mkdir(parents=True, exist_ok=True)
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(str(LOG_DIR / "analyze_errors.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def load_csv(filepath: str) -> list:
    """CSVを読み込み、status='found'のものだけ返す"""
    rows = []
    with open(filepath, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if row["status"] == "found":
                rows.append(row)
    logger.info(f"CSVロード完了: {len(rows)}件（found）")
    return rows


def fetch_html(url: str) -> str:
    """指定URLのHTMLを直接取得して返す。失敗時はNoneを返す"""
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


def fetch_html_scrapingdog(url: str) -> str:
    """ScrapingDog APIを経由してJSレンダリング済みHTMLを取得する。502エラー時は指数バックオフでリトライ。失敗時はNoneを返す"""
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


def extract_structured_fields(html: str) -> str:
    """
    ページのテンプレートとして定義された構造化フィールドのみを抽出する。
    dl/dt/dd ペア、th付きテーブル行を [FIELD: ラベル] 値 の形式で出力し、
    自由記述テキストは含めない。
    """
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "form", "aside"]):
        tag.decompose()

    main = (
        soup.find("main")
        or soup.find("article")
        or soup.find(id="main")
        or soup.find(id="content")
        or soup.find(class_="main")
        or soup.find(class_="content")
        or soup.body
    )
    if main is None:
        return ""

    lines = []

    # dl/dt/dd ペアを抽出
    for dl in main.find_all("dl"):
        terms = dl.find_all("dt")
        descs = dl.find_all("dd")
        for dt, dd in zip(terms, descs):
            label = dt.get_text(strip=True)
            value = dd.get_text(separator=" ", strip=True)
            if label:
                lines.append(f"[FIELD: {label}] {value}")

    # テーブルの th（見出しセル）付き行を抽出
    for table in main.find_all("table"):
        for row in table.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                label = th.get_text(strip=True)
                value = td.get_text(separator=" ", strip=True)
                if label:
                    lines.append(f"[FIELD: {label}] {value}")

    result = "\n".join(lines)
    return result[:6000]  # Geminiのトークン上限を考慮


def extract_prtimes_content(html: str) -> str:
    """
    PR TIMESのsearchrlpページから企業情報（右カラム）と
    直近プレスリリース3件（タイトル＋公開日）を抽出する。
    """
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "noscript", "header", "footer", "nav"]):
        tag.decompose()

    lines = []

    # 企業情報: dl/dt/dd ペア（右カラムの会社情報など）
    for dl in soup.find_all("dl"):
        terms = dl.find_all("dt")
        descs = dl.find_all("dd")
        for dt, dd in zip(terms, descs):
            label = dt.get_text(strip=True)
            value = dd.get_text(separator=" ", strip=True)
            if label:
                lines.append(f"[FIELD: {label}] {value}")

    # 企業情報: th付きテーブル行
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                label = th.get_text(strip=True)
                value = td.get_text(separator=" ", strip=True)
                if label:
                    lines.append(f"[FIELD: {label}] {value}")

    # プレスリリース一覧: article要素またはリスト要素から直近3件を取得
    pr_count = 0
    for article in soup.find_all("article"):
        if pr_count >= 3:
            break
        title_el = article.find(["h2", "h3", "h4"])
        date_el = article.find("time") or article.find(class_=lambda c: c and "date" in c.lower())
        if title_el:
            title = title_el.get_text(strip=True)
            date = date_el.get_text(strip=True) if date_el else ""
            lines.append(f"[PR: {title} | {date}]")
            pr_count += 1

    result = "\n".join(lines)
    return result[:6000]


def extract_kyujin_urls(html: str) -> list[str]:
    """
    リクルートエージェントの企業詳細ページHTMLから
    求人ページへのリンク（/kensaku/kyujin/xxxxx.html）を抽出する。

    Returns:
        求人ページの完全URLリスト
    """
    soup = BeautifulSoup(html, "html.parser")
    base_url = "https://www.r-agent.com"
    urls = []
    seen = set()

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if "/kensaku/kyujin/" in href and href.endswith(".html"):
            # 相対パスを絶対URLに変換
            if href.startswith("/"):
                full_url = base_url + href
            elif href.startswith("http"):
                full_url = href
            else:
                continue
            if full_url not in seen:
                seen.add(full_url)
                urls.append(full_url)

    logger.info(f"求人URL抽出: {len(urls)}件")
    return urls


def extract_similar_search_fields(html: str) -> dict:
    """
    リクルートエージェントの求人ページHTMLから
    「この求人に似た求人を探す」セクションのテーブルを解析し、
    職種・勤務地・業界（階層分割）・スキル・こだわりを抽出する。

    Returns:
        {
          "職種": "...", "勤務地": "...",
          "業界_階層1": "...", ..., "業界_階層5": "",
          "スキル": "...", "こだわり": "..."
        }
        テーブルが見つからない場合は空dictを返す。
    """
    soup = BeautifulSoup(html, "html.parser")

    # 「この求人に似た求人を探す」の<h2>を探す
    target_h2 = None
    for h2 in soup.find_all("h2"):
        if "この求人に似た求人を探す" in h2.get_text():
            target_h2 = h2
            break

    if target_h2 is None:
        logger.debug("「この求人に似た求人を探す」セクションが見つかりません")
        return {}

    # h2の後にあるtableを探す（兄弟要素を順に探索）
    table = None
    for sibling in target_h2.find_next_siblings():
        if sibling.name == "table":
            table = sibling
            break
        # tableがdiv等に包まれている場合も探す
        found = sibling.find("table")
        if found:
            table = found
            break

    if table is None:
        logger.debug("「この求人に似た求人を探す」テーブルが見つかりません")
        return {}

    # テーブルの各行からth/tdペアを抽出
    raw_fields = {}
    for row in table.find_all("tr"):
        th = row.find("th")
        td = row.find("td")
        if th and td:
            label = th.get_text(strip=True)
            raw_fields[label] = td

    result = {}

    # 業界: <a>タグから階層構造を抽出
    if "業界" in raw_fields:
        td_el = raw_fields["業界"]
        a_tags = td_el.find_all("a")
        hierarchy = [a.get_text(strip=True) for a in a_tags]
        for i in range(5):
            key = f"業界_階層{i + 1}"
            result[key] = hierarchy[i] if i < len(hierarchy) else ""
    else:
        for i in range(5):
            result[f"業界_階層{i + 1}"] = ""

    # 職種・勤務地・スキル・こだわり: テキストを取得
    field_mapping = {
        "職種": "職種_求人",
        "勤務地": "勤務地_求人",
        "スキル": "スキル",
        "こだわり": "こだわり",
    }
    for raw_label, canonical_key in field_mapping.items():
        if raw_label in raw_fields:
            result[canonical_key] = raw_fields[raw_label].get_text(strip=True)
        else:
            result[canonical_key] = ""

    return result


def build_prtimes_prompt(company_texts: dict) -> str:
    """PR TIMES専用のGeminiプロンプトを生成"""
    combined = ""
    for company, text in company_texts.items():
        combined += f"\n\n### {company}\n{text}"

    return f"""あなたはWebスクレイピングの専門家です。
以下は「PR TIMES」における複数企業のページから抽出したデータです。

{combined}

## データの形式
- [FIELD: ラベル名] 値 → ページに明示的に定義された企業情報の構造化フィールド
- [PR: タイトル | 公開日] → 直近のプレスリリースのタイトルと公開日

## 重要な前提
- 分析対象は [FIELD: ...] および [PR: ...] として抽出されたデータのみです
- 企業が自由に記述した内容から項目を推測・追加しないでください

このデータをもとに以下を整理してください。

1. 企業情報として全社に共通して存在する項目一覧
2. 企業によってある場合とない場合がある項目一覧
3. 各項目のページ上での表示形式
4. 取得できた代表的な値のサンプル（企業名付きで3社分程度）
5. プレスリリース欄の構造（タイトルと公開日の形式サンプル）

出力はJSON形式で返してください。
"""


def build_prompt(media_name: str, company_texts: dict) -> str:
    """媒体名と各社テキストからGeminiプロンプトを生成"""
    combined = ""
    for company, text in company_texts.items():
        combined += f"\n\n### {company}\n{text}"

    return f"""あなたはWebスクレイピングの専門家です。
以下は「{media_name}」における複数企業のページから抽出したデータです。
各行は [FIELD: ラベル名] 値 の形式で、ページのHTMLに明示的に定義された構造化フィールドのみを抽出したものです。

{combined}

## 重要な前提
- 分析対象は [FIELD: ...] として抽出されたラベル付き項目のみです
- 会社紹介や事業内容などの自由記述テキストは含まれていません
- 「{media_name}」という媒体がページテンプレートとして設けている項目を整理してください
- 企業が自由に記述した内容から項目を推測・追加しないでください

このデータをもとに以下を整理してください。

1. 全企業に共通して存在する項目一覧（媒体のテンプレートに必須で含まれる項目）
2. 企業によってある場合とない場合がある項目一覧（テンプレートにはあるが任意の項目）
3. 各項目のページ上での表示形式（例：「設立」→「2001年4月」）
4. 取得できた代表的な値のサンプル（企業名付きで3社分程度）

出力はJSON形式で返してください。
"""


def call_gemini(prompt: str) -> dict:
    """Gemini APIにプロンプトを送り、JSON形式のレスポンスを返す"""
    model = genai.GenerativeModel("gemini-2.5-flash")
    response = model.generate_content(
        prompt,
        generation_config=genai.GenerationConfig(response_mime_type="application/json"),
    )
    try:
        return json.loads(response.text)
    except json.JSONDecodeError:
        logger.warning("JSONパース失敗。生テキストをそのまま格納します")
        return {"raw_response": response.text}


def save_json(data: dict, filename: str):
    """辞書をJSONファイルに保存"""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"保存: {filename}")


def main():
    if not GEMINI_API_KEY:
        raise ValueError("GEMINI_API_KEY が .env に設定されていません")
    genai.configure(api_key=GEMINI_API_KEY)

    rows = load_csv(INPUT_CSV)

    # 媒体ごとにグループ化
    media_groups: dict[str, dict[str, str]] = defaultdict(dict)
    for row in rows:
        media_groups[row["媒体名"]][row["企業名"]] = row["URL"]

    for media_name, companies in media_groups.items():
        print(f"\n{'=' * 60}")
        print(f"▶ 媒体: {media_name}（{len(companies)}社）")
        print("=" * 60)

        company_texts = {}
        # リクルートエージェント: 企業詳細ページのHTMLを企業ごとに保持（求人URL抽出用）
        ra_detail_htmls: dict[str, str] = {}

        use_scrapingdog = media_name in JS_RENDERED_MEDIA

        for company, url in companies.items():
            print(f"  [{company}] HTML取得{'(ScrapingDog)' if use_scrapingdog else ''}: {url}")
            html = fetch_html_scrapingdog(url) if use_scrapingdog else fetch_html(url)
            time.sleep(REQUEST_INTERVAL)

            if html is None:
                logger.warning(f"スキップ: {company} / {url}")
                continue

            # リクルートエージェントの場合、詳細ページHTMLを保持
            if media_name == "リクルートエージェント":
                ra_detail_htmls[company] = html

            extract_fn = extract_prtimes_content if media_name == "PR TIMES" else extract_structured_fields
            text = extract_fn(html)
            if not text:
                logger.warning(f"テキスト抽出失敗: {company} / {url}")
                continue

            company_texts[company] = text
            print(f"    ✓ テキスト抽出完了（{len(text)}文字）")

        # --- リクルートエージェント: 求人ページから追加情報を収集 ---
        if media_name == "リクルートエージェント" and ra_detail_htmls:
            print(f"\n  --- 求人ページから追加情報を収集 ---")
            company_kyujin_fields: dict[str, list[dict]] = {}

            for company, detail_html in ra_detail_htmls.items():
                kyujin_urls = extract_kyujin_urls(detail_html)
                if not kyujin_urls:
                    logger.info(f"  [{company}] 求人URLなし")
                    continue

                print(f"  [{company}] 求人URL: {len(kyujin_urls)}件")
                fields_list = []

                for kyujin_url in kyujin_urls:
                    print(f"    求人ページ取得(ScrapingDog): {kyujin_url}")
                    kyujin_html = fetch_html_scrapingdog(kyujin_url)
                    time.sleep(REQUEST_INTERVAL)

                    if kyujin_html is None:
                        logger.warning(f"    求人ページ取得失敗: {kyujin_url}")
                        continue

                    fields = extract_similar_search_fields(kyujin_html)
                    if fields:
                        fields["source_url"] = kyujin_url
                        fields_list.append(fields)
                        print(f"    ✓ フィールド抽出完了: {list(fields.keys())}")
                    else:
                        logger.warning(f"    テーブル抽出失敗: {kyujin_url}")

                if fields_list:
                    company_kyujin_fields[company] = fields_list

            if company_kyujin_fields:
                kyujin_output = str(DEBUG_DIR / "リクルートエージェント_kyujin_fields.json")
                save_json(company_kyujin_fields, kyujin_output)
                print(f"  → {kyujin_output} に保存しました（{len(company_kyujin_fields)}社）")

        if not company_texts:
            logger.warning(f"{media_name}: 有効なテキストなし。スキップ")
            continue

        print(f"  Gemini APIに送信中...")
        prompt = build_prtimes_prompt(company_texts) if media_name == "PR TIMES" else build_prompt(media_name, company_texts)
        try:
            result = call_gemini(prompt)
        except Exception as e:
            logger.error(f"Gemini APIエラー [{media_name}]: {e}")
            continue

        output_filename = str(DEBUG_DIR / f"{media_name}_structure.json")
        save_json(result, output_filename)
        print(f"  → {output_filename} に保存しました")

    print(f"\n全媒体の処理が完了しました。")


if __name__ == "__main__":
    main()
