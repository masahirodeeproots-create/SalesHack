"""各スクレイパーのテスト実行（各サービス50社制限）"""

import sys
import csv
import time
import logging
from pathlib import Path

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).parent))

from config import OUTPUT_DIR, LOG_DIR, CSV_ENCODING, CSV_COLUMNS
from http_client import HttpClient

LOG_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "test_scrapers.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("test")

MAX_RESULTS = 50  # 各サービスの上限


def save_results(service_name: str, results: list[dict], filename: str):
    """テスト結果をCSVに保存"""
    output_path = OUTPUT_DIR / filename
    with open(output_path, "w", newline="", encoding=CSV_ENCODING) as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(results[:MAX_RESULTS])
    return output_path


def test_requests_scraper(name: str, urls: list[str], parse_fn, client: HttpClient) -> list[dict]:
    """requests系スクレイパーを1-3ページ分テスト"""
    results = []
    for url in urls:
        logger.info(f"  Fetching: {url}")
        html = client.fetch_requests(url)
        if html is None:
            logger.warning(f"  取得失敗: {url}")
            continue
        items = parse_fn(html)
        logger.info(f"  → {len(items)}件取得")
        results.extend(items)
        if len(results) >= MAX_RESULTS:
            break
        client.sleep()
    return results[:MAX_RESULTS]


def test_scrapingdog_scraper(name: str, urls: list[str], parse_fn, client: HttpClient) -> list[dict]:
    """ScrapingDog系スクレイパーを1ページ分テスト"""
    results = []
    for url in urls[:1]:  # APIクレジット節約: 1ページのみ
        logger.info(f"  Fetching (ScrapingDog): {url}")
        html = client.fetch_scrapingdog(url, dynamic=True)
        if html is None:
            logger.warning(f"  取得失敗: {url}")
            continue
        items = parse_fn(html)
        logger.info(f"  → {len(items)}件取得")
        results.extend(items)
        client.sleep()
    return results[:MAX_RESULTS]


def main():
    client = HttpClient()
    summary = []

    print("\n" + "=" * 70)
    print("HRサービス スクレイパー テスト実行（各50社制限）")
    print("=" * 70)

    # =============================================
    # 1. キャリアチケット (requests, 業種ループ)
    # =============================================
    print("\n▶ [1/14] キャリアチケット")
    try:
        from scrapers.career_ticket import Scraper as S1
        s = S1(client)
        urls = [
            "https://careerticket.jp/industry/1/",
            "https://careerticket.jp/industry/2/",
            "https://careerticket.jp/industry/3/",
        ]
        results = test_requests_scraper("career_ticket", urls, s._parse_page, client)
        path = save_results("career_ticket", results, "career_ticket.csv")
        summary.append(("キャリアチケット", len(results), "✓" if results else "✗", str(path)))
        if results:
            for r in results[:3]:
                print(f"    {r['企業名']}")
    except Exception as e:
        logger.error(f"  エラー: {e}", exc_info=True)
        summary.append(("キャリアチケット", 0, f"✗ {e}", ""))

    # =============================================
    # 2. type就活 (requests, POST)
    # =============================================
    print("\n▶ [2/14] type就活")
    try:
        from scrapers.type_shinsotsu import Scraper as S2
        s = S2(client)
        urls = ["https://typeshukatsu.jp/company/"]
        results = test_requests_scraper("type_shinsotsu", urls, s._parse_page, client)
        path = save_results("type_shinsotsu", results, "type_shinsotsu.csv")
        summary.append(("type就活", len(results), "✓" if results else "✗", str(path)))
        if results:
            for r in results[:3]:
                print(f"    {r['企業名']}")
    except Exception as e:
        logger.error(f"  エラー: {e}", exc_info=True)
        summary.append(("type就活", 0, f"✗ {e}", ""))

    # =============================================
    # 3. キャリタス (requests)
    # =============================================
    print("\n▶ [3/14] キャリタス")
    try:
        from scrapers.caritasu import Scraper as S3
        s = S3(client)
        urls = [
            "https://job.career-tasu.jp/condition-search/result/?corpOtherCheckCd=04-%E3%82%AD%E3%83%A3%E3%83%AA%E3%82%BF%E3%82%B9%E9%99%90%E5%AE%9A%E6%83%85%E5%A0%B1%E3%81%82%E3%82%8A&p=1",
            "https://job.career-tasu.jp/condition-search/result/?corpOtherCheckCd=04-%E3%82%AD%E3%83%A3%E3%83%AA%E3%82%BF%E3%82%B9%E9%99%90%E5%AE%9A%E6%83%85%E5%A0%B1%E3%81%82%E3%82%8A&p=2",
        ]
        results = test_requests_scraper("caritasu", urls, s._parse_page, client)
        path = save_results("caritasu", results, "caritasu.csv")
        summary.append(("キャリタス", len(results), "✓" if results else "✗", str(path)))
        if results:
            for r in results[:3]:
                print(f"    {r['企業名']}")
    except Exception as e:
        logger.error(f"  エラー: {e}", exc_info=True)
        summary.append(("キャリタス", 0, f"✗ {e}", ""))

    # =============================================
    # 4. EN転職 (requests)
    # =============================================
    print("\n▶ [4/14] EN転職")
    try:
        from scrapers.en_tenshoku import Scraper as S4
        s = S4(client)
        urls = [
            "https://employment.en-japan.com/a/kanto/s_setsubikanri-unyu/?caroute=1101&PK=F4DF97",
            "https://employment.en-japan.com/a/kanto/s_setsubikanri-unyu/2/?caroute=1101&PK=F4DF97",
        ]
        results = test_requests_scraper("en_tenshoku", urls, s._parse_page, client)
        path = save_results("en_tenshoku", results, "en_tenshoku.csv")
        summary.append(("EN転職", len(results), "✓" if results else "✗", str(path)))
        if results:
            for r in results[:3]:
                print(f"    {r['企業名']} | {r['タイトル'][:30]}")
    except Exception as e:
        logger.error(f"  エラー: {e}", exc_info=True)
        summary.append(("EN転職", 0, f"✗ {e}", ""))

    # =============================================
    # 5. アンビ (requests)
    # =============================================
    print("\n▶ [5/14] アンビ")
    try:
        from scrapers.en_ambi import Scraper as S5
        s = S5(client)
        urls = [
            "https://en-ambi.com/search/?jobmerit=350&krt=top",
            "https://en-ambi.com/search/?jobmerit=350&krt=top&per_page=2",
        ]
        results = test_requests_scraper("en_ambi", urls, s._parse_page, client)
        path = save_results("en_ambi", results, "en_ambi.csv")
        summary.append(("アンビ", len(results), "✓" if results else "✗", str(path)))
        if results:
            for r in results[:3]:
                print(f"    {r['企業名']}")
    except Exception as e:
        logger.error(f"  エラー: {e}", exc_info=True)
        summary.append(("アンビ", 0, f"✗ {e}", ""))

    # =============================================
    # 6. Labbase (requests, Next.js)
    # =============================================
    print("\n▶ [6/14] Labbase")
    try:
        from scrapers.labbase import Scraper as S6
        s = S6(client)
        urls = [
            "https://compass.labbase.jp/search?mode=default&page=0",
            "https://compass.labbase.jp/search?mode=default&page=1",
            "https://compass.labbase.jp/search?mode=default&page=2",
        ]
        results = test_requests_scraper("labbase", urls, s._parse_page, client)
        path = save_results("labbase", results, "labbase.csv")
        summary.append(("Labbase", len(results), "✓" if results else "✗", str(path)))
        if results:
            for r in results[:3]:
                print(f"    {r['企業名']}")
    except Exception as e:
        logger.error(f"  エラー: {e}", exc_info=True)
        summary.append(("Labbase", 0, f"✗ {e}", ""))

    # =============================================
    # 7. タレントブック (requests, Next.js)
    # =============================================
    print("\n▶ [7/14] タレントブック")
    try:
        from scrapers.talentbook import Scraper as S7
        s = S7(client)
        urls = [
            "https://www.talent-book.jp/companies",
            "https://www.talent-book.jp/companies?page=2",
        ]
        results = test_requests_scraper("talentbook", urls, s._parse_page, client)
        path = save_results("talentbook", results, "talentbook.csv")
        summary.append(("タレントブック", len(results), "✓" if results else "✗", str(path)))
        if results:
            for r in results[:3]:
                print(f"    {r['企業名']}")
    except Exception as e:
        logger.error(f"  エラー: {e}", exc_info=True)
        summary.append(("タレントブック", 0, f"✗ {e}", ""))

    # =============================================
    # 8. キミスカ (requests, Next.js)
    # =============================================
    print("\n▶ [8/14] キミスカ")
    try:
        from scrapers.kimisuka import Scraper as S8
        s = S8(client)
        urls = [
            "https://kimisuka.com/company/case",
            "https://kimisuka.com/company/case?page=2",
        ]
        results = test_requests_scraper("kimisuka", urls, s._parse_page, client)
        path = save_results("kimisuka", results, "kimisuka.csv")
        summary.append(("キミスカ", len(results), "✓" if results else "✗", str(path)))
        if results:
            for r in results[:3]:
                print(f"    {r['企業名']}")
    except Exception as e:
        logger.error(f"  エラー: {e}", exc_info=True)
        summary.append(("キミスカ", 0, f"✗ {e}", ""))

    # =============================================
    # 9. type中途 (requests, offset)
    # =============================================
    print("\n▶ [9/14] type中途")
    try:
        from scrapers.type_chuto import Scraper as S9
        s = S9(client)
        urls = [
            "https://type.jp/job/search/?pathway=4&offset=0",
        ]
        results = test_requests_scraper("type_chuto", urls, s._parse_page, client)
        path = save_results("type_chuto", results, "type_chuto.csv")
        summary.append(("type中途", len(results), "✓" if results else "✗", str(path)))
        if results:
            for r in results[:3]:
                print(f"    {r['企業名']} | {r['タイトル'][:30]}")
    except Exception as e:
        logger.error(f"  エラー: {e}", exc_info=True)
        summary.append(("type中途", 0, f"✗ {e}", ""))

    # =============================================
    # 10. ワンキャリア (ScrapingDog)
    # =============================================
    print("\n▶ [10/14] ワンキャリア (ScrapingDog)")
    try:
        from scrapers.onecareer import Scraper as S10
        s = S10(client)
        urls = ["https://www.onecareer.jp/events/seminar?page=1&per=30"]
        results = test_scrapingdog_scraper("onecareer", urls, s._parse_page, client)
        path = save_results("onecareer", results, "onecareer.csv")
        summary.append(("ワンキャリア", len(results), "✓" if results else "✗", str(path)))
        if results:
            for r in results[:3]:
                print(f"    {r['企業名']}")
    except Exception as e:
        logger.error(f"  エラー: {e}", exc_info=True)
        summary.append(("ワンキャリア", 0, f"✗ {e}", ""))

    # =============================================
    # 11. ビズリーチキャンパス (ScrapingDog)
    # =============================================
    print("\n▶ [11/14] ビズリーチキャンパス (ScrapingDog)")
    try:
        from scrapers.bizreach_campus import Scraper as S11
        s = S11(client)
        urls = ["https://br-campus.jp/events"]
        results = test_scrapingdog_scraper("bizreach_campus", urls, s._parse_page, client)
        path = save_results("bizreach_campus", results, "bizreach_campus.csv")
        summary.append(("ビズリーチキャンパス", len(results), "✓" if results else "✗", str(path)))
        if results:
            for r in results[:3]:
                print(f"    {r['企業名']}")
    except Exception as e:
        logger.error(f"  エラー: {e}", exc_info=True)
        summary.append(("ビズリーチキャンパス", 0, f"✗ {e}", ""))

    # =============================================
    # 12. レバテックルーキー (ScrapingDog)
    # =============================================
    print("\n▶ [12/14] レバテックルーキー (ScrapingDog)")
    try:
        from scrapers.levtech_rookie import Scraper as S12
        s = S12(client)
        urls = ["https://rookie.levtech.jp/company/?page=1"]
        results = test_scrapingdog_scraper("levtech_rookie", urls, s._parse_page, client)
        path = save_results("levtech_rookie", results, "levtech_rookie.csv")
        summary.append(("レバテックルーキー", len(results), "✓" if results else "✗", str(path)))
        if results:
            for r in results[:3]:
                print(f"    {r['企業名']}")
    except Exception as e:
        logger.error(f"  エラー: {e}", exc_info=True)
        summary.append(("レバテックルーキー", 0, f"✗ {e}", ""))

    # =============================================
    # 13. オファーボックス (Playwright - スキップ可)
    # =============================================
    print("\n▶ [13/14] オファーボックス (Playwright)")
    try:
        from scrapers.offerbox import Scraper as S13
        s = S13()
        # Playwright は非同期で full scrape するが、50件で十分
        # テストのため直接実行
        import asyncio
        from playwright.async_api import async_playwright
        import os

        async def test_offerbox():
            email = os.getenv("OFFERBOX_EMAIL")
            password = os.getenv("OFFERBOX_PASSWORD")
            if not email:
                return []

            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
                )
                page = await context.new_page()
                try:
                    await page.goto("https://app.offerbox.jp/", wait_until="networkidle", timeout=30000)
                    await page.wait_for_timeout(2000)

                    email_input = page.locator('input[type="email"], input[name="email"], input[name="username"]')
                    if await email_input.count() > 0:
                        await email_input.first.fill(email)

                    pw_input = page.locator('input[type="password"]')
                    if await pw_input.count() > 0:
                        await pw_input.first.fill(password)

                    submit = page.locator('button[type="submit"], input[type="submit"]')
                    if await submit.count() > 0:
                        await submit.first.click()

                    await page.wait_for_load_state("networkidle")
                    await page.wait_for_timeout(3000)

                    await page.goto("https://app.offerbox.jp/v2/scompany", wait_until="networkidle", timeout=30000)
                    await page.wait_for_timeout(3000)

                    html = await page.content()
                    return s._parse_page(html)
                finally:
                    await browser.close()

        results = asyncio.run(test_offerbox())
        path = save_results("offerbox", results, "offerbox.csv")
        summary.append(("オファーボックス", len(results), "✓" if results else "✗", str(path)))
        if results:
            for r in results[:3]:
                print(f"    {r['企業名']}")
    except Exception as e:
        logger.error(f"  エラー: {e}", exc_info=True)
        summary.append(("オファーボックス", 0, f"✗ {e}", ""))

    # =============================================
    # 14. ビズリーチ (Playwright - スキップ可)
    # =============================================
    print("\n▶ [14/14] ビズリーチ (Playwright)")
    try:
        from scrapers.bizreach import Scraper as S14
        s = S14()
        import asyncio
        from playwright.async_api import async_playwright
        import os

        async def test_bizreach():
            email = os.getenv("BIZREACH_EMAIL")
            password = os.getenv("BIZREACH_PASSWORD")
            if not email:
                return []

            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
                )
                page = await context.new_page()
                try:
                    await page.goto("https://www.bizreach.jp/login/", wait_until="networkidle", timeout=30000)
                    await page.wait_for_timeout(2000)

                    email_input = page.locator('input[type="email"], input[name="email"], input[name="username"]')
                    if await email_input.count() > 0:
                        await email_input.first.fill(email)

                    pw_input = page.locator('input[type="password"]')
                    if await pw_input.count() > 0:
                        await pw_input.first.fill(password)

                    submit = page.locator('button[type="submit"], input[type="submit"]')
                    if await submit.count() > 0:
                        await submit.first.click()

                    await page.wait_for_load_state("networkidle")
                    await page.wait_for_timeout(3000)

                    await page.goto("https://www.bizreach.jp/job/?p=1&pageSize=20", wait_until="networkidle", timeout=30000)
                    await page.wait_for_timeout(3000)

                    html = await page.content()
                    return s._parse_page(html)
                finally:
                    await browser.close()

        results = asyncio.run(test_bizreach())
        path = save_results("bizreach", results, "bizreach.csv")
        summary.append(("ビズリーチ", len(results), "✓" if results else "✗", str(path)))
        if results:
            for r in results[:3]:
                print(f"    {r['企業名']} | {r['タイトル'][:30] if r['タイトル'] else ''}")
    except Exception as e:
        logger.error(f"  エラー: {e}", exc_info=True)
        summary.append(("ビズリーチ", 0, f"✗ {e}", ""))

    # =============================================
    # サマリー出力
    # =============================================
    print("\n" + "=" * 70)
    print("テスト結果サマリー")
    print("=" * 70)
    print(f"{'サービス名':<20} {'取得件数':>8} {'状態':<6}")
    print("-" * 50)
    total_ok = 0
    total_ng = 0
    for name, count, status, path in summary:
        print(f"{name:<20} {count:>8}件  {status}")
        if status == "✓":
            total_ok += 1
        else:
            total_ng += 1
    print("-" * 50)
    print(f"成功: {total_ok}/14, 失敗: {total_ng}/14")
    print(f"出力先: {OUTPUT_DIR}/")
    print("=" * 70)


if __name__ == "__main__":
    main()
