"""ビズリーチキャンパス スクレイパー - https://br-campus.jp/events
React SPA + GraphQL API。
イベントページ(/events)はログイン不要で企業名が取得可能。
企業一覧ページ(/companies)はログイン必須。
企業名セレクタ: [class*="company"] 要素のテキスト
"""

import asyncio

from scrapers.base import BaseScraper


class Scraper(BaseScraper):
    service_name = "bizreach_campus"
    output_filename = "bizreach_campus.csv"

    EVENTS_URL = "https://br-campus.jp/events"

    def scrape(self) -> list[dict]:
        """Playwrightで非同期実行"""
        asyncio.run(self._async_scrape())
        return self.results

    async def _async_scrape(self):
        from playwright.async_api import async_playwright

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            )
            page = await context.new_page()
            seen_companies: set[str] = set()

            try:
                self.logger.info(f"イベントページへ遷移: {self.EVENTS_URL}")
                await page.goto(self.EVENTS_URL, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(8000)

                # スクロールして追加コンテンツを読み込み
                for _ in range(5):
                    await page.evaluate("window.scrollBy(0, 1000)")
                    await page.wait_for_timeout(1000)

                html = await page.content()
                companies = self._parse_page(html)

                for c in companies:
                    if c["企業名"] not in seen_companies:
                        seen_companies.add(c["企業名"])
                        self.results.append(c)

                self.logger.info(f"取得完了: {len(self.results)}社")

            except Exception as e:
                self.logger.error(f"Playwright エラー: {e}", exc_info=True)
            finally:
                await browser.close()

    def _parse_page(self, html: str) -> list[dict]:
        """HTML解析でイベントページから企業名を抽出
        [class*="company"] 要素のテキストから企業名を取得。
        「ビズリーチ・キャンパス」自体は除外する。
        """
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "lxml")
        companies = []

        company_els = soup.select('[class*="company"]')
        for el in company_els:
            text = el.get_text(strip=True)
            if text and len(text) >= 2 and "ビズリーチ" not in text:
                companies.append({
                    "企業名": text,
                    "タイトル": "",
                    "掲載日": "",
                })

        return companies


if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    import logging
    from config import LOG_DIR

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[
            logging.FileHandler(LOG_DIR / "scraper.log", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    scraper = Scraper()
    scraper.run()
