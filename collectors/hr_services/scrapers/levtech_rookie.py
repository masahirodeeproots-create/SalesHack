"""レバテックルーキー スクレイパー - https://rookie.levtech.jp/company/
Nuxt SPA のためPlaywrightでレンダリングが必要。ログイン不要。
企業名セレクタ: div.companyCard > div.headerWrapper p.name
ページネーション: /company/p2/, /company/p3/ ...
"""

import asyncio

from scrapers.base import BaseScraper


class Scraper(BaseScraper):
    service_name = "levtech_rookie"
    output_filename = "levtech_rookie.csv"

    BASE_URL = "https://rookie.levtech.jp/company/"

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
            page_num = 1

            try:
                while True:
                    url = self.BASE_URL if page_num == 1 else f"{self.BASE_URL}p{page_num}/"
                    self.logger.info(f"Page {page_num}: {url}")

                    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    # SPAレンダリング待機
                    await page.wait_for_timeout(8000)

                    html = await page.content()
                    companies = self._parse_page(html)

                    if not companies:
                        self.logger.info(f"Page {page_num}: 結果なし → 終了")
                        break

                    # 重複排除
                    new_count = 0
                    for c in companies:
                        if c["企業名"] not in seen_companies:
                            seen_companies.add(c["企業名"])
                            self.results.append(c)
                            new_count += 1

                    self.logger.info(
                        f"Page {page_num}: {len(companies)}社（新規{new_count}社、累計: {len(self.results)}社）"
                    )

                    # 次ページの存在確認
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(html, "lxml")
                    next_link = soup.select_one(f'a[href="/company/p{page_num + 1}/"]')
                    if not next_link:
                        self.logger.info(f"Page {page_num}: 次ページなし → 終了")
                        break

                    page_num += 1

            except Exception as e:
                self.logger.error(f"Playwright エラー: {e}", exc_info=True)
                if self.results:
                    self.save_csv()
            finally:
                await browser.close()

    def _parse_page(self, html: str) -> list[dict]:
        """HTML解析で企業名を抽出
        カード構造:
          div.companyCard
            div.headerWrapper
              div.companyHeader > p.head → カテゴリ
              div.companyHeadline > p.name → 企業名
        """
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "lxml")
        companies = []

        cards = soup.select("div.companyCard")
        for card in cards:
            name_el = card.select_one("p.name")
            if name_el:
                name = name_el.get_text(strip=True)
                if name and len(name) >= 2:
                    companies.append({
                        "企業名": name,
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
