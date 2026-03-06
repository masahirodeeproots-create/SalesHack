"""オファーボックス スクレイパー - https://app.offerbox.jp/v2/scompany
Playwright によるログイン認証 + 企業一覧スクレイピング。
ログインフォーム: input[name="LOGINID"] / input[name="PASSWORD"]
企業名セレクタ: ul.company-list > li.check > p.link-list__tit
"""

import os
import asyncio
from dotenv import load_dotenv

from scrapers.base import BaseScraper

load_dotenv()


class Scraper(BaseScraper):
    service_name = "offerbox"
    output_filename = "offerbox.csv"

    LOGIN_URL = "https://app.offerbox.jp/"
    COMPANY_LIST_URL = "https://app.offerbox.jp/v2/scompany"

    def scrape(self) -> list[dict]:
        """Playwrightで非同期実行"""
        asyncio.run(self._async_scrape())
        return self.results

    async def _async_scrape(self):
        from playwright.async_api import async_playwright

        email = os.getenv("OFFERBOX_EMAIL")
        password = os.getenv("OFFERBOX_PASSWORD")
        if not email or not password:
            self.logger.error("OFFERBOX_EMAIL / OFFERBOX_PASSWORD が.envに設定されていません")
            return

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

            try:
                # ログイン
                self.logger.info("ログインページへ遷移中...")
                await page.goto(self.LOGIN_URL, wait_until="domcontentloaded")
                await page.wait_for_timeout(3000)

                # OfferBox 固有のフォーム: LOGINID / PASSWORD
                await page.locator('input[name="LOGINID"]').fill(email)
                await page.locator('input[name="PASSWORD"]').fill(password)

                # ログインボタン（「Googleでログイン」ではなく最後の「ログイン」ボタン）
                buttons = page.locator('button:has-text("ログイン")')
                btn_count = await buttons.count()
                await buttons.nth(btn_count - 1).click()

                await page.wait_for_load_state("domcontentloaded")
                await page.wait_for_timeout(5000)
                self.logger.info(f"ログイン完了: {page.url}")

                # 企業一覧ページへ遷移
                self.logger.info("企業一覧ページへ遷移中...")
                await page.goto(self.COMPANY_LIST_URL, wait_until="domcontentloaded")
                await page.wait_for_timeout(5000)

                # ページネーションしながら企業名を取得
                page_num = 1
                seen_companies: set[str] = set()

                while True:
                    self.logger.info(f"Page {page_num}: 企業データ取得中...")

                    html = await page.content()
                    companies = self._parse_page(html)

                    if not companies:
                        self.logger.info(f"Page {page_num}: 結果なし → 終了")
                        break

                    # 重複排除しながら追加
                    new_count = 0
                    for c in companies:
                        if c["企業名"] not in seen_companies:
                            seen_companies.add(c["企業名"])
                            self.results.append(c)
                            new_count += 1

                    self.logger.info(
                        f"Page {page_num}: {len(companies)}社（新規{new_count}社、累計: {len(self.results)}社）"
                    )

                    # 次のページへ
                    next_btn = page.locator(
                        'a:has-text("次へ"), button:has-text("次へ"), '
                        '[class*="next"], [aria-label="next"]'
                    )
                    if await next_btn.count() > 0:
                        await next_btn.first.click()
                        await page.wait_for_load_state("domcontentloaded")
                        await page.wait_for_timeout(3000)
                        page_num += 1
                    else:
                        self.logger.info("次へボタンなし → 終了")
                        break

            except Exception as e:
                self.logger.error(f"Playwright エラー: {e}", exc_info=True)
                if self.results:
                    self.save_csv()
            finally:
                await browser.close()

    def _parse_page(self, html: str) -> list[dict]:
        """HTML解析で企業名を抽出"""
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "lxml")
        companies = []

        # OfferBox の企業名セレクタ: p.link-list__tit
        name_els = soup.select("p.link-list__tit")
        for el in name_els:
            name = el.get_text(strip=True)
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
