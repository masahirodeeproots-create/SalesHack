"""オファーボックス スクレイパー - https://app.offerbox.jp/v2/scompany
Playwright によるログイン認証 + 企業一覧スクレイピング。
"""

import os
import asyncio
import re
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
                await page.goto(self.LOGIN_URL, wait_until="networkidle")
                await page.wait_for_timeout(2000)

                # メールアドレス入力
                email_input = page.locator('input[type="email"], input[name="email"], input[name="username"]')
                if await email_input.count() > 0:
                    await email_input.first.fill(email)
                else:
                    # 代替セレクタ
                    inputs = page.locator("input")
                    count = await inputs.count()
                    for i in range(count):
                        input_type = await inputs.nth(i).get_attribute("type")
                        if input_type in ("email", "text"):
                            await inputs.nth(i).fill(email)
                            break

                # パスワード入力
                password_input = page.locator('input[type="password"]')
                if await password_input.count() > 0:
                    await password_input.first.fill(password)

                # ログインボタンクリック
                submit_btn = page.locator('button[type="submit"], input[type="submit"]')
                if await submit_btn.count() > 0:
                    await submit_btn.first.click()
                else:
                    # 代替: ログインテキストを含むボタン
                    login_btn = page.locator('button:has-text("ログイン")')
                    if await login_btn.count() > 0:
                        await login_btn.first.click()

                await page.wait_for_load_state("networkidle")
                await page.wait_for_timeout(3000)
                self.logger.info("ログイン完了")

                # 企業一覧ページへ遷移
                self.logger.info("企業一覧ページへ遷移中...")
                await page.goto(self.COMPANY_LIST_URL, wait_until="networkidle")
                await page.wait_for_timeout(3000)

                # ページネーションしながら企業名を取得
                page_num = 1
                while True:
                    self.logger.info(f"Page {page_num}: 企業データ取得中...")

                    html = await page.content()
                    companies = self._parse_page(html)

                    if not companies:
                        self.logger.info(f"Page {page_num}: 結果なし → 終了")
                        break

                    self.results.extend(companies)
                    self.logger.info(
                        f"Page {page_num}: {len(companies)}社取得（累計: {len(self.results)}社）"
                    )

                    # 次のページへ
                    next_btn = page.locator(
                        'a:has-text("次へ"), button:has-text("次へ"), '
                        '[class*="next"], [aria-label="next"]'
                    )
                    if await next_btn.count() > 0:
                        await next_btn.first.click()
                        await page.wait_for_load_state("networkidle")
                        await page.wait_for_timeout(2000)
                        page_num += 1
                    else:
                        self.logger.info("次へボタンなし → 終了")
                        break

            except Exception as e:
                self.logger.error(f"Playwright エラー: {e}", exc_info=True)
            finally:
                await browser.close()

    def _parse_page(self, html: str) -> list[dict]:
        """HTML解析で企業名を抽出"""
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "lxml")
        companies = []
        seen = set()

        # 企業カードのリンクや見出しから企業名を抽出
        # パターン1: company関連リンク
        company_els = soup.select(
            "a[href*='company'], [class*='company'] h2, "
            "[class*='company'] h3, [class*='company'] a"
        )

        for el in company_els:
            name = el.get_text(strip=True)
            if name and len(name) >= 2 and name not in seen:
                if name not in ("企業一覧", "企業検索", "もっと見る"):
                    seen.add(name)
                    companies.append({
                        "企業名": name,
                        "タイトル": "",
                        "掲載日": "",
                    })

        return companies


if __name__ == "__main__":
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
