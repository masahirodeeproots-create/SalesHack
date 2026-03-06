"""キミスカ スクレイパー - https://kimisuka.com/
逆求人型（企業→学生スカウト）のため企業検索/一覧機能が存在しない。
ログインURL: https://kimisuka.com/user/auth/login
フォーム: input[name="email"] / input[name="u_pass"]

取得可能なデータソース:
- スカウト受信一覧（アカウントにスカウトが来ている場合のみ）
- インターンシップ検索（掲載がある場合のみ）
- 企業説明会一覧（掲載がある場合のみ）

現状: 新規アカウントのためスカウト0件、インターン募集0件。
→ プロフィール充実後、またはスカウト受信後に再実装を検討。
"""

import os
import asyncio
from dotenv import load_dotenv

from scrapers.base import BaseScraper

load_dotenv()


class Scraper(BaseScraper):
    service_name = "kimisuka"
    output_filename = "kimisuka.csv"

    LOGIN_URL = "https://kimisuka.com/user/auth/login"

    def scrape(self) -> list[dict]:
        """Playwrightで非同期実行"""
        asyncio.run(self._async_scrape())
        return self.results

    async def _async_scrape(self):
        from playwright.async_api import async_playwright

        email = os.getenv("KIMISUKA_EMAIL")
        password = os.getenv("KIMISUKA_PASSWORD")
        if not email or not password:
            self.logger.error("KIMISUKA_EMAIL / KIMISUKA_PASSWORD が.envに設定されていません")
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
                await page.goto(self.LOGIN_URL, wait_until="domcontentloaded", timeout=15000)
                await page.wait_for_timeout(2000)

                await page.locator('input[name="email"]').fill(email)
                await page.locator('input[name="u_pass"]').fill(password)
                await page.locator('button[type="submit"]').click()

                await page.wait_for_load_state("domcontentloaded")
                await page.wait_for_timeout(5000)
                self.logger.info(f"ログイン完了: {page.url}")

                if "login" in page.url:
                    self.logger.error("ログイン失敗")
                    return

                # スカウト一覧から企業名を取得
                year = page.url.split("/")[3] if len(page.url.split("/")) > 3 else "2027"
                scout_url = f"https://kimisuka.com/{year}/message/scout_list"
                self.logger.info(f"スカウト一覧へ遷移: {scout_url}")
                await page.goto(scout_url, wait_until="domcontentloaded", timeout=15000)
                await page.wait_for_timeout(5000)

                html = await page.content()
                companies = self._parse_scout_list(html)

                if companies:
                    self.results.extend(companies)
                    self.logger.info(f"スカウト一覧から{len(companies)}社取得")
                else:
                    self.logger.warning(
                        "スカウト受信0件。逆求人型のため、プロフィール充実後に再実行が必要です。"
                    )

            except Exception as e:
                self.logger.error(f"Playwright エラー: {e}", exc_info=True)
            finally:
                await browser.close()

    def _parse_scout_list(self, html: str) -> list[dict]:
        """スカウト一覧HTMLから企業名を抽出"""
        import re
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "lxml")
        companies = []
        seen = set()

        # 「株式会社」「有限会社」等を含むテキストを企業名として抽出
        text = soup.get_text()
        matches = re.findall(r'(?:株式会社[\w・ー]+|[\w・ー]+株式会社)', text)
        for m in matches:
            name = m.strip()
            if name and len(name) >= 4 and name not in seen:
                seen.add(name)
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
