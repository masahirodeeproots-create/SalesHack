"""ビズリーチ スクレイパー - https://www.bizreach.jp/jobs/search/
Playwright によるOAuthログイン(auth.id.bizreach.jp) + 求人ページネーション。
100,000件以上 → 新規企業発見率が低下したら自動停止。
チェックポイントで中断再開対応。
"""

import os
import asyncio
from dotenv import load_dotenv

from scrapers.base import BaseScraper

load_dotenv()


class Scraper(BaseScraper):
    service_name = "bizreach"
    output_filename = "bizreach.csv"         # 企業単位 dedup 済み
    output_filename_raw = "bizreach_raw.csv"  # 求人単位 raw

    # OAuth経由でログインするため、/login/ がauth.id.bizreach.jpにリダイレクトされる
    LOGIN_URL = "https://www.bizreach.jp/login/"
    JOB_SEARCH_URL = "https://www.bizreach.jp/jobs/search/"

    # 新規企業発見率がこの閾値未満になったら停止
    NEW_COMPANY_RATE_THRESHOLD = 0.05
    # 閾値チェック開始ページ数
    THRESHOLD_CHECK_START = 50
    CHECKPOINT_INTERVAL = 20

    def scrape(self) -> list[dict]:
        """Playwrightで非同期実行"""
        asyncio.run(self._async_scrape())
        return self.results

    async def _async_scrape(self):
        from playwright.async_api import async_playwright

        email = os.getenv("BIZREACH_EMAIL")
        password = os.getenv("BIZREACH_PASSWORD")
        if not email or not password:
            self.logger.error("BIZREACH_EMAIL / BIZREACH_PASSWORD が.envに設定されていません")
            return

        # 中断再開
        start_page = self.get_checkpoint()
        known_companies = set()
        if start_page > 0:
            self.results = self.load_existing_results()
            known_companies = {r["企業名"] for r in self.results}
            self.logger.info(
                f"チェックポイントから再開: Page {start_page}（既存{len(self.results)}件、{len(known_companies)}社）"
            )

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
                # ログイン（auth.id.bizreach.jp にリダイレクトされる）
                self.logger.info("ログインページへ遷移中...")
                await page.goto(self.LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(3000)

                # OAuth ログインフォーム（auth.id.bizreach.jp）
                # ソーシャルログインボタン群の後に email/password フォーム
                await page.locator('input[type="email"]').first.fill(email)
                await page.locator('input[type="password"]').first.fill(password)

                # 最後のボタンが「ログイン」（ソーシャルログインボタンの後）
                buttons = page.locator("button")
                btn_count = await buttons.count()
                await buttons.nth(btn_count - 1).click()

                await page.wait_for_load_state("domcontentloaded")
                await page.wait_for_timeout(5000)
                self.logger.info(f"ログイン完了: {page.url}")

                # 求人検索ページネーション（フィルターなし・更新日順）
                page_num = max(start_page, 1)
                recent_new_count = 0
                recent_total_count = 0

                while True:
                    url = f"{self.JOB_SEARCH_URL}?pageNumber={page_num}&sort=UPDATED_DESC"
                    self.logger.info(f"Page {page_num}: {url}")

                    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    await page.wait_for_timeout(5000)

                    html = await page.content()
                    items = self._parse_page(html)

                    if not items:
                        self.logger.info(f"Page {page_num}: 結果なし → 終了")
                        break

                    # 新規企業カウント
                    new_count = 0
                    for item in items:
                        if item["企業名"] not in known_companies:
                            known_companies.add(item["企業名"])
                            new_count += 1
                        self.results.append(item)

                    recent_new_count += new_count
                    recent_total_count += len(items)

                    self.logger.info(
                        f"Page {page_num}: {len(items)}件（新規{new_count}社）"
                        f"累計: {len(self.results)}件（{len(known_companies)}社）"
                    )

                    # チェックポイント保存
                    if page_num % self.CHECKPOINT_INTERVAL == 0:
                        self.save_checkpoint(page_num)
                        self.save_csv()

                        # 新規企業発見率チェック
                        if page_num >= self.THRESHOLD_CHECK_START and recent_total_count > 0:
                            rate = recent_new_count / recent_total_count
                            self.logger.info(
                                f"直近{self.CHECKPOINT_INTERVAL}ページの新規企業率: "
                                f"{rate:.1%} ({recent_new_count}/{recent_total_count})"
                            )
                            if rate < self.NEW_COMPANY_RATE_THRESHOLD:
                                self.logger.info(
                                    f"新規企業率 {rate:.1%} < {self.NEW_COMPANY_RATE_THRESHOLD:.0%} → 自動停止"
                                )
                                break
                        recent_new_count = 0
                        recent_total_count = 0

                    page_num += 1

            except Exception as e:
                self.logger.error(f"Playwright エラー: {e}", exc_info=True)
                # 途中結果を保存
                if self.results:
                    self.save_csv()
                    self.save_checkpoint(page_num)
            finally:
                await browser.close()

        self.clear_checkpoint()

    def _parse_page(self, html: str) -> list[dict]:
        """HTML解析で企業名と求人タイトルを抽出
        カード構造:
          li[class*="JobListItem"]
            h3[class*="JobTitle"] → 求人タイトル
            [class*="grow-1"] p[class*="bold"] → 企業名
        """
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "lxml")
        items = []

        cards = soup.select('li[class*="JobListItem"]')

        for card in cards:
            # 求人タイトル
            title_el = card.select_one('h3[class*="JobTitle"]')
            title = title_el.get_text(strip=True) if title_el else ""

            # 企業名: grow-1 div 内の bold p 要素
            company_name = ""
            grow_div = card.select_one('[class*="grow-1"]')
            if grow_div:
                bold_p = grow_div.select_one('p[class*="bold"]')
                if bold_p:
                    company_name = bold_p.get_text(strip=True)

            if not company_name:
                # フォールバック: カード内の bold p で「気になる」「万円」以外
                for bp in card.select('p[class*="bold"]'):
                    text = bp.get_text(strip=True)
                    if text and text not in ("気になる", "") and "万円" not in text:
                        company_name = text
                        break

            if company_name:
                items.append({
                    "企業名": company_name,
                    "タイトル": title,
                    "掲載日": "",
                })

        return items


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
